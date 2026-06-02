use std::collections::HashMap;

use raftcore::{
    RaftCore,
    types::{Action, Message, NodeId},
};
use types::{RaftConfig, Storage, Transport};

use tokio::{
    sync::{self, oneshot},
    time::Interval,
};

use crate::types::{AppliedEntry, Proposal, ProposalError, Snapshot};

pub mod types;

/// The primary type to interact with an event loop from an application.
/// This type will create a new Tokio task that actually executes the event loop and will return
/// to the application the ability to interact with the event loop through propose() and a channel
/// to receive applied commands on.
pub struct RaftNode {
    /// Channel to send proposed commands to the Raft log
    proposal: sync::mpsc::Sender<Proposal>,

    /// Channel to send read requests to Raft through
    read: sync::mpsc::Sender<oneshot::Sender<Result<(), ProposalError>>>,
}

impl RaftNode {
    const CHANNEL_BUFFER: usize = 1024;

    /// Returns the RaftNode as well as a channel to receive 'commands' to apply to the application
    /// (completely up to the applicationt to define what a channel looks like).
    pub async fn new<T: Transport + Send + 'static, S: Storage + Send + 'static>(
        config: RaftConfig<T, S>,
    ) -> (Self, sync::mpsc::Receiver<AppliedEntry>) {
        let (proposal_sender, proposal_receiver) = sync::mpsc::channel(Self::CHANNEL_BUFFER);
        let (read_sender, read_receiver) = sync::mpsc::channel(Self::CHANNEL_BUFFER);
        let (applied_sender, applied_receiver) = sync::mpsc::channel(Self::CHANNEL_BUFFER);

        let driver =
            RaftDriver::new(config, proposal_receiver, read_receiver, applied_sender).await;
        tokio::spawn(async move { driver.event_loop().await });

        (
            Self {
                proposal: proposal_sender,
                read: read_sender,
            },
            applied_receiver,
        )
    }

    /// Propose a command into the cluster and get notified when the cluster has replicated the
    /// command
    ///
    /// Return value:
    /// Ok(()): Command was successfully replicated throughout the cluster
    /// Err(e): Error encountered during replication
    pub async fn propose(&self, command: Vec<u8>) -> Result<(), ProposalError> {
        let (tx, rx) = oneshot::channel();
        self.proposal
            .send(Proposal {
                command,
                respond: tx,
            })
            .await
            .expect("Unable to send proposals; cannot continue");
        rx.await
            .expect("Error receiving response of proposal; cannot continue")
    }

    /// Propose a read request in the cluster. The read request effectively just ensures that the
    /// node's state machine that is being read from is still in fact a leader which gives
    /// linearizable reads.
    pub async fn read_request(&self) -> Result<(), ProposalError> {
        let (tx, rx) = oneshot::channel();
        self.read
            .send(tx)
            .await
            .expect("Unable to send read requests; cannot continue");
        rx.await
            .expect("Error receiving response of read request; cannot continue")
    }
}

/// The key event loop type to interact with the RaftCore. This type is not meant to be constructed
/// by applications but interacted with through RaftNode
struct RaftDriver<T: Transport, S: Storage> {
    /// ID of the underlying Raft node
    id: NodeId,

    /// Channel to receive proposed commands on
    proposal: sync::mpsc::Receiver<Proposal>,

    /// Channel to receive read requests on
    read: sync::mpsc::Receiver<sync::oneshot::Sender<Result<(), ProposalError>>>,

    /// Channel to send applied commands back to the application
    applied: sync::mpsc::Sender<AppliedEntry>,

    /// The interval for heartbeats
    interval: Interval,

    /// The sans-I/O Raft implementor
    core: RaftCore,

    /// Pending proposals for commands
    pending_proposals: HashMap<u64, oneshot::Sender<Result<(), ProposalError>>>,

    /// Pending reads to be linearizable
    pending_reads: HashMap<u64, oneshot::Sender<Result<(), ProposalError>>>,

    /// Entries since last snapshot
    num_entries: usize,

    /// Number of entries to apply before snapshotting
    snapshot_threshold: usize,

    /// Transport for sending messages
    transport: T,

    /// Storage engine
    storage: S,
}

impl<T: Transport, S: Storage> RaftDriver<T, S> {
    pub async fn new(
        config: RaftConfig<T, S>,
        proposal_receiver: sync::mpsc::Receiver<Proposal>,
        read_receiver: sync::mpsc::Receiver<sync::oneshot::Sender<Result<(), ProposalError>>>,
        applied_sender: sync::mpsc::Sender<AppliedEntry>,
    ) -> Self {
        // build out the RaftCore type first, panicing in situations where errors occur because we
        // cannot proceed without certain data
        let transport = config.transport;
        let storage = config.storage;
        let node_ids = config.nodes.iter().map(|desc| desc.id).collect::<Vec<_>>();

        // reminder that Err here means unrecoverable error so we panic
        let snapshot = storage
            .restore_snapshot()
            .await
            .expect("Error reading snapshot");
        let metadata = storage
            .restore_metadata()
            .await
            .expect("Error reading persisted metadata");
        let entries = storage
            .restore_log_entries()
            .await
            .expect("Error reading persisted log entries");

        let core = match snapshot {
            Some(snapshot) => {
                // if a snapshot is present, then there MUST be persisted state
                let metadata = metadata.expect("Error: must have metadata with a snapshot");
                let entries = entries.expect("Error: must have log entires with a snapshot");
                RaftCore::restore_with_snapshot(
                    config.id,
                    &node_ids[..],
                    config.heartbeat_interval,
                    config.election_range,
                    metadata.term,
                    metadata.voted_for,
                    entries,
                    snapshot.last_included_index,
                    snapshot.last_included_term,
                )
            }
            None => {
                // no snapshot, so we need to check if we have metadata
                if let Some(metadata) = metadata {
                    RaftCore::restore(
                        config.id,
                        &node_ids[..],
                        config.heartbeat_interval,
                        config.election_range,
                        metadata.term,
                        metadata.voted_for,
                        entries.unwrap_or_default(),
                    )
                } else {
                    // starting from scratch (no need to check if log exists because you MUST have
                    // current_term to restore)
                    RaftCore::new(
                        config.id,
                        &node_ids[..],
                        config.heartbeat_interval,
                        config.election_range,
                    )
                }
            }
        };
        let interval = tokio::time::interval(config.tick_length);

        Self {
            id: config.id,
            proposal: proposal_receiver,
            read: read_receiver,
            applied: applied_sender,
            interval,
            pending_proposals: HashMap::new(),
            pending_reads: HashMap::new(),
            num_entries: 0,
            snapshot_threshold: config.snapshot_threshold,
            core,
            transport,
            storage,
        }
    }

    pub async fn event_loop(mut self) {
        // the MAIN EVENT (loop)
        // here, we run an infinite loop either receiving proposals from the application on the
        // proposal channel or messages from the network
        tracing::info!(node_id = self.id, "Starting event loop");

        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    let actions = self.core.tick();
                    for action in actions {
                        self.handle_action(action).await;
                    }
                }
                Some(proposal) = self.proposal.recv() => {
                    tracing::debug!(node_id = self.id, "proposal received from application");
                    // propose new command and wait for the response to come through
                    if let Some(actions) = self.core.propose(proposal.command) {
                        // grab the PersistLogEntries for the index
                        for action in &actions {
                            if let Action::PersistLogEntries { start_index, .. } = action {
                                let _ = self.pending_proposals.insert(*start_index, proposal.respond);
                                break;
                            }
                        }
                        for action in actions {
                            self.handle_action(action).await;
                        }
                    } else {
                        if proposal.respond.send(Err(ProposalError::FollowerNode)).is_err() {
                            tracing::warn!("Error sending proposal response")
                        }
                    }
                },
                Some(read_request) = self.read.recv() => {
                    tracing::debug!(node_id = self.id, "read request received from application");
                    if let Some(read_id) = self.core.request_read_barrier() {
                        self.pending_reads.insert(read_id, read_request);
                    } else {
                        if read_request.send(Err(ProposalError::FollowerNode)).is_err() {
                            tracing::warn!("Error sending read request response");
                        }
                    }
                },
                msg = self.transport.recv() => {
                    tracing::debug!(node_id = self.id, msg = %msg, "received Message");
                    let before = self.core.is_leader();
                    self.handle_message(msg).await;
                    let after = self.core.is_leader();
                    if before && !after {
                        // we were the leader and now we're not, we have to get rid of the pending
                        // proposals and notify them of failure
                        tracing::info!(node_id = self.id, "leadership change detected, draining pending proposals");
                        for (_, channel) in self.pending_proposals.drain() {
                            let _ = channel.send(Err(ProposalError::LostLeadership));
                        }
                        for (_, channel) in self.pending_reads.drain() {
                            let _ = channel.send(Err(ProposalError::LostLeadership));
                        }
                    }
                },
            }
        }
    }

    async fn handle_action(&mut self, action: Action) {
        match action {
            Action::SendMessage { target, message } => {
                self.transport.send(target, message).await;
            }
            Action::PersistMetadata { term, voted_for } => {
                self.storage
                    .store_metadata(types::PersistedMetadata { term, voted_for })
                    .await
                    .expect("Error storing metadata; cannot continue");
            }
            Action::PersistLogEntries {
                start_index,
                entries,
            } => {
                self.storage
                    .store_log_entries(types::PersistedLogAddendum {
                        start_index,
                        entries,
                    })
                    .await
                    .expect("Error storing log entries; cannot continue");
            }
            Action::InstallSnapshot {
                last_included_index,
                last_included_term,
                data,
            } => {
                // store the snapshot
                self.storage
                    .store_snapshot(types::Snapshot {
                        last_included_index,
                        last_included_term,
                        data: data.clone(),
                    })
                    .await
                    .expect("Error storing snapshot; cannot continue");

                // restore application state
                self.applied
                    .send(AppliedEntry::Snapshot(data))
                    .await
                    .expect("Unable to send applied snapshot; cannot continue");
                // clear the log file for persistent state (since RaftCore has cleared its log)
                self.storage
                    .truncate_log()
                    .await
                    .expect("Unable to clear persistent log; cannot continue");
            }
            Action::SendInstallSnapshot {
                target,
                term,
                last_included_index,
                last_included_term,
            } => {
                let data = self
                    .storage
                    .retrieve_snapshot_bytes()
                    .await
                    .expect("Cannot read snapshot bytes; cannot continue");
                self.transport
                    .send(
                        target,
                        Message::InstallSnapshot(raftcore::types::InstallSnapshotRPC {
                            term,
                            leader_id: self.id,
                            last_included_index,
                            last_included_term,
                            data,
                        }),
                    )
                    .await;
            }
            Action::ApplyToStateMachine { index, command } => {
                // look up the index in the map and reply on the channel that the command was
                // replicated
                if let Some(channel) = self.pending_proposals.remove(&index) {
                    let _ = channel.send(Ok(()));
                }
                // if the command is empty, we don't send to the application
                if command.is_empty() {
                    return;
                }
                self.applied
                    .send(AppliedEntry::Command(command))
                    .await
                    .expect("Unable to send applied commands; cannot continue");
                self.num_entries += 1;
                if self.num_entries >= self.snapshot_threshold {
                    tracing::info!(
                        node_id = self.id,
                        "reached snapshot threshold and snapshotting"
                    );
                    // time to snapshot so we will also request the snapshot data
                    let (tx, rx) = oneshot::channel();
                    self.applied
                        .send(AppliedEntry::SnapshotRequest(tx))
                        .await
                        .expect("Unable to send snapshot request; cannot continue");
                    // SAFETY: once we await the receiver, no other entries could be applied in the
                    // event loop's task, so we know that once the bytes are retrieved and we
                    // synchronously call prepare_snapshot() we know the indices and bytes match up
                    let data = rx
                        .await
                        .expect("Unable to receive application data for snapshot; cannot continue");
                    let metadata = self.core.prepare_snapshot();
                    // the data can be stored in a separate task, it no longer matters because we
                    // have the data and indices matched
                    self.storage
                        .store_snapshot(Snapshot {
                            last_included_index: metadata.last_applied,
                            last_included_term: metadata.last_applied_term,
                            data,
                        })
                        .await
                        .expect("Unable to store snapshot");
                    // signal to the core that we have stored the snapshot
                    self.core.complete_snapshot(metadata);
                    self.storage
                        .truncate_log()
                        .await
                        .expect("Unable to clear persistent log; cannot continue");
                    self.num_entries = 0;
                }
            }
            Action::ReadBarrierReady { id } => {
                if let Some(sender) = self.pending_reads.remove(&id) {
                    let _ = sender.send(Ok(()));
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: Message) {
        let actions = match msg {
            Message::RequestVote(request_vote) => self.core.handle_request_vote(request_vote),
            Message::RequestVoteResponse(request_vote_resp) => {
                self.core.handle_request_vote_response(request_vote_resp)
            }
            Message::AppendEntries(append_entries) => {
                self.core.handle_append_entries(append_entries)
            }
            Message::AppendEntriesResponse(append_entries_resp) => self
                .core
                .handle_append_entries_response(append_entries_resp),
            Message::InstallSnapshot(install_snapshot) => {
                self.core.handle_install_snapshot(install_snapshot)
            }
            Message::InstallSnapshotResponse(install_snapshot_resp) => self
                .core
                .handle_install_snapshot_response(install_snapshot_resp),
        };

        for action in actions {
            self.handle_action(action).await;
        }
    }
}
