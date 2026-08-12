use std::{fmt::Display, ops::Range, time::Duration};

use raftcore::types::{LogEntry, Message};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

pub use raftcore::types::NodeId;

pub struct RaftConfig<T: Transport, S: Storage> {
    pub id: NodeId,
    pub nodes: Vec<RaftNodeDescription<T>>,
    pub heartbeat_interval: Option<u64>,
    pub election_range: Range<u64>,
    pub tick_length: Duration,
    pub snapshot_threshold: usize,
    pub transport: T,
    pub storage: S,
}

pub struct RaftNodeDescription<T: Transport> {
    pub id: NodeId,

    pub address: T::Address,
}

impl<T> Clone for RaftNodeDescription<T>
where
    T::Address: Clone,
    T: Transport,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            address: self.address.clone(),
        }
    }
}

pub trait Transport {
    type Address;

    fn send(&self, node: NodeId, message: Message) -> impl std::future::Future<Output = ()> + Send;

    /// recv should be cancel-safe to avoid dropping any messages in flight.
    fn recv(&mut self) -> impl std::future::Future<Output = Message> + Send;
}

pub trait Storage {
    fn store_metadata(
        &self,
        metadata: PersistedMetadata,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;

    fn restore_metadata(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<Option<PersistedMetadata>>> + Send;

    fn store_log_entries(
        &self,
        addendum: PersistedLogAddendum,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;

    fn restore_log_entries(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<Option<Vec<LogEntry>>>> + Send;

    fn store_snapshot(
        &self,
        snapshot: Snapshot,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;

    fn truncate_log(&self) -> impl std::future::Future<Output = std::io::Result<()>> + Send;

    fn retrieve_snapshot_bytes(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<Vec<u8>>> + Send;

    fn restore_snapshot(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<Option<Snapshot>>> + Send;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedMetadata {
    pub term: u64,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone)]
pub struct PersistedLogAddendum {
    pub start_index: u64,
    pub entries: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

pub struct Proposal {
    pub command: Vec<u8>,
    pub respond: oneshot::Sender<Result<(), ProposalError>>,
}

#[derive(Debug)]
pub enum ProposalError {
    FollowerNode { leader: Option<NodeId> },
    LostLeadership,
    OtherError,
}

impl Display for ProposalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ProposalError::*;
        match self {
            FollowerNode { leader } => write!(
                f,
                "Node is not the leader and cannot propose commands (leader: {leader:?})"
            ),
            LostLeadership => write!(
                f,
                "Node lost its status as leader and cannot propose commands anymore"
            ),
            OtherError => write!(f, "An unknown error occurred"),
        }
    }
}

pub enum AppliedEntry {
    Command(Vec<u8>),
    Snapshot(Vec<u8>),
    SnapshotRequest(oneshot::Sender<Vec<u8>>),
}
