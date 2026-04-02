use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    ops::Range,
};

use crate::types::{
    Action, AppendEntriesRPC, AppendEntriesResponseRPC, LogEntry, Message, NodeId, RaftState,
    RequestVoteRPC, RequestVoteResponseRPC,
};

use rand::prelude::*;

mod types;

/// A sans-i/o implementation of the Raft Consensus Algorithm.
///
/// This type provides a means to setup a node to participate in a cluster of nodes using the
/// Raft Consensus Algorithm. This type requires _NO_ I/O, meaning it will not perform any actual
/// networking on behalf of the user. The logic of the algorithm is the only thing this type does
/// and will emit "Actions" for an event loop to take. Of course, an event loop could ignore these
/// actions, but that would make the entire use of this type a waste.
///
/// An event loop does need to be aware of the types that it will use to interact with the node.
/// One important note to event loop implementors: you must handle the Action's received from
/// RaftCare IN ORDER for protocol correctness. If you do not, you run the risk of breaking some
/// invariants of the protocol and lose the consistency guarantees made by Raft.
///
/// Action:
/// - SendMessage: This Action directs an Event Loop to perform I/O to send a message to another
///   peer. It is important to note that this Action can be dispatched to a separate thread / task
///   without waiting on the result. The other task should simply attempt to send the message with
///   its best effort and not worry if a response is received or not.
/// - ApplyToStateMachine: This Action directs an Event Loop to provide the higher level "state
///   machine" or type that is using Raft to apply the command. For example, this could be a
///   Key-Value store that needs to apply a command such as "set x -> 8". The KV store would then set
///   the key "x" to value "8" and know that it is replicated across a majority of the nodes. Of
///   note, some ApplyToStateMachine commands will be blank. This is an artifact of a Raft
///   optimization to help catch followers up. Event loops can safely skip from delivering this
///   Action to the higher level application.
/// - PersistMetadata: This Action directs an event loop to write the metadata some metadata file.
///   It is up to the implementor if that file is the same as the log file (see below for
///   PersistLogEntries Action). Unlike SendMessage, this Action MUST be synchronous as in it must
///   complete prior to performing any additional Action's. Without waiting until the information has
///   been persisted before moving on, Raft protocol invariants may not be met.
/// - PersistLogEntries: This Action directs an event loop to write LogEntries to a persistent
///   file at the specified index. This type communicates the start index and the entries to follow
///   at that index. For example, there may be a start index of 10 and 3 entries, that means the new
///   entries to persist are at index 10, 11, and 12. This may mean entries in the log are
///   overwritten, but that is how the Action will communicate with the event loop.
struct RaftCore {
    id: NodeId,
    peers: Vec<NodeId>,
    state: RaftState,

    // --- Persistent State ---
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    // --- End Persistent State ---

    // --- Volatile State ---
    commit_index: u64,
    last_applied: u64,
    // --- End Volatile State ---

    // --- Leader Volatile State ---
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    // --- End Leader Volatile State ---
    votes_received: HashSet<NodeId>,

    ticks_since_last_heartbeat: u64,
    election_timeout_range: Range<u64>,
    election_timeout: u64,
    heartbeat_interval: u64,
}

impl RaftCore {
    const DEFAULT_HEARTBEAT: u64 = 5;

    /// Create a new RaftCore instance for a Node.
    ///
    /// Arguments:
    /// - id: This node's ID.
    /// - nodes: A collection of all the NodeId's in the cluster (including THIS node's ID).
    /// - heartbeat_interval: Optional setting of the number of ticks to heartbeat
    /// - election_range: Acceptable range for election timeout. Concrete value chosen with each
    ///   term. Provide the range as ticks based on chosen tick interval (i.e. if ticks are every
    ///   10ms, and you want a 150-300ms election timeout as noted by the paper, this should be
    ///   15..31).
    pub fn new(
        id: NodeId,
        nodes: &[NodeId],
        heartbeat_interval: Option<u64>,
        election_range: Range<u64>,
    ) -> Self {
        // seed the RNG
        let mut rng = rand::rng();

        let peers: Vec<NodeId> = nodes.iter().filter(|x| **x != id).copied().collect();
        let num_peers = peers.len();
        // add dummy entry to the log to make it 0-indexed
        let log = Vec::from([LogEntry {
            term: 0,
            command: vec![],
        }]);

        let election_timeout = rng.random_range(election_range.clone());

        Self {
            id,
            peers,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: None,
            log,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::with_capacity(num_peers),
            match_index: HashMap::with_capacity(num_peers),
            votes_received: HashSet::new(),
            ticks_since_last_heartbeat: 0,
            election_timeout_range: election_range,
            election_timeout,
            heartbeat_interval: heartbeat_interval.unwrap_or(Self::DEFAULT_HEARTBEAT),
        }
    }

    /// Restore this Raft Node from a persistent state. This will restore if a node has crashed the
    /// current_term, voted_for, and log.
    ///
    /// Arguments:
    /// - id: This node's ID.
    /// - nodes: A collection of all the NodeId's in the cluster (including THIS node's ID).
    /// - heartbeat_interval: Optional setting of the number of ticks to heartbeat
    /// - election_range: Acceptable range for election timeout. Concrete value chosen with each
    ///   term. Provide the range as ticks based on chosen tick interval (i.e. if ticks are every
    ///   10ms, and you want a 150-300ms election timeout as noted by the paper, this should be
    ///   15..31).
    /// - current_term: The restored term the node was on.
    /// - voted_for: The restored vote for the current term.
    /// - log: The restored log.
    pub fn restore(
        id: NodeId,
        nodes: &[NodeId],
        heartbeat_interval: Option<u64>,
        election_range: Range<u64>,
        current_term: u64,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
    ) -> Self {
        let mut node = Self::new(id, nodes, heartbeat_interval, election_range);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node
    }

    /// The method used for client's to interact with the Raft cluster.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<Vec<Action>> {
        if self.state != RaftState::Leader {
            return None;
        }

        self.log.push(LogEntry {
            term: self.current_term,
            command,
        });

        // reset the ticker because we don't need to send out a blank AppendEntries immediately
        // after this one
        self.ticks_since_last_heartbeat = 0;

        // send out append entries to peers
        let mut actions = vec![Action::PersistLogEntries {
            start_index: self.log.len() as u64 - 1,
            entries: self.log[self.log.len() - 1..].to_vec(),
        }];
        actions.extend(self.append_entries());
        Some(actions)
    }

    pub fn tick(&mut self) -> Vec<Action> {
        // advance ticks_since_last_heartbeat
        self.ticks_since_last_heartbeat += 1;

        match self.state {
            RaftState::Leader => self.heartbeat(),
            RaftState::Candidate => self.election_timeout(),
            RaftState::Follower => {
                // as a follower, we should check if we have hit the election timeout
                self.election_timeout()
            }
        }
    }

    fn heartbeat(&mut self) -> Vec<Action> {
        // check if we have hit the heartbeat interval
        if self.ticks_since_last_heartbeat >= self.heartbeat_interval {
            // heartbeat hit, send AppendEntries to all peers and reset clock
            let actions = self.append_entries();
            self.ticks_since_last_heartbeat = 0;
            actions
        } else {
            vec![]
        }
    }

    fn build_append_entries_for_peer(&self, peer: NodeId) -> Action {
        // the next index that the peer needs to be aware of from our tracking
        let default = self.log.len() as u64;
        let peer_next_index = self.next_index.get(&peer).unwrap_or(&default);
        // the last index the peer should have seen from our log
        let prev_log_index = if *peer_next_index > 0 {
            *peer_next_index - 1
        } else {
            0
        };
        let prev_log_term = self.log[prev_log_index as usize].term;

        // grab the entries at the next place we think the peer needs it
        let entries = &self.log[*peer_next_index as usize..];
        let msg = Message::AppendEntries(AppendEntriesRPC {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries: entries.to_vec(),
            leader_commit: self.commit_index,
        });
        Action::SendMessage {
            target: peer,
            message: msg,
        }
    }

    /// This is a "generic" function in that it can be used to send heartbeats or actual new
    /// entries added. The leader just needs to call this function when it wants to send out append
    /// entries and this function will compute what entries to send
    fn append_entries(&mut self) -> Vec<Action> {
        self.peers
            .iter()
            .map(|id| self.build_append_entries_for_peer(*id))
            .collect()
    }

    fn election_timeout(&mut self) -> Vec<Action> {
        // have we hit an election timeout?
        if self.ticks_since_last_heartbeat >= self.election_timeout {
            // start an election
            self.current_term += 1;
            self.state = RaftState::Candidate;

            // reset votes and vote for ourself
            self.votes_received.clear();
            self.voted_for = Some(self.id);
            self.votes_received.insert(self.id);
            self.reset_election_timeout();

            let mut actions = Vec::from([Action::PersistMetadata {
                term: self.current_term,
                voted_for: self.voted_for,
            }]);
            let request = RequestVoteRPC {
                term: self.current_term,
                candidate_id: self.id,
                // SAFETY: the dummy command is added in Self::new so self.log.len() is always at
                // least 1
                last_log_index: self.log.len() as u64 - 1,
                last_log_term: self.log[self.log.len() - 1].term,
            };
            let msg = Message::RequestVote(request);
            actions.extend(self.peers.iter().map(|peer_id| Action::SendMessage {
                target: *peer_id,
                message: msg.clone(),
            }));
            actions
        } else {
            vec![]
        }
    }

    fn reset_election_timeout(&mut self) {
        let mut rng = rand::rng();
        self.ticks_since_last_heartbeat = 0;
        // reset election timeout
        self.election_timeout = rng.random_range(self.election_timeout_range.clone());
    }

    /// Function that checks if a message's term is greater than our current term. We step down as
    /// a follower if we receive a message with a higher term and then continue with whatever
    /// message we received
    fn check_msg_term(&mut self, msg_term: u64) -> Option<Vec<Action>> {
        if msg_term > self.current_term {
            self.current_term = msg_term;
            self.state = RaftState::Follower;
            self.voted_for = None;
            self.reset_election_timeout();
            Some(vec![Action::PersistMetadata {
                term: self.current_term,
                voted_for: self.voted_for,
            }])
        } else {
            None
        }
    }

    pub fn handle_request_vote(&mut self, req: RequestVoteRPC) -> Vec<Action> {
        let mut actions = Vec::new();
        if let Some(persist_action) = self.check_msg_term(req.term) {
            actions.extend(persist_action);
        }

        let false_response = Message::RequestVoteResponse(RequestVoteResponseRPC {
            id: self.id,
            term: self.current_term,
            vote_granted: false,
        });

        if req.term < self.current_term {
            // the requestor is on an earlier term, so we return false and don't vote for it!
            actions.push(Action::SendMessage {
                target: req.candidate_id,
                message: false_response,
            });
            return actions;
        }

        let can_vote = match self.voted_for {
            None => true,
            Some(id) if id == req.candidate_id => true,
            Some(_) => false,
        };
        let last_log_index = self.log.len() as u64 - 1;
        let last_log_term = self.log[self.log.len() - 1].term;
        // candidate's log must have a higher term or (if terms are equal) an index at least as
        // long as us
        let log_check = req.last_log_term > last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index);
        let grant_vote = can_vote && log_check;

        if grant_vote {
            self.reset_election_timeout();
            self.voted_for = Some(req.candidate_id);
            actions.extend([
                Action::PersistMetadata {
                    term: self.current_term,
                    voted_for: self.voted_for,
                },
                Action::SendMessage {
                    target: req.candidate_id,
                    message: Message::RequestVoteResponse(RequestVoteResponseRPC {
                        id: self.id,
                        term: self.current_term,
                        vote_granted: true,
                    }),
                },
            ]);
        } else {
            actions.push(Action::SendMessage {
                target: req.candidate_id,
                message: false_response,
            });
        }
        actions
    }

    pub fn handle_request_vote_response(&mut self, resp: RequestVoteResponseRPC) -> Vec<Action> {
        let mut actions = Vec::new();
        if let Some(persist_actions) = self.check_msg_term(resp.term) {
            actions.extend(persist_actions);
        }

        // should only get this if we are a candidate
        if !matches!(self.state, RaftState::Candidate) {
            return actions;
        }
        if resp.vote_granted {
            // received a vote for this election
            self.votes_received.insert(resp.id);

            if self.votes_received.len() as u64 > ((self.peers.len() as u64 + 1) / 2) {
                // we won the election, transition to leader state
                self.state = RaftState::Leader;
                actions.push(self.initialize_leader_state());
                self.reset_election_timeout();
                actions.extend(self.append_entries());
                actions
            } else {
                actions
            }
        } else {
            // didn't get the vote, so we just move on and do nothing
            actions
        }
    }

    pub fn handle_append_entries(&mut self, req: AppendEntriesRPC) -> Vec<Action> {
        let mut actions = Vec::new();
        if let Some(persist_actions) = self.check_msg_term(req.term) {
            actions.extend(persist_actions);
        }

        let mut response = types::AppendEntriesResponseRPC {
            id: self.id,
            term: self.current_term,
            match_index: self.log.len() as u64 - 1,
            success: false,
            first_conflicting_index: None,
            first_conflicting_term: None,
        };

        let make_response = |r: types::AppendEntriesResponseRPC| {
            let false_response = Message::AppendEntriesResponse(r);
            Action::SendMessage {
                target: req.leader_id,
                message: false_response,
            }
        };

        // check if we are a candidate and step down if the term is at least as big as ours
        // (Section 5.2)
        if req.term == self.current_term && self.state == RaftState::Candidate {
            // convert to follower and mark we didn't vote in the term (somehow, majority of
            // servers voted this server sending req to leader, so we follow suit and become a
            // follower)
            self.state = RaftState::Follower;
        }

        // ensure we are a follower
        if self.state != RaftState::Follower {
            // send nothing if we are a leader or candidate
            actions.push(make_response(response));
            return actions;
        }

        if req.term < self.current_term {
            // "Leader" is on an earlier term, so we send back a false response
            actions.push(make_response(response));
            return actions;
        }

        // reset election timeout since we have a valid leader on the right term
        self.reset_election_timeout();

        // ensure that the previous log index is actually less than our current length and at the
        // log index (the last the leader was tracking for us) matches the term
        let valid = req.prev_log_index == 0
            || (req.prev_log_index < self.log.len() as u64
                && self.log[req.prev_log_index as usize].term == req.prev_log_term);
        if !valid {
            actions.push(if req.prev_log_index >= self.log.len() as u64 {
                // this node's log is too short, so there isn't a conflicting term, we just need a
                // lot to catch up
                response.first_conflicting_index = Some(self.log.len() as u64);
                make_response(response)
            } else {
                // this node's log has a conflicting term at this index, so we backtrack until we
                // find an entry that is less than the term at the current index
                let conflicting_term = self.log[req.prev_log_index as usize].term;
                let mut conflicting_index = None;
                for i in (0..req.prev_log_index as usize).rev() {
                    if self.log[i].term != conflicting_term {
                        conflicting_index = Some(i as u64 + 1);
                        break;
                    }
                }
                response.first_conflicting_index = conflicting_index;
                response.first_conflicting_term = Some(conflicting_term);
                make_response(response)
            });
            return actions;
        }

        // now we start appending entries (and overwrite anything that has a bad term)
        // have to be careful with our good old Rust Vec

        // start writing at previous log index + 1
        let next = req.prev_log_index + 1;
        for i in next as usize..next as usize + req.entries.len() {
            let new_entry = req.entries[i - next as usize].clone();

            // check if i is less than length of log. if it is equal then we will just be appending
            // to the log. it can't be greater because that check is done above with the if !valid
            // check
            if i < self.log.len() {
                // this is an existing entry, check if it has the same term
                if self.log[i].term != new_entry.term {
                    // set the log to be everything up to put not including i (keep first i
                    // elements)
                    self.log.truncate(i);
                } else {
                    // same index and same term, so it's the same entry. just move on
                    continue;
                }
            }

            // at this point we know that i == self.log.len() meaning we can just 'push' to
            // self.log with the new entry
            self.log.push(new_entry);
        }

        // if log has changed (had entries) persist the entries starting from req.prev_log_index + 1
        if !req.entries.is_empty() {
            actions.push(Action::PersistLogEntries {
                start_index: req.prev_log_index + 1,
                entries: self.log[req.prev_log_index as usize + 1..].to_vec(),
            });
        }

        // update our commit index
        if req.leader_commit > self.commit_index {
            let old_commit = self.commit_index;
            self.commit_index = min(req.leader_commit, self.log.len() as u64 - 1);
            // we should apply all of the entries between old_commit and the new commit_index
            actions.extend(
                self.log[old_commit as usize + 1..=self.commit_index as usize]
                    .iter()
                    .map(|entry| Action::ApplyToStateMachine {
                        command: entry.command.clone(),
                    })
                    .collect::<Vec<_>>(),
            )
        }

        actions.push(Action::SendMessage {
            target: req.leader_id,
            message: Message::AppendEntriesResponse(AppendEntriesResponseRPC {
                id: self.id,
                term: self.current_term,
                match_index: self.log.len() as u64 - 1,
                success: true,
                first_conflicting_term: None,
                first_conflicting_index: None,
            }),
        });
        actions
    }

    pub fn handle_append_entries_response(
        &mut self,
        resp: AppendEntriesResponseRPC,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        if let Some(persist_actions) = self.check_msg_term(resp.term) {
            actions.extend(persist_actions);
        }

        if resp.success {
            // update the follower's match_index to the provided value in the response
            // next_index should also be updated to match_index + 1
            let _ = self.match_index.insert(resp.id, resp.match_index);
            let _ = self.next_index.insert(resp.id, resp.match_index + 1);
        } else {
            // we will update the next index to be the first conflict that the node sent us (if
            // applicable), otherwise we decrement by 1
            if let Some(ind) = resp.first_conflicting_index {
                self.next_index.entry(resp.id).and_modify(|v| *v = ind);
            } else {
                self.next_index.entry(resp.id).and_modify(|v| *v = 0);
            }
            actions.push(self.build_append_entries_for_peer(resp.id));
            return actions;
        };

        // grab the minimum value that a majority of servers has in their match_index
        let mut peer_indices: Vec<u64> = self.match_index.values().copied().collect();
        // add leader's highest index we have in the log (self.log.len())
        peer_indices.push(self.log.len() as u64 - 1);
        // sort them
        peer_indices.sort();
        // pick out the value that has been matched on a majority of servers
        let replicated_index = peer_indices[peer_indices.len() / 2];
        let prev_commit_index = self.commit_index;
        // find the highest index in our log where index.term == current_term
        let mut new_commit = prev_commit_index;
        for index in prev_commit_index as usize + 1..=replicated_index as usize {
            if self.log[index].term == self.current_term {
                new_commit = index as u64;
            }
        }

        // update commit index and apply all entries from last_applied to commit index
        self.commit_index = new_commit;
        for index in self.last_applied as usize + 1..=self.commit_index as usize {
            actions.push(Action::ApplyToStateMachine {
                command: self.log[index].command.clone(),
            });
        }
        self.last_applied = self.commit_index;

        actions
    }

    fn initialize_leader_state(&mut self) -> Action {
        // add a dummy entry in current_term to our log
        self.log.push(LogEntry {
            term: self.current_term,
            command: vec![],
        });

        for id in &self.peers {
            self.next_index.insert(*id, self.log.len() as u64);
            self.match_index.insert(*id, 0);
        }

        Action::PersistLogEntries {
            start_index: self.log.len() as u64 - 1,
            entries: self.log[self.log.len() - 1..].to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// Helper struct to manage a cluster for tests. Not all tests will leverage this (especially
    /// tests that plan to manually craft RPCs and check responses from nodes), but the majority of
    /// the "happy path" will benefit from this struct to quickly spin up a cluster and take
    /// certain actions.
    struct TestCluster {
        /// The nodes in the cluster
        nodes: HashMap<NodeId, RaftCore>,
        /// Queued messages to send (sender, receiver, Message)
        pending_messages: Vec<(NodeId, NodeId, Message)>,
        /// Set of nodes that are "partitioned" and shouldn't receive any messages in pending
        partitioned: HashSet<NodeId>,
        /// Current leader (if any)
        leader: Option<NodeId>,
    }

    impl TestCluster {
        /// Creates a test cluster with specified node_ids without electing a leader
        fn new(node_ids: &[NodeId]) -> Self {
            let mut nodes = HashMap::with_capacity(node_ids.len());
            for id in node_ids {
                nodes.insert(*id, RaftCore::new(*id, node_ids, None, 15..31));
            }
            Self {
                nodes,
                pending_messages: Vec::new(),
                partitioned: HashSet::new(),
                leader: None,
            }
        }

        /// Creates a test cluster with specified node_ids, elects the leader, and replicates and
        /// commits (across all nodes) the dummy entry inserted after leader is elected.
        fn new_with_leader(node_ids: &[NodeId], leader_id: NodeId) -> Self {
            let mut cluster = Self::new(node_ids);
            cluster.elect_leader(leader_id);
            cluster
        }

        fn node(&self, id: NodeId) -> &RaftCore {
            self.nodes.get(&id).unwrap()
        }

        fn node_mut(&mut self, id: NodeId) -> &mut RaftCore {
            self.nodes.get_mut(&id).unwrap()
        }

        fn tick_until_candidate(&mut self, id: NodeId) {
            let node = self.nodes.get_mut(&id).unwrap();
            for _ in 0..node.election_timeout - 1 {
                node.tick();
            }
        }

        fn tick_until_heartbeat(&mut self, id: NodeId) {
            let node = self.nodes.get_mut(&id).unwrap();
            for _ in 0..node.heartbeat_interval - 1 {
                node.tick();
            }
        }

        fn tick_node(&mut self, id: NodeId) -> Vec<Action> {
            let node = self.nodes.get_mut(&id).unwrap();
            node.tick()
        }

        fn elect_leader(&mut self, id: NodeId) {
            self.tick_until_candidate(id);
            let actions = self.tick_node(id);
            // get those actions collecting into pending messages
            self.collect_actions(id, actions);
            // get the request_votes to send out to everyone
            let _ = self.deliver_all();
            // force the request vote responses to get back to the leader
            // note that this will queue up any new messages (like AppendEntries) from the leader
            // upon election
            let _ = self.deliver_all();
            // ensure the leader is now actually the leader
            assert_eq!(self.node(id).state, RaftState::Leader);
            self.leader = Some(id);

            // deliver append entries queued up
            let _ = self.deliver_all();
            // get responses back to the leader (who will then commit)
            let _ = self.deliver_all();
            // all responses will FAIL because prev_log_index is 1 (based on leader's log)

            // do another append entries (heartbeat) to get followers synced and deliver the dummy
            // entry
            let actions = self.node_mut(id).append_entries();
            self.collect_actions(id, actions);
            let _ = self.deliver_all();
            // clear out pending with the responses to the heartbeat to start clean
            let _ = self.deliver_all();

            assert_eq!(self.node(id).commit_index, 1);

            // now, to get the followers to sync their commit index as well, we need to do another
            // heartbeat for them to learn that the leader committed
            let actions = self.node_mut(id).append_entries();
            self.collect_actions(id, actions);
            let _ = self.deliver_all();
            let _ = self.deliver_all();
            // cluster is fully synced
            for node_id in self.nodes.keys() {
                assert_eq!(self.node(*node_id).commit_index, 1);
            }
        }

        /// Drains the pending_messages queue and delivers all messages to recipients. Queues up
        /// any messages that result from delivery and places them in pending_messages (once the
        /// function returns) and returns any Action variants that are not SendMessage
        fn deliver_all(&mut self) -> HashMap<NodeId, Vec<Action>> {
            let mut results: HashMap<NodeId, Vec<Action>> = HashMap::new();

            // safety limit of 50 messages will be delivered
            let pending = if self.pending_messages.len() > 50 {
                self.pending_messages.drain(..50).collect()
            } else {
                // take it all and leave a new Vec
                std::mem::take(&mut self.pending_messages)
            };

            for (_, recipient, msg) in pending {
                // skip partitioned nodes from receiving messages
                if self.partitioned.contains(&recipient) {
                    continue;
                }

                // deliver the message
                let actions = match msg {
                    Message::RequestVote(req) => self.node_mut(recipient).handle_request_vote(req),
                    Message::RequestVoteResponse(resp) => {
                        self.node_mut(recipient).handle_request_vote_response(resp)
                    }
                    Message::AppendEntries(req) => {
                        self.node_mut(recipient).handle_append_entries(req)
                    }
                    Message::AppendEntriesResponse(resp) => self
                        .node_mut(recipient)
                        .handle_append_entries_response(resp),
                };
                let mut new_actions = self.collect_actions(recipient, actions);
                results
                    .entry(recipient)
                    .or_default()
                    .extend(new_actions.remove(&recipient).unwrap_or_default());
            }

            results
        }

        /// Delivers all messages destined "to" target (where target is the recipient). Respects
        /// paritioning as well. This will queue any new messages that result in the delivery and
        /// return any non-SendMessage Actions.
        fn deliver_to(&mut self, target: NodeId) -> HashMap<NodeId, Vec<Action>> {
            let mut results = HashMap::from([(target, vec![])]);

            let pending = std::mem::take(&mut self.pending_messages);

            let (to_target, other) = pending
                .into_iter()
                .partition(|(_from, to, _msg)| *to == target);

            // reset the Vec with the messages that aren't for target
            let _ = std::mem::replace(&mut self.pending_messages, other);

            // now, process the messages to add new messages in the queue
            for (_, recipient, msg) in to_target {
                if self.partitioned.contains(&recipient) {
                    continue;
                }

                let actions = match msg {
                    Message::RequestVote(req) => self.node_mut(recipient).handle_request_vote(req),
                    Message::RequestVoteResponse(resp) => {
                        self.node_mut(recipient).handle_request_vote_response(resp)
                    }
                    Message::AppendEntries(req) => {
                        self.node_mut(recipient).handle_append_entries(req)
                    }
                    Message::AppendEntriesResponse(resp) => self
                        .node_mut(recipient)
                        .handle_append_entries_response(resp),
                };
                results.entry(target).or_default().extend(
                    self.collect_actions(recipient, actions)
                        .remove(&target)
                        .unwrap_or_default(),
                );
            }

            results
        }

        /// Removes SendMessage Action's and places them in pending and returns any non-SendMessage
        /// Actions. Messages added to pending can be delivered with deliver_all() or deliver_to().
        /// This function will respect partitions, meaning if from is currently partitioned, no
        /// messages will be added to pending (because the node cannot send messages).
        fn collect_actions(
            &mut self,
            from: NodeId,
            actions: Vec<Action>,
        ) -> HashMap<NodeId, Vec<Action>> {
            let (messages, other) = actions.into_iter().partition(|action| {
                matches!(
                    action,
                    Action::SendMessage {
                        target: _,
                        message: _
                    }
                )
            });

            if self.partitioned.contains(&from) {
                // drop messages if the node is currently partioned
                return HashMap::from([(from, other)]);
            }

            self.pending_messages
                .extend(messages.into_iter().map(|a| match a {
                    Action::SendMessage { target, message } => (from, target, message),
                    _ => panic!("Expected a SendMessage action only!"),
                }));

            HashMap::from([(from, other)])
        }

        /// Sends a proposal to the given leader, panics if there isn't one
        fn propose(&mut self, command: Vec<u8>) -> Vec<Action> {
            let leader_id = self.leader.expect("No leader for cluster");
            let actions = self.node_mut(leader_id).propose(command).unwrap();
            self.collect_actions(leader_id, actions)
                .remove(&leader_id)
                .unwrap_or_default()
        }

        /// Proposes a command to the leader and fully replicates the command across the cluster so
        /// everyone is committed. Gives back any additional Action's (likely ApplyToStateMachine)
        /// for every node with the specified NodeId.
        fn propose_and_sync(&mut self, command: Vec<u8>) -> HashMap<NodeId, Vec<Action>> {
            // propose to the leader the new command and get the actions
            let leader_id = self.leader.expect("No leader for cluster");
            let actions = self.node_mut(leader_id).propose(command).unwrap();
            let _ = self.collect_actions(leader_id, actions);
            // deliver to nodes
            let _ = self.deliver_all();
            // now, everyone would have responses in the queue, so deliver back to leader (leader
            // may have apply to state machine because it has received a majority of resposnes)
            let mut leader_actions = self.deliver_all();

            // now leader should have advanced commit index, so force an append entries to go out
            // so followers know as well
            let append_entries = self.node_mut(leader_id).append_entries();
            let _ = self.collect_actions(leader_id, append_entries);
            let follower_actions = self.deliver_all();

            let mut combined = HashMap::new();
            combined.insert(
                leader_id,
                leader_actions.remove(&leader_id).unwrap_or_default(),
            );
            for (follower_id, actions) in follower_actions {
                combined.insert(follower_id, actions);
            }
            combined
        }

        /// Adds a node to the partitioned set.
        fn partition(&mut self, id: NodeId) {
            self.partitioned.insert(id);
        }

        /// Removes a node from the partitioned set.
        fn heal(&mut self, id: NodeId) -> bool {
            self.partitioned.remove(&id)
        }
    }

    /// Ticks a node until it is one tick away from hitting the election timeout
    fn tick_until_candidate(node: &mut RaftCore) {
        for _ in 0..node.election_timeout - 1 {
            node.tick();
        }
    }

    fn extract_request_votes(actions: Vec<Action>) -> Vec<(NodeId, RequestVoteRPC)> {
        let mut request_votes = Vec::with_capacity(actions.len());
        for a in actions {
            match a {
                Action::SendMessage { target, message } => {
                    if let Message::RequestVote(req) = message {
                        request_votes.push((target, req));
                    }
                }
                _ => {}
            }
        }
        request_votes
    }

    /// Only called for a single node's actions
    fn extract_request_vote_responses(
        actions: Vec<Action>,
    ) -> Vec<(NodeId, RequestVoteResponseRPC)> {
        let mut responses = vec![];
        for a in actions {
            match a {
                Action::SendMessage { target, message } => {
                    if let Message::RequestVoteResponse(resp) = message {
                        responses.push((target, resp));
                    }
                }
                _ => {}
            }
        }
        responses
    }

    fn deliver_request_votes(
        mut nodes: HashMap<NodeId, &mut RaftCore>,
        messages: Vec<(NodeId, RequestVoteRPC)>,
    ) -> Vec<(NodeId, RequestVoteResponseRPC)> {
        let mut responses = vec![];
        for (id, msg) in messages {
            if let Some(node) = nodes.get_mut(&id) {
                let resp_actions = node.handle_request_vote(msg);
                responses.push(extract_request_vote_responses(resp_actions));
            }
        }
        responses.into_iter().flatten().collect()
    }

    fn deliver_request_vote_responses(
        candidate: &mut RaftCore,
        messages: Vec<(NodeId, RequestVoteResponseRPC)>,
    ) {
        let candidate_id = candidate.id;
        for (_, msg) in messages.iter().filter(|(id, _)| *id == candidate_id) {
            candidate.handle_request_vote_response(msg.clone());
        }
    }

    fn deliver_request_vote_responses_with_actions(
        candidate: &mut RaftCore,
        messages: Vec<(NodeId, RequestVoteResponseRPC)>,
    ) -> Vec<Action> {
        let candidate_id = candidate.id;
        let mut actions = Vec::with_capacity(messages.len());
        for (_, msg) in messages.iter().filter(|(id, _)| *id == candidate_id) {
            actions.push(candidate.handle_request_vote_response(msg.clone()));
        }
        actions.into_iter().flatten().collect()
    }

    #[test]
    fn create_new_raft_core() {
        let nodes = [1, 2, 3];

        let node = RaftCore::new(1, &nodes, None, 15..31);

        assert_eq!(node.id, 1);
    }

    fn init_three_node_noop_cluster() -> (RaftCore, RaftCore, RaftCore) {
        let nodes = [1, 2, 3];
        let node1 = RaftCore::new(1, &nodes, None, 15..31);
        let node2 = RaftCore::new(2, &nodes, None, 15..31);
        let node3 = RaftCore::new(3, &nodes, None, 15..31);
        // make sure we all start as followers
        assert!(node1.state == RaftState::Follower);
        assert!(node2.state == RaftState::Follower);
        assert!(node3.state == RaftState::Follower);

        (node1, node2, node3)
    }

    fn init_five_node_noop_cluster() -> (RaftCore, RaftCore, RaftCore, RaftCore, RaftCore) {
        let nodes = [1, 2, 3, 4, 5];
        let node1 = RaftCore::new(1, &nodes, None, 15..31);
        let node2 = RaftCore::new(2, &nodes, None, 15..31);
        let node3 = RaftCore::new(3, &nodes, None, 15..31);
        let node4 = RaftCore::new(4, &nodes, None, 15..31);
        let node5 = RaftCore::new(5, &nodes, None, 15..31);
        // make sure we all start as followers
        assert!(node1.state == RaftState::Follower);
        assert!(node2.state == RaftState::Follower);
        assert!(node3.state == RaftState::Follower);
        assert!(node4.state == RaftState::Follower);
        assert!(node5.state == RaftState::Follower);

        (node1, node2, node3, node4, node5)
    }

    #[test]
    fn raft_initial_election() {
        // Make a TestCluster with three nodes and make node 1 the leader, check everyone's terms
        // and states is correct
        let cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        assert_eq!(cluster.node(1).state, RaftState::Leader);
        assert_eq!(cluster.node(1).current_term, 1);

        assert_eq!(cluster.node(2).state, RaftState::Follower);
        assert_eq!(cluster.node(3).state, RaftState::Follower);
        assert_eq!(cluster.node(2).current_term, 1);
        assert_eq!(cluster.node(3).current_term, 1);
    }

    #[test]
    fn request_vote_correct_log_info() {
        let (mut leader, mut node2, mut node3) = init_three_node_noop_cluster();

        // get some log entries in the leader (don't really care that they match the followers)
        let mut entry = LogEntry {
            term: 1,
            command: DUMMY.to_vec(),
        };
        leader.log.push(entry.clone());
        entry.term = 2;
        leader.log.push(entry);

        tick_until_candidate(&mut leader);
        let actions = leader.tick();
        let vote_requests = extract_request_votes(actions);

        // ensure the RequestVote's have last_log_index == 2 and term is 2 as well
        assert_eq!(
            vote_requests
                .iter()
                .filter(|(_, req)| req.last_log_index == 2)
                .count(),
            2
        );
        assert_eq!(
            vote_requests
                .iter()
                .filter(|(_, req)| req.last_log_term == 2)
                .count(),
            2
        );
    }

    #[test]
    fn test_simultaneous_election() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();

        tick_until_candidate(&mut node1);
        tick_until_candidate(&mut node2);

        let node1_actions = node1.tick();
        let node2_actions = node2.tick();

        let node1_msgs = extract_request_votes(node1_actions);
        let node2_msgs = extract_request_votes(node2_actions);

        // deliver node1 messages to node2 and node3
        let node_map = HashMap::from([(2, &mut node2), (3, &mut node3)]);
        let responses_for_node1 = deliver_request_votes(node_map, node1_msgs);

        let node_map = HashMap::from([(1, &mut node1), (3, &mut node3)]);
        let responses_for_node2 = deliver_request_votes(node_map, node2_msgs);

        // now we have to give node 1 and 2 the responses (giving node1 the responses first) and
        // check to see that node1 will be the leader at the end and node2 should not assume
        // leadership (because node3 should not have voted for node2!)
        deliver_request_vote_responses(&mut node1, responses_for_node1);

        assert!(node1.state == RaftState::Leader);

        // before delivering to 2:
        // find response from node3 to node2 and ensure it did not grant the vote (could be a
        // separate test, but avoids the setup)
        for (id, msg) in &responses_for_node2 {
            if *id == 3 {
                assert!(!msg.vote_granted);
            }
        }
        // and make sure node3 voted_for is set for the term
        assert_eq!(node3.voted_for, Some(1));

        deliver_request_vote_responses(&mut node2, responses_for_node2);

        // node2 should still be a candidate and only one entry in votes (itself)
        assert!(node2.state == RaftState::Candidate);
        assert_eq!(node2.votes_received.len(), 1);
    }

    #[test]
    fn node_receives_request_vote_lower_term() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();

        // set each node's term to be something higher than 0
        node1.current_term = 2;
        node2.current_term = 2;
        node3.current_term = 2;

        // send a RequestVote from node1 to node2 with a term as 1 (older term)
        let msg = RequestVoteRPC {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
        let node2_actions = node2.handle_request_vote(msg);
        if let Action::SendMessage { target, message } = &node2_actions[0] {
            if let Message::RequestVoteResponse(response) = message {
                assert_eq!(*target, 1);
                assert!(!response.vote_granted);
            } else {
                panic!("Should have received a RequestVoteResponse");
            }
        } else {
            panic!("Should have an SendMessage Action");
        }
    }

    #[test]
    fn candidate_steps_down_with_higher_term_request_vote() {
        // a candidate will be seeking votes and receive a RequestVote RPC from another node with a
        // higher term. in that case, the candidate should revert to a follower _and_ vote for that
        // new node
        let (mut node1, _node2, _node3) = init_three_node_noop_cluster();
        // make node1 a candidate
        tick_until_candidate(&mut node1);
        let _ = node1.tick();
        assert!(node1.state == RaftState::Candidate);

        let next_term = node1.current_term + 1;

        // node1 now is seeking to be the leader, craft a dummy RequestVote RPC with a higher term
        // than node1's election and see if it steps down
        let msg = RequestVoteRPC {
            term: next_term,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let node1_actions = node1.handle_request_vote(msg);
        // node1 should now be a Follower, have voted for node2 and be on term next_term
        assert!(node1.state == RaftState::Follower);
        assert_eq!(node1.voted_for, Some(2));
        assert_eq!(node1.current_term, next_term);

        if let Action::SendMessage { target: _, message } = &node1_actions[0] {
            if let Message::RequestVoteResponse(response) = message {
                assert!(response.vote_granted);
            }
        }
    }

    #[test]
    fn candidate_hits_election_timeout() {
        // node1 should be a candidate and will not get its votes delivered and then should start a
        // NEW election once it hits the election timeout. We will have node2 and 3 also
        // participate so that on the second election node1 becomes leader (ensures node2 and 3
        // respond correctly to the scenario as well)
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();

        tick_until_candidate(&mut node1);
        let actions = node1.tick();
        assert!(node1.state == RaftState::Candidate);
        assert_eq!(node1.current_term, 1);
        // node1 is a candidate, send requests to node2 / node3

        let vote_requests = extract_request_votes(actions);
        let node_map = HashMap::from([(2, &mut node2), (3, &mut node3)]);
        let _responses = deliver_request_votes(node_map, vote_requests);
        assert_eq!(node2.current_term, 1);
        assert_eq!(node2.voted_for, Some(1));
        assert_eq!(node3.current_term, 1);
        assert_eq!(node3.voted_for, Some(1));

        // these responses are never delivered to node1! trigger node1 to another timeout
        tick_until_candidate(&mut node1);
        let actions = node1.tick();
        // node1 should still be a candidate but should have moved to term 2
        assert!(node1.state == RaftState::Candidate);
        assert_eq!(node1.current_term, 2);

        let vote_requests = extract_request_votes(actions);
        let node_map = HashMap::from([(2, &mut node2), (3, &mut node3)]);
        let responses = deliver_request_votes(node_map, vote_requests);
        // node 2 and 3 should now have voted again and be in term 2
        assert_eq!(node2.current_term, 2);
        assert_eq!(node2.voted_for, Some(1));
        assert_eq!(node3.current_term, 2);
        assert_eq!(node3.voted_for, Some(1));

        // now deliver these to node1 and check it is the leader
        deliver_request_vote_responses(&mut node1, responses);

        assert!(node1.state == RaftState::Leader);
        assert_eq!(node1.current_term, 2);
    }

    #[test]
    fn candidate_receives_higher_term_response() {
        // node1 will become a candidate and receive a RequestVoteResponse with a higher term
        // (term 2) -> should step down and become a follower
        let (mut node1, _node2, _node3) = init_three_node_noop_cluster();

        tick_until_candidate(&mut node1);
        let _ = node1.tick();

        let next_term = node1.current_term + 1;

        // create a fake response from node2 to node1 with a higher term (node2 would not vote for
        // node1 if in a higher term)
        let msg = RequestVoteResponseRPC {
            id: 2,
            term: next_term,
            vote_granted: false,
        };
        let _ = node1.handle_request_vote_response(msg);
        // node1 should now be a Follower, have voted for node2 and be on term next_term
        assert!(node1.state == RaftState::Follower);
        assert_eq!(node1.current_term, next_term);
    }

    #[test]
    fn candidate_receives_duplicate_vote() {
        // node1 will become a candidate and receive a duplicate vote from node2, which should not
        // cause it to think it won the election in a 5-node cluster
        let (mut node1, mut node2, mut node3, mut node4, mut node5) = init_five_node_noop_cluster();

        tick_until_candidate(&mut node1);
        let actions = node1.tick();

        let vote_requests = extract_request_votes(actions);
        // deliver to every node
        let node_map = HashMap::from([
            (2, &mut node2),
            (3, &mut node3),
            (4, &mut node4),
            (5, &mut node5),
        ]);

        let responses = deliver_request_votes(node_map, vote_requests);
        // only let node2 responses get delivered to 1 and ensure that it doesn't think it won. in
        // this election, 3 votes is the winner, but can't say we won with a duplicate node2 vote
        let (_, node2_resp) = responses
            .iter()
            .find(|(_, response)| response.id == 2)
            .unwrap();

        // check node1 is a candidate
        assert!(node1.state == RaftState::Candidate);
        node1.handle_request_vote_response(node2_resp.clone());
        // check we have two votes now
        assert_eq!(node1.votes_received.len(), 2);
        // deliver next
        node1.handle_request_vote_response(node2_resp.clone());
        // should still be a candidate and only have 2 votes received
        assert!(node1.state == RaftState::Candidate);
        assert_eq!(node1.votes_received.len(), 2);
    }

    #[test]
    fn leader_steps_down() {
        // this test ensures that a leader will step down if there is a RequestVotes with a higher
        // term
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();

        tick_until_candidate(&mut node1);
        assert!(node1.state == RaftState::Follower);

        // tick the node1 to trigger an election
        let actions = node1.tick();
        // ensure node1 is a candidate now and we have actions to take
        assert!(node1.state == RaftState::Candidate);
        assert!(!actions.is_empty());

        let vote_requests = extract_request_votes(actions);
        let node_map = HashMap::from([(2, &mut node2), (3, &mut node3)]);

        let responses = deliver_request_votes(node_map, vote_requests);
        deliver_request_vote_responses(&mut node1, responses);

        // should be a node1 and have our term be == 1
        assert!(node1.state == RaftState::Leader);
        assert_eq!(node1.current_term, 1);

        // now, let's get node2 to become a leader and send a RequestVote with a higher term to
        // node1 (node1 may have been in a separate network partition or something similar)
        tick_until_candidate(&mut node2);
        let actions = node2.tick();

        let vote_requests = extract_request_votes(actions);
        let node_map = HashMap::from([(1, &mut node1), (3, &mut node3)]);

        // deliver the request vote to the nodes and ensure node1 steps down and is now a follower
        let _ = deliver_request_votes(node_map, vote_requests);

        assert!(node1.state == RaftState::Follower);
    }

    /// Make provided ID the leader
    fn make_leader(leader: &mut RaftCore, nodes: HashMap<NodeId, &mut RaftCore>) {
        tick_until_candidate(leader);
        let actions = leader.tick();
        let vote_requests = extract_request_votes(actions);
        let responses = deliver_request_votes(nodes, vote_requests);
        deliver_request_vote_responses(leader, responses);
        assert!(leader.state == RaftState::Leader);
    }

    fn make_leader_with_actions(
        leader: &mut RaftCore,
        nodes: HashMap<NodeId, &mut RaftCore>,
    ) -> Vec<Action> {
        tick_until_candidate(leader);
        let actions = leader.tick();
        let vote_requests = extract_request_votes(actions);
        let responses = deliver_request_votes(nodes, vote_requests);
        let actions = deliver_request_vote_responses_with_actions(leader, responses);
        assert!(leader.state == RaftState::Leader);
        actions
    }

    /// Ticks a node until heartbeat interval
    fn tick_until_heartbeat(node: &mut RaftCore) {
        for _ in 0..node.heartbeat_interval - 1 {
            node.tick();
        }
    }

    #[test]
    fn leader_sends_heartbeat_at_interval() {
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        // tick node 1 until the heartbeat
        cluster.tick_until_heartbeat(1);
        // check that on the next tick, the node will produce 2 empty AppendEntries
        let hb_actions = cluster.tick_node(1);
        // get the actions into pending
        let _ = cluster.collect_actions(1, hb_actions);
        assert_eq!(cluster.pending_messages.len(), 2);
        // check that the AppendEntries itself has no new entries (no propose between leadership
        // election and the heartbeat)
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(_from, _to, msg)| match msg {
                    Message::AppendEntries(req) => req.entries.is_empty(),
                    _ => false,
                })
                .count(),
            2
        );
    }

    fn extract_append_entries(actions: Vec<Action>) -> Vec<(NodeId, AppendEntriesRPC)> {
        let mut append_entries = Vec::with_capacity(actions.len());
        for a in actions {
            match a {
                Action::SendMessage { target, message } => {
                    if let Message::AppendEntries(req) = message {
                        append_entries.push((target, req));
                    }
                }
                _ => {}
            }
        }
        append_entries
    }

    fn extract_append_entries_responses(
        actions: Vec<Action>,
    ) -> Vec<(NodeId, AppendEntriesResponseRPC)> {
        let mut responses = vec![];
        for a in actions {
            match a {
                Action::SendMessage { target, message } => {
                    if let Message::AppendEntriesResponse(resp) = message {
                        responses.push((target, resp));
                    }
                }
                _ => {}
            }
        }
        responses
    }

    fn deliver_append_entries(
        mut nodes: HashMap<NodeId, &mut RaftCore>,
        messages: Vec<(NodeId, AppendEntriesRPC)>,
    ) -> Vec<(NodeId, AppendEntriesResponseRPC)> {
        let mut responses = vec![];
        for (id, msg) in messages {
            if let Some(node) = nodes.get_mut(&id) {
                let resp_actions = node.handle_append_entries(msg);
                responses.push(extract_append_entries_responses(resp_actions));
            }
        }
        responses.into_iter().flatten().collect()
    }

    fn deliver_append_entries_responses(
        leader: &mut RaftCore,
        messages: Vec<(NodeId, AppendEntriesResponseRPC)>,
    ) {
        let leader_id = leader.id;
        for (_, msg) in messages.iter().filter(|(id, _)| *id == leader_id) {
            leader.handle_append_entries_response(msg.clone());
        }
    }

    fn deliver_append_entries_responses_with_actions(
        leader: &mut RaftCore,
        messages: Vec<(NodeId, AppendEntriesResponseRPC)>,
    ) -> Vec<Action> {
        let leader_id = leader.id;
        let mut actions = Vec::with_capacity(messages.len());
        for (_, msg) in messages.iter().filter(|(id, _)| *id == leader_id) {
            actions.push(leader.handle_append_entries_response(msg.clone()));
        }
        actions.into_iter().flatten().collect()
    }

    #[test]
    fn follower_resets_election_timeout() {
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        // heartbeat node1
        cluster.tick_until_heartbeat(1);
        let actions = cluster.tick_node(1);
        // get the actions into pending
        let _ = cluster.collect_actions(1, actions);

        // tick node2 and 3 to move their tick counter
        cluster.tick_node(2);
        cluster.tick_node(3);
        assert!(cluster.node(2).ticks_since_last_heartbeat > 0);
        assert!(cluster.node(3).ticks_since_last_heartbeat > 0);

        // deliver append entries to the nodes
        cluster.deliver_all();

        // ensure that nodes reset timers and responded successfully to the AppendEntries
        assert_eq!(cluster.node(2).ticks_since_last_heartbeat, 0);
        assert_eq!(cluster.node(3).ticks_since_last_heartbeat, 0);

        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(_, _, msg)| match msg {
                    Message::AppendEntriesResponse(resp) => resp.success,
                    _ => false,
                })
                .count(),
            2
        );
    }

    #[test]
    fn follower_rejects_heartbeat_with_lower_term() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        make_leader(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );

        // everyone is on term 1 at this point...
        assert_eq!(node1.current_term, 1);
        assert_eq!(node2.current_term, 1);
        assert_eq!(node3.current_term, 1);

        // send an AppendEntries (manually created) with term 0 to node2 and make sure they respond
        // false
        let msg = AppendEntriesRPC {
            term: 0,
            leader_id: node1.id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        let action = node2.handle_append_entries(msg);
        assert_eq!(action.len(), 1);

        if let Action::SendMessage { target: _, message } = &action[0] {
            if let Message::AppendEntriesResponse(resp) = message {
                assert!(!resp.success)
            } else {
                panic!("Did not get AppendEntriesResponseRPC");
            }
        } else {
            panic!("Did not get Action::SendMessage");
        }
    }

    #[test]
    fn candidate_steps_down_with_equal_term_append_entries() {
        // a candidate will be seeking votes and receive a AppendEntries RPC from another node with an
        // equal term. in that case, the candidate should revert to a follower
        let (mut node1, _node2, _node3) = init_three_node_noop_cluster();
        // make node1 a candidate
        tick_until_candidate(&mut node1);
        let _ = node1.tick();
        assert!(node1.state == RaftState::Candidate);

        // node1 is seeking to be the leader, but gets an AppendEntries from node2 with equal term
        let msg = AppendEntriesRPC {
            term: node1.current_term,
            leader_id: 2,
            prev_log_term: 0,
            prev_log_index: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let node1_actions = node1.handle_append_entries(msg);
        // node1 should now be a Follower be on term next_term
        assert!(node1.state == RaftState::Follower);

        if let Action::SendMessage { target: _, message } = &node1_actions[0] {
            if let Message::AppendEntriesResponse(resp) = message {
                assert!(resp.success);
            } else {
                panic!("No AppendEntriesResponse");
            }
        } else {
            panic!("No SendMessage Action");
        }
    }

    #[test]
    fn candidate_steps_down_with_higher_term_append_entries() {
        // a candidate will be seeking votes and receive a AppendEntries RPC from another node with a
        // higher term. in that case, the candidate should revert to a follower
        let (mut node1, _node2, _node3) = init_three_node_noop_cluster();
        // make node1 a candidate
        tick_until_candidate(&mut node1);
        let _ = node1.tick();
        assert!(node1.state == RaftState::Candidate);

        let next_term = node1.current_term + 1;

        // node1 is seeking to be the leader, but gets an AppendEntries from node2 with higher term
        let msg = AppendEntriesRPC {
            term: next_term,
            leader_id: 2,
            prev_log_term: 0,
            prev_log_index: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let node1_actions = node1.handle_append_entries(msg);
        // node1 should now be a Follower be on term next_term
        assert!(node1.state == RaftState::Follower);
        assert_eq!(node1.current_term, next_term);

        assert_eq!(
            node1_actions
                .iter()
                .filter(|action| match action {
                    Action::SendMessage {
                        target: _,
                        message: Message::AppendEntriesResponse(resp),
                    } => {
                        resp.success
                    }
                    _ => false,
                })
                .count(),
            1
        );
    }

    #[test]
    fn correct_next_match_index() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        make_leader(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );

        // next_index for each node is going to be 2 because of the dummy entry
        for node in [2, 3] {
            assert_eq!(node1.next_index.get(&node).unwrap(), &2);
            assert_eq!(node1.match_index.get(&node).unwrap(), &0);
        }
    }

    const DUMMY: [u8; 13] = [
        b'D', b'U', b'M', b'M', b'Y', b'_', b'C', b'O', b'M', b'M', b'A', b'N', b'D',
    ];

    #[test]
    fn basic_propose_test() {
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        // this will place messages in pending_messages
        let _ = cluster.propose(DUMMY.to_vec());

        let node1 = cluster.node(1);
        // ensure node1 added this DUMMY_COMMAND to the log (last index) and it's term is the
        // current_term (should be 1)
        assert_eq!(node1.current_term, 1);
        assert_eq!(node1.log[node1.log.len() - 1].term, node1.current_term);
        assert_eq!(node1.log[node1.log.len() - 1].command, DUMMY.to_vec());
        // ensure we have 2 append entries to send out
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(_, _, msg)| matches!(msg, Message::AppendEntries(_)))
                .count(),
            2
        );
    }

    #[test]
    fn followers_append_entries() {
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        // queues messages in pending_messages
        cluster.propose(DUMMY.to_vec());

        // deliver messages to the followers
        cluster.deliver_all();

        // ensure that the followers have appended the command to their logs
        let node2 = cluster.node(2);
        let node3 = cluster.node(3);
        assert_eq!(node2.log[node2.log.len() - 1].term, 1);
        assert_eq!(node2.log[node2.log.len() - 1].command, DUMMY.to_vec());
        assert_eq!(node3.log[node3.log.len() - 1].term, 1);
        assert_eq!(node3.log[node3.log.len() - 1].command, DUMMY.to_vec());
    }

    #[test]
    fn full_append_entries_message() {
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        // propose a new command and sync it across the cluster
        let actions = cluster.propose_and_sync(DUMMY.to_vec());

        // check node2 and node3 have their next and match index updated in the leader
        assert_eq!(
            cluster
                .node(cluster.leader.unwrap())
                .next_index
                .get(&2)
                .unwrap(),
            &3
        );
        assert_eq!(
            cluster
                .node(cluster.leader.unwrap())
                .match_index
                .get(&2)
                .unwrap(),
            &2
        );
        assert_eq!(
            cluster
                .node(cluster.leader.unwrap())
                .next_index
                .get(&3)
                .unwrap(),
            &3
        );
        assert_eq!(
            cluster
                .node(cluster.leader.unwrap())
                .match_index
                .get(&3)
                .unwrap(),
            &2
        );

        // leader should have committed this index (2; dummy entry)
        assert_eq!(cluster.node(cluster.leader.unwrap()).commit_index, 2);
        let leader_actions = actions.get(&cluster.leader.unwrap()).unwrap();
        assert_eq!(leader_actions.len(), 1);
        assert!(matches!(
            leader_actions[0],
            Action::ApplyToStateMachine { command: _ }
        ));
    }

    #[test]
    fn follower_commit_advancement() {
        // ensure that a follower advances its commit index when it has appended an entry to its
        // log and receives a new AppendEntries RPC (like a heartbeat, for example)
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        let actions = make_leader_with_actions(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );
        append_entries_loop(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            actions,
        );
        let actions = node1.append_entries();
        append_entries_loop(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            actions,
        );

        let Some(actions) = node1.propose(DUMMY.to_vec()) else {
            panic!("Node used to call propose not a leader");
        };

        let append_entries = extract_append_entries(actions);
        // deliver append entries to the followers
        let responses = deliver_append_entries(
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            append_entries,
        );

        for (_, resp) in responses {
            node1.handle_append_entries_response(resp);
        }

        // followers should have a commit_index of 1 (because of dummy entry)
        assert_eq!(node2.commit_index, 1);
        assert_eq!(node3.commit_index, 1);

        // now, the leader will send out a heartbeat message and we should see all follower's
        // commit_index move from 0 to 1
        // manually set the heartbeat to fire
        node1.ticks_since_last_heartbeat = node1.heartbeat_interval;
        let heartbeat = node1.heartbeat();
        let append_entries = extract_append_entries(heartbeat);
        assert_eq!(append_entries.len(), 2);

        // manually deliver the AppendEntries so we can verify that the followers give two actions
        // in response (one send message and one apply to state machine)
        for (id, msg) in append_entries {
            let actions = if id == 2 {
                node2.handle_append_entries(msg)
            } else {
                node3.handle_append_entries(msg)
            };
            assert_eq!(actions.len(), 2);
            for a in actions {
                match a {
                    Action::SendMessage { target, message: _ } => {
                        assert_eq!(target, node1.id);
                    }
                    Action::ApplyToStateMachine { command } => {
                        assert_eq!(command, DUMMY.to_vec());
                    }
                    _ => panic!("Unexpected Action"),
                }
            }
        }

        // now that the followers received AppendEntries, they should have their commit_index set
        // to 2
        assert_eq!(node2.commit_index, 2);
        assert_eq!(node3.commit_index, 2);

        // propose and sync a command across the cluster
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        // propose a new command and sync it across the cluster
        let actions = cluster.propose_and_sync(DUMMY.to_vec());

        // check that the follower's have ApplyToStateMachine Actions and that they advanced their
        // commit index to 2
        assert_eq!(cluster.node(2).commit_index, 2);
        let node_actions = actions.get(&2).unwrap();
        assert_eq!(node_actions.len(), 1);
        assert!(matches!(
            node_actions[0],
            Action::ApplyToStateMachine { command: _ }
        ));
        assert_eq!(cluster.node(3).commit_index, 2);
        let node_actions = actions.get(&3).unwrap();
        assert_eq!(node_actions.len(), 1);
        assert!(matches!(
            node_actions[0],
            Action::ApplyToStateMachine { command: _ }
        ));
    }

    #[test]
    fn leader_commit_three_node_cluster() {
        // this test partitions one node, but shows that with just one node responding to a
        // proposed command, the leader will still commit because the entry is replicated across a
        // majority of the cluster
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);
        // partition node3 off
        cluster.partition(3);

        // propose and attempt to sync across the entire cluster; only node1 and 2 will have their
        // commit_index set to 2, 3 will be partitioned
        let actions = cluster.propose_and_sync(DUMMY.to_vec());
        let leader_actions = actions
            .get(&cluster.leader.expect("No leader for cluster"))
            .unwrap();
        assert_eq!(leader_actions.len(), 1);
        assert!(matches!(
            leader_actions[0],
            Action::ApplyToStateMachine { command: _ }
        ));
        assert_eq!(
            cluster
                .node(cluster.leader.expect("No leader for cluster"))
                .commit_index,
            2
        );
        assert_eq!(cluster.node(2).commit_index, 2);
        assert_eq!(cluster.node(3).commit_index, 1);
    }

    #[test]
    fn leader_commit_five_node_cluster() {
        // same test as leader_commit_three_node_cluster, just with five nodes to test our majority
        // computation still holds
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3, 4, 5], 1);
        // partition node4 and 5 off
        cluster.partition(4);
        cluster.partition(5);
        let actions = cluster.propose_and_sync(DUMMY.to_vec());
        let leader_actions = actions
            .get(&cluster.leader.expect("No leader for cluster"))
            .unwrap();
        assert_eq!(leader_actions.len(), 1);
        assert!(matches!(
            leader_actions[0],
            Action::ApplyToStateMachine { command: _ }
        ));
        // leader should be at 2 for commit_index
        assert_eq!(
            cluster
                .node(cluster.leader.expect("No leader for cluster"))
                .commit_index,
            2
        );
        // node2 and node3 also at 2
        assert_eq!(cluster.node(2).commit_index, 2);
        assert_eq!(cluster.node(3).commit_index, 2);
        // node4 and node5 still have a commit_index of 1
        assert_eq!(cluster.node(4).commit_index, 1);
        assert_eq!(cluster.node(5).commit_index, 1);
    }

    #[test]
    fn multiple_entries_applied() {
        // this test attempts to bulk send entries to followers. we'll first get everyone synced,
        // then cause a parition to occur so that the first proposal doesn't work
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);
        cluster.partition(2);
        cluster.partition(3);

        // propose the first command (won't get delivered)
        cluster.propose_and_sync(DUMMY.to_vec());

        // heal the partition and propose and sync another command
        cluster.heal(2);
        cluster.heal(3);

        let mut second = DUMMY.to_vec();
        second.push(b'2');
        cluster.propose(second);
        // check that we are batching commands (2 entries in the message)
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(_, _, msg)| match msg {
                    Message::AppendEntries(req) => req.entries.len() == 2,
                    _ => false,
                })
                .count(),
            2
        );

        // collect actions, deliver twice to get it all out and check leader advances commit_index
        // to 3
        let leader_id = cluster.leader.expect("No leader for cluster");
        let _ = cluster.deliver_all();
        let leader_actions = cluster.deliver_all();

        assert_eq!(cluster.node(leader_id).commit_index, 3);
        // send out a heartbeat and get followers to have a commit_index of 3 and check their
        // commands
        let heartbeat = cluster.node_mut(leader_id).append_entries();
        let _ = cluster.collect_actions(leader_id, heartbeat);
        let follower_actions = cluster.deliver_all();
        let _ = cluster.deliver_all();
        assert_eq!(cluster.node(2).commit_index, 3);
        assert_eq!(cluster.node(3).commit_index, 3);
        for (id, actions) in follower_actions {
            let mut cmd1 = false;
            let mut cmd2 = false;
            println!("{actions:?}");
            assert_eq!(actions.len(), 2);
            for a in actions {
                if let Action::ApplyToStateMachine { command } = a {
                    if *command.last().unwrap() == b'D' {
                        cmd1 = true;
                    } else if *command.last().unwrap() == b'2' {
                        cmd2 = true;
                    }
                }
            }
            // each node should have both commands applied
            assert!(cmd1 && cmd2);
        }
    }

    #[test]
    fn follower_conflicting_entry() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        let actions = make_leader_with_actions(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );
        append_entries_loop(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            actions,
        );
        let actions = node1.append_entries();
        append_entries_loop(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            actions,
        );

        // make a proposal
        let Some(actions) = node1.propose(DUMMY.to_vec()) else {
            panic!("Node used to call propose not a leader");
        };

        // leader believes everyone's next_index is 2, but we'll slip a different termed entry at
        // index 2 of node2 ; node2 should accept the AppendEntriesRPC (truncating its log to only
        // include index 0 and 1 the first entry and then the dummy from election) and
        // append this proposed command
        node2.log.push(LogEntry {
            term: 0,
            command: "OVERWRITE ME".as_bytes().to_vec(),
        });

        let append_entries = extract_append_entries(actions);
        let responses = deliver_append_entries(
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            append_entries,
        );
        // check successful response
        assert_eq!(responses.iter().filter(|(_, r)| r.success).count(), 2);
        // ensure that node2.log[1] has term 1 now (was overwritten)
        assert_eq!(node2.log[1].term, 1);
    }

    fn append_entries_loop(
        leader: &mut RaftCore,
        nodes: HashMap<NodeId, &mut RaftCore>,
        actions: Vec<Action>,
    ) {
        let append_entries = extract_append_entries(actions);
        let responses = deliver_append_entries(nodes, append_entries);
        deliver_append_entries_responses(leader, responses);
    }

    #[test]
    fn catch_up_follower() {
        // here we will propose a command and only send it to node2, then we'll propose another
        // command and send to both. in the AppendEntries there, we should have 2 entries for node3
        // (one for node2) and node3 should append those log entries
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        // partition node3 off
        cluster.partition(3);

        // propose and sync a command
        cluster.propose_and_sync(DUMMY.to_vec());

        // heal the partition and propose and sync another command
        cluster.heal(3);

        let mut second = DUMMY.to_vec();
        second.push(b'2');
        cluster.propose(second.clone());
        // check that we are batching commands for node3
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(_, to, msg)| match msg {
                    Message::AppendEntries(req) => *to == 3 && req.entries.len() == 2,
                    _ => false,
                })
                .count(),
            1
        );

        // collect actions, deliver twice to get it all out
        let _ = cluster.deliver_all();
        let _ = cluster.deliver_all();

        // check node3 has the two entries in its log
        assert_eq!(cluster.node(3).log.len(), 4);
        assert_eq!(cluster.node(3).log[2].command, DUMMY.to_vec());
        assert_eq!(cluster.node(3).log[3].command, second);
        // and that next index for node3 is 4
        assert_eq!(
            *cluster
                .node(cluster.leader.expect("No leader for cluster"))
                .next_index
                .get(&3)
                .unwrap(),
            4
        );
    }

    #[test]
    fn follower_well_behind() {
        // this test attempts to get a follower back up to speed with the rest of the cluster
        // we'll have a follower that never gets any log entries need to catchup
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);
        // every node's log looks like this after leader election:
        //    0     1     2
        //  ----- ----- -----
        // |     |     |     |
        // | t:0 | t:1 |     |
        // |     |     |     |
        //  ----- ----- -----

        // have node1 and 3 progress with node2 falling behind and having different entries
        let mut entry = LogEntry {
            term: 1,
            command: DUMMY.to_vec(),
        };
        cluster
            .node_mut(1)
            .log
            .extend([entry.clone(), entry.clone(), entry.clone()]);
        cluster
            .node_mut(3)
            .log
            .extend([entry.clone(), entry.clone(), entry]);
        // node2 will NOT get these entries, but have its next_index set to the same as node3's
        // node1 and node3 log now looks like this:
        //    0     1     2     3     4
        //  ----- ----- ----- ----- -----
        // |     |     |     |     |     |
        // | t:0 | t:1 | t:1 | t:1 | t:1 |
        // |     |     |     |     |     |
        //  ----- ----- ----- ----- -----

        cluster
            .node_mut(1)
            .next_index
            .entry(2)
            .and_modify(|v| *v = 5);
        cluster
            .node_mut(1)
            .next_index
            .entry(3)
            .and_modify(|v| *v = 5);

        // get a heartbeat out and deliver to followers
        cluster.tick_until_heartbeat(1);
        let actions = cluster.tick_node(1);
        cluster.collect_actions(1, actions);
        cluster.deliver_all();

        // node 2 should have denied the requst and set the first conflicting entry to 2 (because
        // the prev_log_index is less than the log length of node2)
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(from, _, msg)| {
                    match msg {
                        Message::AppendEntriesResponse(resp) => {
                            *from == 2 && !resp.success && resp.first_conflicting_index == Some(2)
                        }
                        _ => false,
                    }
                })
                .count(),
            1
        );

        // deliver to the leader, leader should have updated next_index to 2 for node2 AND sent an
        // AppendEntries with prev_log_index == 1 and entries.len() == 3
        cluster.deliver_all();
        assert_eq!(*cluster.node(1).next_index.get(&2).unwrap(), 2);
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(from, to, msg)| {
                    match msg {
                        Message::AppendEntries(req) => {
                            *from == 1
                                && *to == 2
                                && req.prev_log_index == 1
                                && req.entries.len() == 3
                        }
                        _ => false,
                    }
                })
                .count(),
            1
        );

        // final deliver to node2
        // check node2 responds with success and that the logs match
        cluster.deliver_all();
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(from, _to, msg)| {
                    match msg {
                        Message::AppendEntriesResponse(resp) => *from == 2 && resp.success,
                        _ => false,
                    }
                })
                .count(),
            1
        );
        assert_eq!(
            cluster
                .node(1)
                .log
                .iter()
                .map(|entry| entry.term)
                .collect::<Vec<_>>(),
            cluster
                .node(2)
                .log
                .iter()
                .map(|entry| entry.term)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn follower_ahead() {
        // this test shows a follower that may have been a previous leader and never was able to
        // commit its entries, it will have 3 extra entries on term 0 and need to get its log
        // truncated
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);
        // leader gets dummy entry at index 2 on term 1 (so does node3)
        let mut entry = LogEntry {
            term: 1,
            command: DUMMY.to_vec(),
        };
        cluster.node_mut(1).log.extend([entry.clone()]);
        cluster.node_mut(3).log.extend([entry.clone()]);

        // node2 will get entries at 1, 2, 3 on term 0
        entry.term = 0;
        cluster
            .node_mut(2)
            .log
            .extend([entry.clone(), entry.clone(), entry]);

        // update next_index on node1 to have 2 and 3 as "3" (what the leader believes their next
        // index should be based on its own log)
        cluster
            .node_mut(1)
            .next_index
            .entry(2)
            .and_modify(|v| *v = 3);
        cluster
            .node_mut(1)
            .next_index
            .entry(3)
            .and_modify(|v| *v = 3);

        cluster.tick_until_heartbeat(1);
        // should have a heartbeat with AppendEntries
        let actions = cluster.node_mut(1).tick();
        // send the messages
        cluster.collect_actions(1, actions);
        // deliver to followers
        cluster.deliver_all();
        // check that node2 sent back a false response with first conflicting index at index 2
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(from, _to, msg)| {
                    match msg {
                        Message::AppendEntriesResponse(resp) => {
                            *from == 2 && !resp.success && resp.first_conflicting_index == Some(2)
                        }
                        _ => false,
                    }
                })
                .count(),
            1
        );

        // deliver the responses to the leadership
        cluster.deliver_all();
        // leader should now have next_index for node2 set to 2 and sent out an append entries with
        // prev_log_index == 1 and 1 entry to add to the log (index 2 entry)
        assert_eq!(*cluster.node(1).next_index.get(&2).unwrap(), 2);
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(from, to, msg)| {
                    match msg {
                        Message::AppendEntries(req) => {
                            *from == 1
                                && *to == 2
                                && req.prev_log_index == 1
                                && req.entries.len() == 1
                        }
                        _ => false,
                    }
                })
                .count(),
            1
        );

        // deliver to node2 and check that node2 responded with success and that the logs match now
        // (based on terms)
        cluster.deliver_all();
        assert_eq!(
            cluster
                .pending_messages
                .iter()
                .filter(|(_, to, msg)| {
                    match msg {
                        Message::AppendEntriesResponse(resp) => *to == 1 && resp.success,
                        _ => false,
                    }
                })
                .count(),
            1
        );
        assert_eq!(
            cluster
                .node(1)
                .log
                .iter()
                .map(|entry| entry.term)
                .collect::<Vec<_>>(),
            cluster
                .node(2)
                .log
                .iter()
                .map(|entry| entry.term)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn mix_and_matched_logs() {
        // this test will have a three node cluster where node1 (leader) will have entries in term
        // 1 at index 1 and 2, node2 will have the same thing, and node3 will have a matching term
        //   in index 1, but a conflicting term in index 2. this will rectify the logs to match
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        make_leader(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );

        // make the dummies for node1 and 2 at index 1 and 2
        let mut entry = LogEntry {
            term: 1,
            command: DUMMY.to_vec(),
        };
        node1.log.extend([entry.clone(), entry.clone()]);
        node2.log.extend([entry.clone(), entry.clone()]);
        // give node3 the entry at term 1 at index 1
        node3.log.push(entry.clone());
        // and one at index 2 with term 2
        entry.term = 2;
        node3.log.push(entry);

        // update next_index on node1 to have 2 and 3 as "3" (as long as node1's log)
        node1.next_index.entry(2).and_modify(|v| *v = 3);
        node1.next_index.entry(3).and_modify(|v| *v = 3);

        // send one appendentries and give back to leader, this will update node3's next_index to 2
        // send one more and should have node3 truncate its log and be up to speed
        node1.ticks_since_last_heartbeat = node1.heartbeat_interval;
        let actions = node1.heartbeat();
        let append_entries = extract_append_entries(actions);
        // deliver to node2 and 3
        let responses = deliver_append_entries(
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            append_entries,
        );
        // check that node3 denied the request
        assert_eq!(
            responses
                .iter()
                .filter(|(_, msg)| msg.id == 3 && !msg.success)
                .count(),
            1
        );

        // when we deliver the appendentries responses to the leader, we should get the actions
        // because we'll decrement node3's prev_log_index
        let actions = deliver_append_entries_responses_with_actions(&mut node1, responses);
        assert_eq!(*node1.next_index.get(&3).unwrap(), 2);
        let append_entries = extract_append_entries(actions);
        let responses = deliver_append_entries(
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            append_entries,
        );
        // now node3 should respond successfully
        assert_eq!(
            responses
                .iter()
                .filter(|(_, msg)| msg.id == 3 && msg.success)
                .count(),
            1
        );
        let actions = deliver_append_entries_responses_with_actions(&mut node1, responses);
        // actions should be empty (node3 up to date)
        assert!(actions.is_empty());
        // and node1 and node3's logs should match
        assert_eq!(
            node1.log.iter().map(|entry| entry.term).collect::<Vec<_>>(),
            node3.log.iter().map(|entry| entry.term).collect::<Vec<_>>()
        );
    }

    #[test]
    fn reject_vote_candidate_log_behind() {
        // we will manually update the log entries in the nodes and kick off an election for a
        // specific node
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();

        // add manual entry in term 2 for node2
        node2.log.push(LogEntry {
            term: 2,
            command: DUMMY.to_vec(),
        });

        // make node1 a candidate that will send out votes
        tick_until_candidate(&mut node1);
        let actions = node1.tick();
        let vote_requests = extract_request_votes(actions);
        // send the vote requests to the nodes
        let responses = deliver_request_votes(
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            vote_requests,
        );

        // check that node2 DENIED the request because it has a more up-to-date log (mental note
        // though: node2 is a 'minority' here with its log entry, it will still lose and have its
        // log overwritten but it still should deny the request because its log is more up-to-date
        // than node1's at the moment)
        assert_eq!(
            responses
                .iter()
                .filter(|(_, msg)| msg.id == 2 && !msg.vote_granted)
                .count(),
            1
        );
    }

    #[test]
    fn reject_vote_same_last_term_shorter_log() {
        // in this vote, node1 and node3 will have a single real entry at index 1 with term 1 but
        // node2 will have one extra entry at index 2. node2 should still deny the vote because it
        // has a longer log than the candidate (which will be node1)
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();

        let entry = LogEntry {
            term: 1,
            command: DUMMY.to_vec(),
        };
        node1.log.push(entry.clone());
        node3.log.push(entry.clone());
        node2.log.push(entry.clone());
        // second entry for node2!
        node2.log.push(entry);

        // node1 becomes candidate
        tick_until_candidate(&mut node1);
        let actions = node1.tick();
        let vote_requests = extract_request_votes(actions);
        let responses = deliver_request_votes(
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            vote_requests,
        );

        // check that node2 denies the request and that node3 grants it
        assert_eq!(
            responses
                .iter()
                .filter(|(_, msg)| msg.id == 2 && !msg.vote_granted)
                .count(),
            1
        );

        assert_eq!(
            responses
                .iter()
                .filter(|(_, msg)| msg.id == 3 && msg.vote_granted)
                .count(),
            1
        );
    }

    #[test]
    fn grant_vote_candidate_higher_last_term() {
        // node2 (voter) will have 3 entries on term 1 but node1 (candidate) will have just one
        // entry but on a higher term (2), node2 should grant that vote
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();

        let mut entry = LogEntry {
            term: 1,
            command: DUMMY.to_vec(),
        };
        node2
            .log
            .extend([entry.clone(), entry.clone(), entry.clone()]);
        // update term to have a higher term in node1's log
        entry.term = 2;
        node1.log.push(entry);

        tick_until_candidate(&mut node1);
        let actions = node1.tick();
        let vote_requests = extract_request_votes(actions);
        let responses = deliver_request_votes(
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
            vote_requests,
        );

        // check that node2 granted the vote
        assert_eq!(
            responses
                .iter()
                .filter(|(_, msg)| msg.id == 2 && msg.vote_granted)
                .count(),
            1
        );
    }

    #[test]
    fn election_emits_persist_metadata() {
        // test ensures that when an election is kicked off, the first action is a PersistMetadata
        // and that there are no SendMessage actions before the PersistMetadata to keep our
        // contract
        let mut cluster = TestCluster::new(&[1, 2, 3]);
        cluster.tick_until_candidate(1);
        let actions = cluster.tick_node(1);

        match actions[0] {
            Action::PersistMetadata { term, voted_for } => {
                assert_eq!(term, 1);
                assert_eq!(voted_for, Some(1));
            }
            _ => panic!("First Action is NOT PersistMetadata"),
        }
    }

    #[test]
    fn grant_vote_emits_persist_metadata() {
        // test ensures that when a node grants a vote, that it will first emit a PersistMetadata
        // Action to persist the vote on new term
        let mut cluster = TestCluster::new(&[1, 2, 3]);
        cluster.tick_until_candidate(1);
        let actions = cluster.tick_node(1);
        // can't leverage the TestCluster too much because it disables the ability to ensure
        // PersistMetadata happens before SendMessage
        let node2_act = actions
            .iter()
            .filter(|action| match action {
                Action::SendMessage { target, message: _ } => *target == 2,
                _ => false,
            })
            .take(1)
            .next()
            .unwrap();
        let node2_msg = match node2_act {
            Action::SendMessage {
                target: _,
                message: Message::RequestVote(msg),
            } => msg,
            _ => panic!("No SendMessage for Node 2!"),
        };
        let node2_actions = cluster.node_mut(2).handle_request_vote(node2_msg.clone());

        // because of the way it is setup, the first Action will be PersistMetadata with a
        // voted_for as None (because node2 is on term 0 and check_msg_term will increment the term
        // and set voted_for to None). the second Action is going to be the actual vote after
        // handle_request_vote makes its way through. we check that here
        match node2_actions[0] {
            Action::PersistMetadata { term, voted_for } => {
                assert_eq!(term, 1);
                assert_eq!(voted_for, None);
            }
            _ => panic!("First Action is NOT PersistMetadata"),
        };
        match node2_actions[1] {
            Action::PersistMetadata { term, voted_for } => {
                assert_eq!(term, 1);
                assert_eq!(voted_for, Some(1));
            }
            _ => panic!("Second Action is NOT PersistMetadata"),
        };
    }

    #[test]
    fn propose_emits_persist_log() {
        // this test ensures that when a leader proposes a new command to be added to the log, the
        // first Action is a PersistLogEntries Action to add that new entry
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);
        // propose the command. note that the new command to be added is at index 2
        let actions = cluster.node_mut(1).propose(DUMMY.to_vec()).unwrap();
        match &actions[0] {
            Action::PersistLogEntries {
                start_index,
                entries,
            } => {
                assert_eq!(*start_index, 2);
                assert_eq!(
                    entries,
                    &[LogEntry {
                        term: 1,
                        command: DUMMY.to_vec()
                    }]
                    .to_vec()
                );
            }
            _ => panic!("First Action is NOT PersistLogEntries"),
        }
    }

    #[test]
    fn follower_append_emits_persist_log() {
        // this test ensures that when a follower appends a new entry to their log (upon receiving
        // AppendEntries RPC), that the follower persists that entry
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);
        // propose the command and grab the AppendEntries messages
        let actions = cluster.node_mut(1).propose(DUMMY.to_vec()).unwrap();
        // get node2's SendMessage Action to deliver to node2
        let node2_msg = actions
            .iter()
            .filter_map(|a| match a {
                Action::SendMessage {
                    target: 2,
                    message: Message::AppendEntries(req),
                } => Some(req),
                _ => None,
            })
            .take(1)
            .next()
            .unwrap();
        let node3_msg = actions
            .iter()
            .filter_map(|a| match a {
                Action::SendMessage {
                    target: 3,
                    message: Message::AppendEntries(req),
                } => Some(req),
                _ => None,
            })
            .take(1)
            .next()
            .unwrap();

        let node2_actions = cluster.node_mut(2).handle_append_entries(node2_msg.clone());
        let node3_actions = cluster.node_mut(3).handle_append_entries(node3_msg.clone());

        match &node2_actions[0] {
            Action::PersistLogEntries {
                start_index,
                entries,
            } => {
                assert_eq!(*start_index, 2);
                assert_eq!(
                    entries,
                    &[LogEntry {
                        term: 1,
                        command: DUMMY.to_vec()
                    }]
                    .to_vec()
                );
            }
            _ => panic!("First Action for node 2 is NOT PersistLogEntries"),
        }

        match &node3_actions[0] {
            Action::PersistLogEntries {
                start_index,
                entries,
            } => {
                assert_eq!(*start_index, 2);
                assert_eq!(
                    entries,
                    &[LogEntry {
                        term: 1,
                        command: DUMMY.to_vec()
                    }]
                    .to_vec()
                );
            }
            _ => panic!("First Action for node 3 is NOT PersistLogEntries"),
        }
    }

    #[test]
    fn heartbeat_no_persist() {
        // the leader and the followers should not put out a PersistLogEntries Action with an
        // empty heartbeat message because there is nothing to persist or add to the log
        let mut cluster = TestCluster::new_with_leader(&[1, 2, 3], 1);

        cluster.tick_until_heartbeat(1);
        let actions = cluster.tick_node(1);
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(
                    a,
                    Action::PersistLogEntries {
                        start_index: _,
                        entries: _
                    }
                ))
                .count(),
            0
        );

        cluster.collect_actions(1, actions);
        let node_actions = cluster.deliver_all();
        for (_id, actions) in node_actions {
            assert_eq!(
                actions
                    .iter()
                    .filter(|a| matches!(
                        a,
                        Action::PersistLogEntries {
                            start_index: _,
                            entries: _
                        }
                    ))
                    .count(),
                0
            );
        }
    }

    #[test]
    fn leader_election_emits_persist_log() {
        let mut cluster = TestCluster::new(&[1, 2, 3]);
        // get node1 to start an election
        cluster.tick_until_candidate(1);
        let actions = cluster.tick_node(1);
        // get the messages queued in pending
        cluster.collect_actions(1, actions);
        // deliver to followers
        cluster.deliver_all();
        // get the RequestVoteResponse back to the leader to have the leader win the election
        let actions = cluster.deliver_all();

        // ensure that there is a PersistLogEntries Action for node 1
        let leader_actions = actions.get(&1).unwrap();
        match &leader_actions[0] {
            Action::PersistLogEntries { start_index, entries } => {
                assert_eq!(*start_index, 1);
                assert_eq!(entries, &[LogEntry { term: 1, command: vec![] }].to_vec());
            },
            _ => panic!("No PersistLogEntries for leader after winning election and appending the dummy entry")
        }
    }
}
