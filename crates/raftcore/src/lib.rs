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
        let prev_log_index = *peer_next_index - 1;
        let prev_log_term = self.log[prev_log_index as usize].term;

        // grab te entries at the next place we think the peer needs it
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

            // TODO: persist
            let request = RequestVoteRPC {
                term: self.current_term,
                candidate_id: self.id,
                last_log_index: 0,
                last_log_term: 0,
            };
            let msg = Message::RequestVote(request);
            self.peers
                .iter()
                .map(|peer_id| Action::SendMessage {
                    target: *peer_id,
                    message: msg.clone(),
                })
                .collect()
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
    fn check_msg_term(&mut self, msg_term: u64) {
        if msg_term > self.current_term {
            self.current_term = msg_term;
            self.state = RaftState::Follower;
            self.voted_for = None;
            self.reset_election_timeout();
            // TODO: persist
        }
    }

    pub fn handle_request_vote(&mut self, req: RequestVoteRPC) -> Vec<Action> {
        self.check_msg_term(req.term);

        let false_response = Message::RequestVoteResponse(RequestVoteResponseRPC {
            id: self.id,
            term: self.current_term,
            vote_granted: false,
        });

        // I'm a node that just received a RequestVoteRPC
        if req.term < self.current_term {
            // the requestor is on an earlier term, so we return false and don't vote for it!
            return vec![Action::SendMessage {
                target: req.candidate_id,
                message: false_response,
            }];
        }

        let grant_vote = match self.voted_for {
            None => {
                // no vote in this term
                true
            }
            Some(candidate_id) if candidate_id == req.candidate_id => {
                // already voted for this peer in this term, so send another vote (won't be counted
                // twice)
                true
            }
            Some(_) => false,
        };

        if grant_vote {
            self.current_term = req.term;
            self.reset_election_timeout();
            self.voted_for = Some(req.candidate_id);
            // TODO: persist here
            vec![Action::SendMessage {
                target: req.candidate_id,
                message: Message::RequestVoteResponse(RequestVoteResponseRPC {
                    id: self.id,
                    term: self.current_term,
                    vote_granted: true,
                }),
            }]
        } else {
            vec![Action::SendMessage {
                target: req.candidate_id,
                message: false_response,
            }]
        }
    }

    pub fn handle_request_vote_response(&mut self, resp: RequestVoteResponseRPC) -> Vec<Action> {
        self.check_msg_term(resp.term);

        // should only get this if we are a candidate
        if !matches!(self.state, RaftState::Candidate) {
            return vec![];
        }
        if resp.vote_granted {
            // received a vote for this election
            self.votes_received.insert(resp.id);

            if self.votes_received.len() as u64 > ((self.peers.len() as u64 + 1) / 2) {
                // we won the election, transition to leader state
                self.state = RaftState::Leader;
                self.initalize_leader_state();
                self.reset_election_timeout();
                // set the ticks_since_last_heartbeat to heartbeat_interval so we send out a
                // heartbeat on next tick which will be the first AppendEntries of the new term
                // that we have to send out as prescribed by the paper
                // TODO: think if we should just make the append entries here and return as the
                // Vec<Action> from this function?
                self.ticks_since_last_heartbeat = self.heartbeat_interval;
                vec![]
            } else {
                vec![]
            }
        } else {
            // didn't get the vote, so we just move on and do nothing
            vec![]
        }
    }

    pub fn handle_append_entries(&mut self, req: AppendEntriesRPC) -> Vec<Action> {
        self.check_msg_term(req.term);

        let false_response = Message::AppendEntriesResponse(types::AppendEntriesResponseRPC {
            id: self.id,
            term: self.current_term,
            success: false,
        });
        let false_action = vec![Action::SendMessage {
            target: req.leader_id,
            message: false_response,
        }];

        // check if we are a candidate and step down if the term is at least as big as ours
        // (Section 5.2)
        if req.term == self.current_term && self.state == RaftState::Candidate {
            // convert to follower and mark we didn't vote in the term (somehow, majority of
            // servers voted this server sending req to leader, so we follow suit and become a
            // follower)
            self.state = RaftState::Follower;
            self.voted_for = None;
        }

        // ensure we are a follower
        if self.state != RaftState::Follower {
            // send nothing if we are a leader or candidate
            return false_action;
        }

        if req.term < self.current_term {
            // "Leader" is on an earlier term, so we send back a false response
            return false_action;
        }

        // reset election timeout since we have a valid leader on the right term
        self.reset_election_timeout();

        let valid = req.prev_log_index == 0
            || (req.prev_log_index < self.log.len() as u64
                && self.log[req.prev_log_index as usize].term == req.prev_log_term);
        if !valid {
            return false_action;
        }

        // now we start appending entries (and overwrite anything that has a bad term)
        // have to be careful with our good old Rust Vec

        // start writing at previous log index + 1
        let next = req.prev_log_index + 1;
        for i in next as usize..next as usize + req.entries.len() {
            let new_entry = req.entries[i - next as usize].clone();
            if i > self.log.len() {
                // not sure what to do in this case.. i don't think this would be possible because
                // the valid check would fail and we wouldn't get here..
                // ask claude
            }
            if i < self.log.len() {
                // this is an existing entry, check if it has the same term
                if self.log[i].term != new_entry.term {
                    // set the log to be everything up to put not including i
                    self.log = self.log[..i].to_vec();
                } else {
                    // same index and same term, so it's the same entry. just move on
                    continue;
                }
            }

            // at this point we know that i == self.log.len() meaning we can just 'push' to
            // self.log with the new entry
            self.log.push(new_entry);
        }

        // update our commit index
        if req.leader_commit > self.commit_index {
            self.commit_index = min(req.leader_commit, self.log.len() as u64);
        }

        vec![Action::SendMessage {
            target: req.leader_id,
            message: Message::AppendEntriesResponse(AppendEntriesResponseRPC {
                id: self.id,
                term: self.current_term,
                success: true,
            }),
        }]
    }

    pub fn handle_append_entries_response(
        &mut self,
        resp: AppendEntriesResponseRPC,
    ) -> Vec<Action> {
        self.check_msg_term(resp.term);

        if resp.success {
            // i think this is wrong if we send multiple entries, we need a way to figure out how
            // much the client committed
            // need help from claude
            self.next_index.entry(resp.id).and_modify(|v| *v += 1);
            self.match_index.entry(resp.id).and_modify(|v| *v += 1);
            vec![]
        } else {
            // now we have to decrement next_index for this client and try the AppendEntries again
            self.next_index.entry(resp.id).and_modify(|v| *v -= 1);
            vec![self.build_append_entries_for_peer(resp.id)]
        }

        // now figure out what entries we can commit (if there is an index where majority of
        // servers have a match index > index and the log[N].term == currentTerm we can commit it
        // and send an Action::ApplyToStateMachine)
        // TODO: figure that out because I'm not sure the logic to do that.. :( )
    }

    fn initalize_leader_state(&mut self) {
        for id in &self.peers {
            self.next_index
                .entry(*id)
                .and_modify(|val| *val = self.log.len() as u64)
                .or_insert(self.log.len() as u64);
            self.match_index
                .entry(*id)
                .and_modify(|val| *val = 0)
                .or_insert(0);
        }
        // should we follow section 8 of Raft paper and commit a no-op entry (dummy entry) from
        // this new term and force it to be sent out via AppendEntries heartbeat ?
        // TODO: check if we should do this .. i think so
        // TODO: persist
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        let (mut leader, mut node2, mut node3) = init_three_node_noop_cluster();

        tick_until_candidate(&mut leader);
        assert!(leader.state == RaftState::Follower);

        // tick the leader to trigger an election
        let actions = leader.tick();
        // ensure leader is a candidate now and we have actions to take
        assert!(leader.state == RaftState::Candidate);
        assert!(!actions.is_empty());

        let vote_requests = extract_request_votes(actions);
        let node_map = HashMap::from([(2, &mut node2), (3, &mut node3)]);

        let responses = deliver_request_votes(node_map, vote_requests);
        deliver_request_vote_responses(&mut leader, responses);

        // should be a leader and have our term be == 1
        assert!(leader.state == RaftState::Leader);
        assert_eq!(leader.current_term, 1);

        // node2 and 3 should also have their term set to 1 and be followers
        assert_eq!(node2.current_term, 1);
        assert_eq!(node3.current_term, 1);
        assert!(node2.state == RaftState::Follower);
        assert!(node3.state == RaftState::Follower);
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

    /// Ticks a node until heartbeat interval
    fn tick_until_heartbeat(node: &mut RaftCore) {
        for _ in 0..node.heartbeat_interval - 1 {
            node.tick();
        }
    }

    #[test]
    fn leader_sends_heartbeat_at_interval() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        make_leader(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );

        // on next tick, leader should send out the AppendEntries to start the term
        let actions = node1.tick();
        let append_entries = extract_append_entries(actions);
        assert_eq!(append_entries.len(), 2);

        // now we should tick until heartbeat interval hits on node1 and check if we send out
        // another set of append entries
        tick_until_heartbeat(&mut node1);
        let hb_actions = node1.tick();
        let append_entries = extract_append_entries(hb_actions);
        assert_eq!(append_entries.len(), 2);
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

    #[test]
    fn follower_resets_election_timeout() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        make_leader(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );
        // tick node2 and 3 at least once to get some ticks since heartbeat
        node2.tick();
        node3.tick();
        assert!(node2.ticks_since_last_heartbeat > 0);
        assert!(node3.ticks_since_last_heartbeat > 0);
        // this single tick triggers the heartbeat because make_leader() will cause node1 to become
        // the leader and the leader's election will set the ticks_since_last_heartbeat to the
        // heartbeat interval so that on the NEXT tick, a heartbeat is triggered (which is why we
        // tick here)
        let actions = node1.tick();
        let append_entries = extract_append_entries(actions);
        let node_map = HashMap::from([(2, &mut node2), (3, &mut node3)]);
        let responses = deliver_append_entries(node_map, append_entries);

        // check node2 and node3 have a "ticks since last heartbeat" set to 0;
        assert_eq!(node2.ticks_since_last_heartbeat, 0);
        assert_eq!(node3.ticks_since_last_heartbeat, 0);

        // ensure both followers responded true with their response
        assert_eq!(
            responses
                .iter()
                .filter(|(_, response)| { response.success })
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
        // a candidate will be seeking votes and receive a AppendEntries RPC from another node with a
        // higher term. in that case, the candidate should revert to a follower _and_ vote for that
        // new node
        let (mut node1, _node2, _node3) = init_three_node_noop_cluster();
        // make node1 a candidate
        tick_until_candidate(&mut node1);
        let _ = node1.tick();
        assert!(node1.state == RaftState::Candidate);

        // node1 is seeking to be the leader, but gets an AppendEntries from node2 with higher term
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
        // higher term. in that case, the candidate should revert to a follower _and_ vote for that
        // new node
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
    fn correct_next_match_index() {
        let (mut node1, mut node2, mut node3) = init_three_node_noop_cluster();
        make_leader(
            &mut node1,
            HashMap::from([(2, &mut node2), (3, &mut node3)]),
        );

        for node in [2, 3] {
            assert_eq!(node1.next_index.get(&node).unwrap(), &1);
            assert_eq!(node1.match_index.get(&node).unwrap(), &0);
        }
    }
}
