use std::{collections::HashSet, ops::Range};

use crate::{
    statemachine::StateMachine,
    types::{Action, Message, NodeId, RaftState, RequestVoteRPC, RequestVoteResponseRPC},
};

use rand::prelude::*;

mod statemachine;
mod types;

struct RaftCore<S: StateMachine> {
    id: NodeId,
    peers: Vec<NodeId>,
    state: RaftState,
    current_term: u64,
    voted_for: Option<NodeId>,
    state_machine: S,

    votes_received: HashSet<NodeId>,

    ticks_since_last_heartbeat: u64,
    election_timeout_range: Range<u64>,
    election_timeout: u64,
}

impl<S: StateMachine> RaftCore<S> {
    /// Create a new RaftCore instance for a Node.
    ///
    /// Arguments:
    /// - id: This node's ID.
    /// - nodes: A collection of all the NodeId's in the cluster (including THIS node's ID).
    /// - state_machine: The concrete State Machine that will be used to apply committed entries.
    /// - election_range: Acceptable range for election timeout. Concrete value chosen with each
    /// term. Provide the range as ticks based on chosen tick interval (i.e. if ticks are every
    /// 10ms, and you want a 150-300ms election timeout as noted by the paper, this should be
    /// 15..31).
    pub fn new(id: NodeId, nodes: &[NodeId], state_machine: S, election_range: Range<u64>) -> Self {
        // seed the RNG
        let mut rng = rand::rng();

        let peers: Vec<NodeId> = nodes.iter().filter(|x| **x != id).copied().collect();
        Self {
            id,
            peers,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: None,
            state_machine,
            votes_received: HashSet::new(),
            ticks_since_last_heartbeat: 0,
            election_timeout_range: election_range.clone(),
            election_timeout: rng.random_range(election_range),
        }
    }

    pub fn tick(&mut self) -> Vec<Action> {
        // advance ticks_since_last_heartbeat
        self.ticks_since_last_heartbeat += 1;

        match self.state {
            RaftState::Leader => {
                vec![]
            }
            RaftState::Candidate => {
                // if we are a candidate and hit an election timeout, we would start another
                // election or hopefully have seen an AppendEntries that causes use to go down as a
                // follower; in this current state, we'll just return nothing..? so leaving this
                // logic commented for now
                /*
                if self.ticks_since_last_heartbeat >= self.election_timeout {
                    vec![]
                } else {
                    vec![]
                }
                */
                vec![]
            }
            RaftState::Follower => {
                // as a follower, we should check if we have hit the election timeout
                if self.ticks_since_last_heartbeat >= self.election_timeout {
                    // nothing heard, time to start an election!
                    // increment current term, vote for ourself, reset election timer, and
                    // transition to candidate state
                    self.current_term += 1;
                    self.voted_for = Some(self.id);
                    self.ticks_since_last_heartbeat = 0;
                    self.state = RaftState::Candidate;
                    // reset our vote counter
                    self.votes_received.clear();
                    self.votes_received.insert(self.id);

                    // now craft a message for each node
                    // TODO: need to actually compute last_log_[index|term] instead of hardcoding
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
                            message: msg,
                        })
                        .collect()
                } else {
                    vec![]
                }
            }
        }
    }

    pub fn handle_request_vote(&mut self, req: RequestVoteRPC) -> Vec<Action> {
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

        // check if we voted in this term
        match self.voted_for {
            None => {
                // we either haven't voted in this term or have voted for this peer already, return
                // true
                // set current term to message's term
                self.current_term = req.term;
                vec![Action::SendMessage {
                    target: req.candidate_id,
                    message: Message::RequestVoteResponse(RequestVoteResponseRPC {
                        id: self.id,
                        term: self.current_term,
                        vote_granted: true,
                    }),
                }]
            }
            Some(candidate_id) if candidate_id == req.candidate_id => {
                self.current_term = req.term;
                vec![Action::SendMessage {
                    target: req.candidate_id,
                    message: Message::RequestVoteResponse(RequestVoteResponseRPC {
                        id: self.id,
                        term: self.current_term,
                        vote_granted: true,
                    }),
                }]
            }
            Some(_) => {
                // we voted for someone else so return false
                vec![Action::SendMessage {
                    target: req.candidate_id,
                    message: false_response,
                }]
            }
        }
    }

    pub fn handle_request_vote_response(&mut self, resp: RequestVoteResponseRPC) -> Vec<Action> {
        // should only get this if we are a candidate
        if !matches!(self.state, RaftState::Candidate) {
            return vec![];
        }
        if resp.vote_granted {
            // received a vote for this election
            self.votes_received.insert(resp.id);

            if self.votes_received.len() as u64 > (self.votes_received.len() as u64 / 2) {
                let mut rng = rand::rng();
                // we won the election, transition to leader state
                self.state = RaftState::Leader;
                self.ticks_since_last_heartbeat = 0;
                // reset election timeout
                self.election_timeout = rng.random_range(self.election_timeout_range.clone());
                self.voted_for = None;
                // TODO: add AppendEntries RPC message to be sent to each peer upon becoming leader
                // but for now we don't have that so we just do empty
                vec![]
            } else {
                vec![]
            }
        } else {
            // didn't get the vote, so we just move on and do nothing
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoOpStateMachine;

    impl StateMachine for NoOpStateMachine {
        type Command = ();
        type Response = ();

        fn apply(&mut self, _command: Self::Command) -> Self::Response {}
    }

    #[test]
    fn create_new_raft_core() {
        let nodes = [1, 2, 3];
        let s = NoOpStateMachine;

        let node = RaftCore::new(1, &nodes, s, 15..31);

        assert_eq!(node.id, 1);
    }

    #[test]
    fn raft_initial_election() {
        let nodes = [1, 2, 3];

        let mut leader = RaftCore::new(1, &nodes, NoOpStateMachine, 15..31);
        let mut node2 = RaftCore::new(2, &nodes, NoOpStateMachine, 15..31);
        let mut node3 = RaftCore::new(3, &nodes, NoOpStateMachine, 15..31);
        // make sure we all start as followers
        assert!(leader.state == RaftState::Follower);
        assert!(node2.state == RaftState::Follower);
        assert!(node3.state == RaftState::Follower);

        let ticks_to_election = leader.election_timeout;

        // tick the leader to that spot
        for _ in 0..ticks_to_election - 1 {
            leader.tick();
        }

        assert!(leader.state == RaftState::Follower);

        // tick the leader to trigger an election
        let actions = leader.tick();
        // ensure leader is a candidate now and we have actions to take
        assert!(leader.state == RaftState::Candidate);
        assert!(!actions.is_empty());
        // for each of the actions, we send the message to the corresponding node and gather them
        // up for delivery to the leader
        let mut responses = vec![];
        for a in actions {
            match a {
                Action::SendMessage { target, message } => {
                    let msg = match message {
                        Message::RequestVote(m) => m,
                        _ => {
                            panic!("Unexpected message type");
                        }
                    };
                    let response_actions = if target == 2 {
                        node2.handle_request_vote(msg)
                    } else {
                        node3.handle_request_vote(msg)
                    };
                    assert!(!response_actions.is_empty());
                    match response_actions[0] {
                        Action::SendMessage { target, message } => {
                            // ensure its heading to the leader
                            assert!(target == leader.id);
                            match message {
                                Message::RequestVoteResponse(resp) => {
                                    responses.push(resp);
                                }
                                _ => {
                                    panic!("Unexpected message from follower nodes");
                                }
                            }
                        }
                    };
                }
            }
        }

        // should have two responses to check
        assert_eq!(responses.len(), 2);
        // deliver the responses to the leader, one at a time
        leader.handle_request_vote_response(responses[0]);
        // we should have a quorum now, so we should be the leader
        assert!(leader.state == RaftState::Leader);
        // deliver the next one
        leader.handle_request_vote_response(responses[1]);
        // check we are still the leader
        assert!(leader.state == RaftState::Leader);
    }

    #[test]
    fn test_simultaneous_election() {
        let nodes = [1, 2, 3];

        let mut node1 = RaftCore::new(1, &nodes, NoOpStateMachine, 15..31);
        let mut node2 = RaftCore::new(2, &nodes, NoOpStateMachine, 15..31);
        let mut node3 = RaftCore::new(3, &nodes, NoOpStateMachine, 15..31);
        // make sure we all start as followers
        assert!(node1.state == RaftState::Follower);
        assert!(node2.state == RaftState::Follower);
        assert!(node3.state == RaftState::Follower);

        let node1_ticks_to_election = node1.election_timeout;
        let node2_ticks_to_election = node2.election_timeout;

        // get node1 and node2 to start an election. node3 will be delievered node1's RequestVote
        // first then node2 which will make node1 win the election. also must deliver the messages
        // to node1/2
        for _ in 0..node1_ticks_to_election - 1 {
            node1.tick();
        }
        let node1_actions = node1.tick();
        for _ in 0..node2_ticks_to_election - 1 {
            node2.tick();
        }
        let node2_actions = node2.tick();

        let mut to_node1 = vec![];
        let mut to_node2 = vec![];

        for a in node1_actions {
            match a {
                Action::SendMessage { target, message } => {
                    match message {
                        Message::RequestVote(req) => {
                            if target == 2 {
                                to_node1.push(node2.handle_request_vote(req));
                            } else {
                                to_node1.push(node3.handle_request_vote(req));
                            }
                        }
                        _ => {
                            panic!("Unexpected message!")
                        }
                    };
                }
            }
        }
        for a in node2_actions {
            match a {
                Action::SendMessage { target, message } => match message {
                    Message::RequestVote(req) => {
                        if target == 1 {
                            to_node2.push(node1.handle_request_vote(req));
                        } else {
                            to_node2.push(node3.handle_request_vote(req));
                        }
                    }
                    _ => {
                        panic!("Unexpected message!")
                    }
                },
            }
        }

        // deliver the messages to node1
        for resps in to_node1 {
            for a in resps {
                match a {
                    Action::SendMessage { target, message } => {
                        assert_eq!(target, 1);
                        match message {
                            Message::RequestVoteResponse(resp) => {
                                node1.handle_request_vote_response(resp);
                            }
                            _ => {
                                panic!("Unexpected message");
                            }
                        }
                    }
                }
            }
        }
        // normally, we would deliver to node2, but because we don't have AppendEntries yet, node2
        // will actually think they win the election

        // node1 should be a leader
        assert!(node1.state == RaftState::Leader);
    }
}
