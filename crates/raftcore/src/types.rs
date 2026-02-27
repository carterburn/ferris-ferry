#[derive(Debug, Eq, PartialEq)]
pub enum RaftState {
    Leader,
    Follower,
    Candidate,
}

pub type NodeId = u64;

#[derive(Debug)]
pub enum Action {
    SendMessage { target: NodeId, message: Message },
}

#[derive(Clone, Copy, Debug)]
pub enum Message {
    RequestVote(RequestVoteRPC),
    RequestVoteResponse(RequestVoteResponseRPC),
}

#[derive(Clone, Copy, Debug)]
pub struct RequestVoteRPC {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RequestVoteResponseRPC {
    pub id: u64,
    pub term: u64,
    pub vote_granted: bool,
}
