use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq)]
pub enum RaftState {
    Leader,
    Follower,
    Candidate,
}

pub type NodeId = u64;

#[derive(Debug)]
pub enum Action {
    SendMessage {
        target: NodeId,
        message: Message,
    },
    ApplyToStateMachine {
        index: u64,
        command: Vec<u8>,
    },
    PersistMetadata {
        term: u64,
        voted_for: Option<NodeId>,
    },
    PersistLogEntries {
        start_index: u64,
        entries: Vec<LogEntry>,
    },
    SendInstallSnapshot {
        target: NodeId,
        term: u64,
        last_included_index: u64,
        last_included_term: u64,
    },
    InstallSnapshot {
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    RequestVote(RequestVoteRPC),
    RequestVoteResponse(RequestVoteResponseRPC),
    AppendEntries(AppendEntriesRPC),
    AppendEntriesResponse(AppendEntriesResponseRPC),
    InstallSnapshot(InstallSnapshotRPC),
    InstallSnapshotResponse(InstallSnapshotResponseRPC),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteRPC {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteResponseRPC {
    /// Id of responder
    pub id: u64,
    pub term: u64,
    pub vote_granted: bool,
}

/// An entry in the log
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    /// Serialized version of the command
    pub command: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEntriesRPC {
    /// Leader's term
    pub term: u64,

    /// Leader ID
    pub leader_id: NodeId,

    /// Index of log entry immediately preceding new ones as tracked by leader
    pub prev_log_index: u64,

    /// Term of prev_log_index entry
    pub prev_log_term: u64,

    /// log entries to store in this AppendEntries (will be empty for heartbeats)
    pub entries: Vec<LogEntry>,

    /// commit_index of the leader
    pub leader_commit: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEntriesResponseRPC {
    /// Id of responder
    pub id: u64,

    /// Term of the node
    pub term: u64,

    /// Match index this follower is on based on the new AppendEntries received
    pub match_index: u64,

    /// Whether the node has an entry at prev_log_index with prev_log_term
    pub success: bool,

    /// Optional index of first log entry that contains a conflicting term
    pub first_conflicting_index: Option<u64>,

    /// Optional term for that conflicting index
    pub first_conflicting_term: Option<u64>,
}

pub struct SnapshotMetadata {
    pub last_applied: u64,
    pub last_applied_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSnapshotRPC {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSnapshotResponseRPC {
    pub id: NodeId,
    pub term: u64,
    pub last_included_index: u64,
}
