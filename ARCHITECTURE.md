# Raft Implementation in Rust — Architecture

## Design Philosophy

This is a generic, reusable Raft consensus library in Rust, **not** a monolithic Raft + KV store demo. The implementation follows the Raft paper as the primary reference. The core principle is **separation of concerns**: the Raft algorithm is isolated from I/O, networking, and application logic, making it deterministic and highly testable.

---

## Crate Structure

```
raft/              — core Raft library, generic over StateMachine and RaftTransport
raft-kv/           — example KV store implementing StateMachine + client-facing API
raft-transport/    — (optional) transport implementations (TCP, QUIC, in-memory)
```

---

## Core Architecture: Two-Layer Split

The system is split into two layers:

### 1. `RaftCore<S: StateMachine>` — Pure Logic (sync, no I/O)

Owns all Raft algorithm state. Accepts input events via method calls, returns a `Vec<Action>` describing what I/O should be performed. **Never performs I/O directly.** Completely deterministic — given the same sequence of inputs, always produces the same outputs.

```rust
struct RaftCore<S: StateMachine> {
    id: NodeId,
    state: RaftState,              // Follower | Candidate | Leader
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    state_machine: S,

    // Leader-only volatile state
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    // Timer tracking (tick-based, not wall-clock)
    ticks_since_last_heartbeat: u64,
    election_timeout: u64,         // concrete value randomly chosen from configured range
    heartbeat_interval: u64,

    // Election state
    votes_received: HashSet<NodeId>,
}
```

**Input methods** — each returns `Vec<Action>`:

- `tick()` — called on every tick interval; internally tracks elapsed ticks for election timeout (followers/candidates) and heartbeat interval (leaders)
- `handle_request_vote(req) -> Vec<Action>`
- `handle_request_vote_response(from, resp) -> Vec<Action>`
- `handle_append_entries(req) -> Vec<Action>`
- `handle_append_entries_response(from, resp) -> Vec<Action>`
- `handle_install_snapshot(req) -> Vec<Action>`
- `propose(command) -> Vec<Action>` — client command submission

**Output actions:**

```rust
enum Action {
    SendMessage { target: NodeId, message: Message },
    PersistState(PersistentState),        // term, voted_for, log entries
    ApplyToStateMachine(Vec<LogEntry>),
    ResetElectionTimer,
}
```

### 2. Async Event Loop Driver

Owns `RaftCore`, the transport, and channels. Runs as a single Tokio task. Multiplexes inbound messages, ticks, and client proposals via `tokio::select!`.

```rust
async fn run_event_loop<S, T>(
    mut core: RaftCore<S>,
    transport: T,
    mut propose_rx: mpsc::Receiver<ProposeRequest>,
) {
    let mut tick_interval = tokio::time::interval(Duration::from_millis(10));
    let mut inbound = transport.listen();

    loop {
        tokio::select! {
            _ = tick_interval.tick() => {
                let actions = core.tick();
                execute_actions(&transport, actions).await;
            }
            Some(msg) = inbound.recv() => {
                let actions = match msg {
                    Message::RequestVote(req) => core.handle_request_vote(req),
                    Message::AppendEntries(req) => core.handle_append_entries(req),
                    Message::RequestVoteResponse(from, resp) => {
                        core.handle_request_vote_response(from, resp)
                    }
                    Message::AppendEntriesResponse(from, resp) => {
                        core.handle_append_entries_response(from, resp)
                    }
                    // ...
                };
                execute_actions(&transport, actions).await;
            }
            Some(proposal) = propose_rx.recv() => {
                let actions = core.propose(proposal.command);
                execute_actions(&transport, actions).await;
            }
        }
    }
}
```

**Key property:** If both a tick and an inbound message are ready simultaneously, `select!` picks one randomly. This is safe because the event loop processes them sequentially, and Raft is correct regardless of message ordering.

---

## No Mutexes

`RaftCore` is owned exclusively by the event loop task. There is no shared access. External communication (client proposals, responses) flows through channels. This is the **actor model pattern** — single owner, message passing for all interaction. No locks needed anywhere.

---

## Timer Design: Single Tick Metronome

There is **one** fixed-interval timer in the event loop (e.g., every 10ms). The event loop calls `core.tick()` on each interval. All timeout logic lives inside `RaftCore`:

- **Followers/Candidates:** `ticks_since_last_heartbeat` increments on each tick. When it reaches `election_timeout`, start an election. Receiving a valid `AppendEntries` resets the counter to 0 and re-randomizes the election timeout.
- **Leaders:** Same counter, but checked against the shorter `heartbeat_interval`. When reached, emit `SendMessage` actions with empty `AppendEntries` (heartbeats) to all peers, reset counter.

Suggested values (tunable):
- Tick interval: 10ms
- Heartbeat interval: ~5 ticks (50ms)
- Election timeout range: 15–30 ticks (150–300ms)

This avoids the complexity of managing multiple `tokio::time::interval`s that need resetting and swapping based on state transitions. The event loop stays brainless; all interesting logic is in the testable core.

---

## `RaftNode` — Application-Facing Handle

The application only interacts with `RaftNode`, a thin handle holding a channel sender to the event loop. It knows nothing about `RaftCore`, the event loop, or the transport.

```rust
pub struct RaftNode {
    propose_tx: mpsc::Sender<ProposeRequest>,
}

impl RaftNode {
    pub async fn new<S, T>(config: RaftConfig, state_machine: S, transport: T) -> Result<Self>
    where
        S: StateMachine,
        T: RaftTransport,
    {
        let (propose_tx, propose_rx) = mpsc::channel(64);
        let core = RaftCore::new(config, state_machine);

        tokio::spawn(async move {
            run_event_loop(core, transport, propose_rx).await
        });

        Ok(RaftNode { propose_tx })
    }

    pub async fn propose(&self, cmd: Vec<u8>) -> Result<Response> {
        // Send through channel, await response via oneshot
    }
}
```

---

## StateMachine Trait — Application Decoupling

The Raft library is generic over the application state machine. Raft's job is replicating a log of commands; what those commands mean is up to the application.

```rust
pub trait StateMachine {
    type Command;
    type Response;

    fn apply(&mut self, command: Self::Command) -> Self::Response;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&mut self, snapshot: &[u8]);
}
```

**Ownership:** The `RaftCore` owns the state machine and calls `apply()` when entries are committed. The application layer **never** applies commands directly — it proposes them through `RaftNode::propose()`, and only after Raft commits them does the state machine get called.

**Flow:** Client request → `RaftNode::propose()` → channel → event loop → `core.propose()` → replication → commitment → `state_machine.apply()` → response back through channel to client.

**Serialization:** `Command` needs to be serializable for the log. Require `serde::Serialize + DeserializeOwned` bounds, or use `Command = Vec<u8>` for maximum flexibility.

**Read path:** Linearizable reads either go through the log (propose a no-op) or use a read index / lease-based approach. The trait may benefit from a separate `fn read(&self, query: Query) -> Response` path that Raft can call after confirming leadership without writing to the log.

---

## RaftTransport Trait

```rust
#[async_trait]
pub trait RaftTransport: Clone + Send + 'static {
    async fn send(&self, target: NodeId, msg: Message) -> Result<()>;
    fn listen(&self) -> mpsc::Receiver<Message>;
}
```

Must be `Clone + Send + 'static` because outbound sends are spawned as fire-and-forget Tokio tasks (see below).

### Outbound Sends: Fire-and-Forget Spawned Tasks

Outbound messages are spawned as separate Tokio tasks so the event loop is never blocked by a slow or partitioned peer:

```rust
async fn execute_actions<T: RaftTransport>(transport: &T, actions: Vec<Action>) {
    for action in actions {
        match action {
            Action::SendMessage { target, message } => {
                let transport = transport.clone();
                tokio::spawn(async move {
                    if let Err(e) = transport.send(target, message).await {
                        tracing::warn!("failed to send to {target}: {e}");
                    }
                });
            }
            Action::PersistState(state) => {
                // MUST be awaited — safety requirement from the paper
                persist(state).await;
            }
            Action::ApplyToStateMachine(entries) => {
                // MUST be awaited — must complete before responding
            }
        }
    }
}
```

**Important:** `PersistState` and `ApplyToStateMachine` actions must be handled synchronously (awaited) in the event loop. Only network sends are fire-and-forget. The paper's safety requirements demand persistence before responding to RPCs.

### Candidate Election: Non-Blocking by Design

When a candidate starts an election, `core.tick()` returns `SendMessage` actions for `RequestVote` RPCs to all peers. These are spawned as separate tasks. The event loop immediately returns to `select!` and can process inbound messages — including `AppendEntries` from a newly elected leader with a higher term, which correctly causes the candidate to step down to follower. The core is never "waiting" for vote responses; it handles them individually as they arrive.

---

## TcpTransport Implementation

Uses **per-peer reader/writer tasks** communicating through channels. The transport itself never touches sockets directly — it's just a bag of channel senders behind an `Arc`.

```rust
struct TcpTransport {
    peers: Arc<HashMap<NodeId, mpsc::Sender<Message>>>,
}
```

### Per-Peer Writer Task

Each peer has a dedicated task that owns the `OwnedWriteHalf` of the TCP connection. The transport's `send()` method just pushes onto the peer's channel — cheap and non-blocking.

```rust
// Spawned once per peer
async fn writer_task(mut rx: mpsc::Receiver<Message>, mut writer: OwnedWriteHalf) {
    while let Some(msg) = rx.recv().await {
        let bytes = serialize(&msg);
        let len = (bytes.len() as u32).to_be_bytes();
        writer.write_all(&len).await.ok();
        writer.write_all(&bytes).await.ok();
    }
}
```

### Per-Peer Reader Task

Each peer has a dedicated task that owns the `OwnedReadHalf` and pushes decoded messages into the event loop's inbound channel.

```rust
async fn reader_task(mut reader: OwnedReadHalf, inbound_tx: mpsc::Sender<Message>) {
    loop {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload).await?;

        let msg: Message = deserialize(&payload)?;
        inbound_tx.send(msg).await?;
    }
}
```

### Wire Protocol

Simple length-prefixed binary framing:

```
[msg_type: u8][length: u32][payload: bytes]
```

Serialization via `serde` + `bincode` or `postcard`.

### Architecture Diagram

```
Per peer:  [writer task owns OwnedWriteHalf] <-- mpsc channel -- TcpTransport.send()
Per peer:  [reader task owns OwnedReadHalf]  -- mpsc channel --> event loop inbound
```

Reconnection can be handled internally by writer/reader tasks without the rest of the system knowing.

---

## MemoryTransport — For Testing

An in-memory transport for running multi-node clusters in a single process. Enables deterministic testing of network partitions, message drops, and delays without real sockets.

---

## Testing Strategy

### Phase 1: RaftCore Unit Tests (sync, no async runtime)

Build and thoroughly test `RaftCore` first. All Raft protocol scenarios can be validated with pure synchronous tests by manually routing messages between core instances ("being the network"):

```rust
#[test]
fn test_full_replication_flow() {
    let mut leader = RaftCore::new(config(), KvStore::new());
    let mut follower1 = RaftCore::new(config(), KvStore::new());
    let mut follower2 = RaftCore::new(config(), KvStore::new());

    // ... elect leader ...

    let actions = leader.propose(KvCommand::Set("key", "value"));
    let append_entries = extract_append_entries(&actions);

    let actions1 = follower1.handle_append_entries(append_entries.clone());
    let response1 = extract_response(&actions1);

    let actions = leader.handle_append_entries_response(node1, response1);
    // With 2/3 nodes, entry is committed — should see ApplyToStateMachine action
}
```

**Election timeout testing:** Expose the concrete randomly-chosen timeout value for precise boundary testing:

```rust
let timeout = core.election_timeout();
for _ in 0..timeout - 1 { core.tick(); }
assert_eq!(core.state, RaftState::Follower);
core.tick();
assert_eq!(core.state, RaftState::Candidate);
```

**Test harness:** Build a `TestCluster` helper with methods like `deliver_all_messages()`, `partition(node)`, `heal_partition()`, `tick_until_leader_elected()` to reduce boilerplate for complex scenarios.

**Scenarios to cover:**
- Election timeout triggers candidacy
- Split votes and re-election
- Log replication and commitment (majority rule)
- Log conflict resolution
- Leader stepdown on higher term
- Candidate stepdown on receiving AppendEntries with >= term
- Replication to a node that's behind
- Partitioned leader rejoining with stale term

### Phase 2: Integration Tests with MemoryTransport

Run multi-node clusters in-process with simulated partitions and message drops.

### Phase 3: Real Network Tests with TcpTransport

### Phase 4: KV Store Application Layer

---

## Example StateMachine Implementations

### Minimal Counter (for early Raft testing)

```rust
struct Counter(u64);

impl StateMachine for Counter {
    type Command = ();
    type Response = u64;

    fn apply(&mut self, _cmd: ()) -> u64 {
        self.0 += 1;
        self.0
    }
}
```

### KV Store

```rust
enum KvCommand {
    Set(String, String),
    Get(String),
    Delete(String),
}

enum KvResponse {
    Ok,
    Value(Option<String>),
}

struct KvStore {
    data: HashMap<String, String>,
}

impl StateMachine for KvStore {
    type Command = KvCommand;
    type Response = KvResponse;

    fn apply(&mut self, cmd: KvCommand) -> KvResponse {
        match cmd {
            KvCommand::Set(k, v) => { self.data.insert(k, v); KvResponse::Ok }
            KvCommand::Get(k) => KvResponse::Value(self.data.get(&k).cloned()),
            KvCommand::Delete(k) => { self.data.remove(&k); KvResponse::Ok }
        }
    }
}
```

---

## Development Milestones

1. **RaftCore** + comprehensive unit tests covering the paper's key scenarios
2. **MemoryTransport** + integration tests with multi-node in-process cluster
3. **TcpTransport** + real network testing
4. **KV store** state machine on top

Build each layer only after the previous one is solid. If something breaks after adding async/networking, you know the bug is in the I/O layer, not the consensus algorithm.

---

## Key Dependencies

- `tokio` — async runtime, channels, timers
- `serde` + `bincode`/`postcard` — serialization
- `tracing` — structured logging
- `async-trait` — async trait methods
