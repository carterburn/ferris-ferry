# Ferris Ferry

A Raft consensus library in Rust, built from the ground up following the [Raft paper](https://raft.github.io/raft.pdf). Designed around **separation of concerns**: the consensus algorithm is isolated from I/O, networking, and application logic.

## Architecture

```
Application (raft-kv (example binary provided), or any application needing distributed consensus)
        |
   RaftNode::propose() / AppliedEntry receiver
        |
   raft-event-loop (tokio::select! over ticks, proposals, messages)
        |
   RaftCore (sans-I/O, deterministic state machine)
       / \
Transport   Storage
```

**Sans-I/O core**: `RaftCore` is a pure synchronous state machine. It accepts inputs (ticks, messages, proposals) and returns `Vec<Action>` describing what I/O to perform. It never touches the network or disk directly. This makes it deterministic and testable without mocking.

**Event loop driver**: `RaftNode` spawns a single Tokio task that owns `RaftCore`, a `Transport`, and a `Storage` implementation. It runs a `tokio::select!` loop over a tick interval, inbound messages, and client proposals, translating `Action`s into real I/O.

**Pluggable transport and storage**: The `Transport` and `Storage` traits define how messages are sent/received and how state is persisted. Swap implementations without touching the consensus logic.

## Crate Layout

| Crate | Type | Description |
|---|---|---|
| `raftcore` | lib | Sans-I/O Raft algorithm: leader election, log replication, snapshotting |
| `raft-event-loop` | lib | Async driver, `Transport`/`Storage` trait definitions, `RaftNode` handle |
| `raft-file-storage` | lib | File-based `Storage` with atomic writes and crash-safe log framing |
| `raft-tcp-transport` | lib | TCP-based `Transport` with per-peer sender tasks and a connection listener |
| `raft-test-utils` | lib | In-memory `Storage` and `Transport` for integration testing |
| `raft-kv` | bin | Example distributed key-value store built on the library |

## Key Design Decisions

- **Actions, not callbacks**: `RaftCore` returns actions (`PersistMetadata`, `SendMessage`, `ApplyToStateMachine`, etc.) rather than performing I/O internally. The event loop processes them in order, awaiting persistence before sending messages.
- **Single tick metronome**: One fixed-interval timer drives all timeout logic. Election and heartbeat timeouts are internal to `RaftCore` via tick counting.
- **Proposal correlation**: The event loop tracks pending proposals in a `HashMap<u64, oneshot::Sender>` keyed by log index, matching committed entries back to the client that proposed them.
- **Snapshot compaction**: The event loop initiates snapshots (not the application). It requests serialized state from the application via a oneshot channel, then calls `prepare_snapshot()` / `complete_snapshot()` on `RaftCore` synchronously -- guaranteeing index/data alignment since no entries can be applied in between.
- **No-op on election**: The leader appends an empty log entry on election win (Section 8) to ensure previous-term entries can be committed. The event loop filters these out and does not deliver them to the application.

## Implementing Your Own Transport

Implement the `Transport` trait from `raft-event-loop`:

```rust
use raft_event_loop::types::Transport;
use raftcore::types::{Message, NodeId};

pub struct MyTransport { /* ... */ }

impl Transport for MyTransport {
    type Address = String; // whatever your addressing scheme is

    async fn send(&self, node: NodeId, message: Message) {
        // Fire-and-forget: serialize `message` and send it to `node`.
        // If delivery fails, silently drop it -- Raft handles retries.
    }

    async fn recv(&mut self) -> Message {
        // Return the next inbound message.
        // IMPORTANT: This must be cancel-safe (backed by a channel, not a raw socket read)
        // because it runs inside tokio::select!
    }
}
```

Key points:
- One `Transport` instance manages all peer connections (not one per peer).
- `send()` takes `&self` -- use interior mutability or channels if needed.
- `recv()` must be cancel-safe. Back it with an `mpsc` channel.

## Implementing Your Own Storage

Implement the `Storage` trait from `raft-event-loop`:

```rust
use raft_event_loop::types::{Storage, PersistedMetadata, PersistedLogAddendum, Snapshot};
use raftcore::types::LogEntry;

pub struct MyStorage { /* ... */ }

impl Storage for MyStorage {
    async fn store_metadata(&self, metadata: PersistedMetadata) -> std::io::Result<()> {
        // Persist term and voted_for. Must be durable when this returns.
    }

    async fn restore_metadata(&self) -> std::io::Result<Option<PersistedMetadata>> {
        // Return None on first boot, Some on recovery.
    }

    async fn store_log_entries(&self, addendum: PersistedLogAddendum) -> std::io::Result<()> {
        // Append entries starting at addendum.start_index.
        // If start_index overlaps existing entries, overwrite from that point (conflict resolution).
        // Must be durable when this returns.
    }

    async fn restore_log_entries(&self) -> std::io::Result<Option<Vec<LogEntry>>> {
        // Return None if no log file exists. Return Some(vec![]) if the file exists but is empty.
    }

    async fn store_snapshot(&self, snapshot: Snapshot) -> std::io::Result<()> {
        // Atomically persist the snapshot (write-to-temp, rename).
    }

    async fn restore_snapshot(&self) -> std::io::Result<Option<Snapshot>> {
        // Return None if no snapshot exists.
    }

    async fn retrieve_snapshot_bytes(&self) -> std::io::Result<Vec<u8>> {
        // Return the raw snapshot data for sending to a follower via InstallSnapshot.
        // Error if no snapshot exists (this should never be called without one).
    }

    async fn truncate_log(&self) -> std::io::Result<()> {
        // Clear the log file (called after snapshot compaction or InstallSnapshot).
    }
}
```

Key points:
- All methods take `&self` -- use interior mutability if needed.
- `store_metadata` and `store_log_entries` must be durable (fsync) before returning. The event loop relies on this for crash safety.
- `restore_*` methods return `Option`: `None` means fresh start, `Err` means I/O failure (unrecoverable).
- For crash safety on metadata and snapshots, use the write-to-temp-then-rename pattern.
- For the log file, use length-prefixed frames so partial writes from crashes can be detected and discarded on recovery.

## Running raft-kv

Build the project:

```bash
cargo build -p raft-kv
```

Start a 3-node cluster (in separate terminals):

```bash
# Node 1
RUST_LOG=info cargo run -p raft-kv -- \
  --nodes 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  --id 1 --addr 0.0.0.0:3001 --directory /tmp/raft-kv/node1

# Node 2
RUST_LOG=info cargo run -p raft-kv -- \
  --nodes 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  --id 2 --addr 0.0.0.0:3002 --directory /tmp/raft-kv/node2

# Node 3
RUST_LOG=info cargo run -p raft-kv -- \
  --nodes 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  --id 3 --addr 0.0.0.0:3003 --directory /tmp/raft-kv/node3
```

Interact with the cluster (requests must go to the current leader; try each node):

```bash
# Set a key
curl -X POST http://localhost:3001/key \
  -H 'Content-Type: application/json' \
  -d '{"key": "hello", "value": "world"}'

# Get a key
curl http://localhost:3001/key/hello

# Delete a key
curl -X DELETE http://localhost:3001/key/hello
```

Try killing a node and watching the cluster elect a new leader. Set keys, restart the killed node, and verify it catches up via log replication or snapshot transfer.
