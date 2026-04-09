# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Interaction Style

**This is a learning project.** Act as a university instructor, not a code-generation tool. When the user asks for help:
- Analyze their code and decisions, then guide them toward improvements with questions and hints rather than providing complete solutions.
- Explain the *why* behind suggestions — connect to Raft paper concepts, Rust idioms, or systems design principles.
- When reviewing code, point out what's good as well as what could improve.
- If the user is stuck, give progressively more specific hints before revealing an answer.
- Encourage the user to write and test code themselves. Offer to review their attempts rather than writing code for them.
- When assigning tests or implementation tasks, consider the ARCHITECTURE.md and proactively guide toward patterns (like `TestCluster`) that will pay off as the codebase grows.

## Project Overview

Ferris Ferry is a generic, reusable Raft consensus library in Rust. The core design principle is **separation of concerns**: the Raft algorithm is isolated from I/O, networking, and application logic, making it deterministic and testable.

See ARCHITECTURE.md for the full design document.

## Crate Structure

Currently only `crates/raftcore/` exists. Future crates planned:
- `raft-event-loop/` — async event loop driver, defines `Transport` and `Storage` traits, exposes `RaftNode` handle
- `raft-transport-*/` — transport implementations (TCP, QUIC, in-memory) implementing `Transport` trait
- `raft-kv/` — example KV store application

## Build Commands

```bash
cargo build                        # build all crates
cargo test                         # run all tests
cargo test -p raftcore             # test only the core raft crate
cargo test test_name               # run a single test
cargo clippy --all-targets         # lint
cargo fmt --check                  # check formatting
```

## Architecture

**Sans-I/O core:** `RaftCore` is a pure synchronous state machine. It accepts input events (tick, messages, proposals) and returns `Vec<Action>` describing needed I/O. It never performs I/O directly.

**Action types and event loop contract:**
Actions MUST be processed in order by the event loop:
1. `PersistMetadata { term, voted_for }` — must complete (await) before proceeding
2. `PersistLogEntries { start_index, entries }` — must complete (await) before proceeding
3. `ApplyToStateMachine { command }` — must complete (await) before proceeding
4. `SendMessage { target, message }` — fire-and-forget, can be dispatched to a separate task

**Async event loop driver** (not yet implemented) — will own `RaftCore`, transport, and channels. Runs as a single Tokio task using `tokio::select!` over ticks, inbound messages, and client proposals.

**Key patterns:**
- **Actor model / no mutexes**: `RaftCore` is exclusively owned by the event loop task. All external communication flows through channels.
- **Single tick metronome**: One fixed-interval timer. All timeout logic (election, heartbeat) is internal to `RaftCore` via tick counting.
- **`RaftNode`** (planned): Application-facing handle that holds only a channel sender to the event loop.

## RaftCore Implementation Status

**Completed:**
- Leader election with Section 5.4.1 log up-to-date check
- Log replication with propose, AppendEntries, commitment via sorted match_index median
- Section 5.4.2 safety: no-op entry on election (Section 8) ensures previous-term entries commit
- Log conflict resolution with Section 5.3 optimization (follower reports conflicting term/index for faster catch-up)
- Persistence actions (`PersistMetadata`, `PersistLogEntries`) emitted at all required points
- `restore()` constructor for crash recovery from persisted state

**Next up:**
- Log compaction / snapshots (Section 7)
- Cluster membership changes (Section 6) — likely single-server changes first

## Key Source Files

- `crates/raftcore/src/types.rs` — All types: `Action`, `Message`, RPCs, `LogEntry`, `RaftState`, `NodeId`
- `crates/raftcore/src/lib.rs` — `RaftCore` implementation + all unit tests with `TestCluster` harness

## Testing Strategy

Tests are built in phases — each layer tested before the next is built:

1. **RaftCore unit tests** (current) — Sync, no async runtime. `TestCluster` harness owns all nodes and manages message delivery with `deliver_all()`, `deliver_to()`, `partition()`, `heal()`, `propose_and_sync()`. Surgical tests for specific edge cases use direct node manipulation.
2. **Integration tests** — Multi-node in-process clusters using `MemoryTransport` (future).
3. **Network tests** — Real TCP connections via `TcpTransport` (future).
4. **KV store tests** — Application layer on top (future).

## Key Dependencies

- `rand` — election timeout randomization
- `serde` — serialization derives on message types

Future: `tokio`, `tracing`, `bincode`/`postcard`, `async-trait`
