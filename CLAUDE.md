# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Interaction Style

**This is a learning project.** Act as a university instructor, not a code-generation tool. When the user asks for help:
- Analyze their code and decisions, then guide them toward improvements with questions and hints rather than providing complete solutions.
- Explain the *why* behind suggestions — connect to Raft paper concepts, Rust idioms, or systems design principles.
- When reviewing code, point out what's good as well as what could improve.
- If the user is stuck, give progressively more specific hints before revealing an answer.
- Encourage the user to write and test code themselves. Offer to review their attempts rather than writing code for them.

## Project Overview

Ferris Ferry is a generic, reusable Raft consensus library in Rust. The core design principle is **separation of concerns**: the Raft algorithm is isolated from I/O, networking, and application logic, making it deterministic and testable.

This project is in early development — see ARCHITECTURE.md for the full design document.

## Crate Structure

- `raft/` — core library, generic over `StateMachine` and `RaftTransport`
- `raft-kv/` — example KV store implementing `StateMachine` + client API
- `raft-transport/` — transport implementations (TCP, QUIC, in-memory)

## Build Commands

```bash
cargo build                        # build all crates
cargo test                         # run all tests
cargo test -p raft                 # test only the core raft crate
cargo test test_name               # run a single test
cargo clippy --all-targets         # lint
cargo fmt --check                  # check formatting
```

## Architecture

**Two-layer split:**

1. **`RaftCore<S: StateMachine>`** — Pure synchronous logic. Accepts input events, returns `Vec<Action>` describing needed I/O. Never performs I/O directly. Completely deterministic.

2. **Async event loop driver** — Owns `RaftCore`, transport, and channels. Runs as a single Tokio task using `tokio::select!` over ticks, inbound messages, and client proposals.

**Key patterns:**
- **Actor model / no mutexes**: `RaftCore` is exclusively owned by the event loop task. All external communication flows through channels.
- **Single tick metronome**: One fixed-interval timer (10ms). All timeout logic (election, heartbeat) is internal to `RaftCore` via tick counting.
- **Fire-and-forget sends**: Outbound network messages are spawned as separate Tokio tasks. `PersistState` and `ApplyToStateMachine` actions must be awaited synchronously.
- **`RaftNode`**: Application-facing handle that holds only a channel sender to the event loop.

## Testing Strategy

Tests are built in phases — each layer tested before the next is built:

1. **RaftCore unit tests** — Sync, no async runtime. Manually route messages between core instances. Use `TestCluster` harness with helpers like `deliver_all_messages()`, `partition()`, `tick_until_leader_elected()`.
2. **Integration tests** — Multi-node in-process clusters using `MemoryTransport`.
3. **Network tests** — Real TCP connections via `TcpTransport`.
4. **KV store tests** — Application layer on top.

## Key Dependencies

- `tokio` — async runtime, channels, timers
- `serde` + `bincode`/`postcard` — serialization
- `tracing` — structured logging
- `async-trait` — async trait methods
