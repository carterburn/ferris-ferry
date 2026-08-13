# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.97

# --- chef: shared base with cargo-chef installed ---
FROM rust:${RUST_VERSION}-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /build

# --- planner: distill the dependency graph into recipe.json ---
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --- builder: cook deps (cached on recipe.json alone), then build our source ---
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p raft-kv

# --- runtime ---
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# /var/lib is root-owned 0755, so an unprivileged process cannot create a
# directory under it. Pre-create the default --directory so a bare `docker run`
# works; any other --directory must be supplied as a mount.
RUN groupadd --gid 10001 raft \
 && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin raft \
 && mkdir -p /var/lib/raft \
 && chown 10001:10001 /var/lib/raft

COPY --from=builder /build/target/release/raft-kv /usr/local/bin/raft-kv

# Numeric so Kubernetes `runAsNonRoot` can validate it.
USER 10001:10001
EXPOSE 3000 9000
ENTRYPOINT ["/usr/local/bin/raft-kv"]
