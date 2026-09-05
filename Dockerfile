# Build stage.
#
# The toolchain is pinned to match rust-toolchain.toml; a mismatch here shows up
# as a dependency that will not resolve rather than as a clear error.
FROM rust:1.88-bookworm AS builder

WORKDIR /app

# protoc is a build requirement: the ingester compiles the stream protocol from
# proto/slot_stream.proto through tonic-build.
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Everything the build actually reads. migrations/ is not optional: the persister
# embeds it at compile time with sqlx::migrate!, so a missing directory is a
# compile error, not a runtime one.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY proto ./proto
COPY migrations ./migrations

RUN cargo build --release --workspace --locked

# Runtime stage.
FROM debian:bookworm-slim

WORKDIR /app

# curl is here for the healthcheck below, which is otherwise a no-op that always
# reports unhealthy.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/slot-stream /usr/local/bin/slot-stream
COPY --from=builder /app/target/release/slot-stream-source /usr/local/bin/slot-stream-source

RUN useradd -m -s /bin/bash slotstream
USER slotstream

ENV RUST_LOG=info
ENV DATABASE_URL=postgres://postgres:postgres@postgres:5432/slot_stream
ENV GRPC_ENDPOINT=http://source:10000

# The API and the Prometheus scrape both live on this port.
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8080/health/ready || exit 1

# The indexer takes no arguments; it is configured through CONFIG_FILE and the
# environment.
ENTRYPOINT ["slot-stream"]
