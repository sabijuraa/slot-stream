# Build stage
FROM rust:1.75-bookworm as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build dependencies first (cached layer)
RUN cargo build --release --workspace 2>/dev/null || true

# Build the actual application
RUN cargo build --release --workspace

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy built binaries
COPY --from=builder /app/target/release/slot-stream* /usr/local/bin/

# Copy migrations for runtime use
COPY migrations /app/migrations

# Create non-root user
RUN useradd -m -s /bin/bash slotstream
USER slotstream

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health/live || exit 1

# Default command
ENTRYPOINT ["slot-stream"]
CMD ["--help"]

# Environment variables
ENV RUST_LOG=info
ENV DATABASE_URL=postgres://postgres:postgres@postgres:5432/slot_stream

# Expose metrics port
EXPOSE 8080
