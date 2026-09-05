# slot-stream

A high-throughput, reorg-immune Solana indexing pipeline that ingests Geyser/gRPC streams, handles chain reorganizations correctly, and persists structured events to PostgreSQL with exactly-once semantics.

## Hard Problems This Solves

### 1. Chain Reorg/Fork Handling

Solana occasionally experiences chain reorganizations where confirmed blocks become orphaned. slot-stream detects forks by tracking slot-parent relationships and executes automatic rollbacks to invalidate orphaned data.

**Solution**: Slot-parent chain tracking with soft-delete rollback. Events from orphaned forks are marked invalid, preserving audit trail while ensuring query correctness.

### 2. Backpressure Management

The gRPC stream can produce 50,000+ events/second during peak activity. Without backpressure, memory grows unboundedly leading to OOM crashes.

**Solution**: Bounded memory buffers with configurable overflow policies (drop-oldest, drop-newest, or block). Decouples ingestion from processing so slow downstream never blocks the stream.

### 3. Dead Letter Queue

Events can fail for various reasons (malformed data, validation errors, database failures). Without a DLQ, failed events are silently lost.

**Solution**: All failed events go to a persistent DLQ with error categorization, retry support, and replay mechanisms. No silent data loss.

### 4. Backfill + Real-time Merge

Historical backfill and real-time streaming must merge correctly without gaps or duplicates.

**Solution**: Dual cursor system with deduplication by (slot, sequence) pair. Gap detection triggers automatic backfill. Both sources merge through a unified event merger.

### 5. Exactly-Once Semantics

Network issues, reconnects, and restarts can cause duplicate events.

**Solution**: Monotonic sequence numbers combined with idempotent upserts. UNIQUE(slot, sequence) constraint ensures duplicates are safely ignored.

## Architecture

```
gRPC Stream --> Ingester --> Bounded Buffer --> Processor --> Persister --> PostgreSQL
                   |              |                |
                   |              |                +--> Reorg Detector
                   |              |                         |
                   |              +--> Backpressure         +--> Rollback
                   |
                   +--> DLQ (failed events)

RPC -----------> Backfill Engine ----+
                                     |
                                     +--> Event Merger --> Persister
```

## Crate Structure

| Crate | Description |
|-------|-------------|
| `slot-stream-common` | Shared types, sequence numbers, slot metadata, errors |
| `slot-stream-ingester` | gRPC stream consumer, sequence tracking, bounded buffers |
| `slot-stream-processor` | Event processing, reorg detection, handler routing |
| `slot-stream-persister` | PostgreSQL writer with idempotent upserts, rollback support |
| `slot-stream-backfill` | Historical data backfill engine, merge strategy |
| `slot-stream-dlq` | Dead letter queue implementation, replay mechanism |

## Quick Start

### Prerequisites

- Rust 1.75+
- PostgreSQL 14+
- Access to a Solana validator with Geyser plugin (e.g., Yellowstone gRPC)

### Setup

```bash
# Clone and build
git clone https://github.com/your-org/slot-stream
cd slot-stream
cargo build --release

# Start PostgreSQL (using Docker)
docker-compose up -d postgres

# Run migrations
export DATABASE_URL="postgres://localhost/slot_stream"
sqlx migrate run

# Run the indexer
./target/release/slot-stream \
    --geyser-endpoint http://validator:10000 \
    --database-url postgres://localhost/slot_stream
```

### Docker

```bash
# Build image
docker build -t slot-stream .

# Run with docker-compose
docker-compose up
```

## Configuration

```toml
# config.toml

[ingester]
buffer_capacity = 100000
overflow_policy = "drop_oldest"  # or "drop_newest", "block"
reconnect_max_attempts = 10

[processor]
worker_count = 4
batch_size = 100

[persister]
database_url = "postgres://localhost/slot_stream"
max_connections = 20
batch_size = 1000
batch_timeout_ms = 100

[backfill]
rpc_endpoint = "https://api.mainnet-beta.solana.com"
batch_size = 100
rate_limit_rps = 100

[dlq]
max_retries = 3
auto_retry = false
```

## Key Design Decisions

See the [Architecture Decision Records](docs/adr/) for detailed rationale:

- [ADR-001: Reorg Detection Strategy](docs/adr/001-reorg-detection.md)
- [ADR-002: Sequence Ordering Guarantees](docs/adr/002-sequence-ordering.md)
- [ADR-003: Backpressure Model](docs/adr/003-backpressure-model.md)
- [ADR-004: Dead Letter Queue Design](docs/adr/004-dlq-design.md)
- [ADR-005: Backfill Merge Strategy](docs/adr/005-backfill-strategy.md)
- [ADR-006: PostgreSQL Schema Design](docs/adr/006-postgres-schema.md)

## Monitoring

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `ingester.events_received` | Events from gRPC stream | - |
| `ingester.events_dropped` | Events dropped due to backpressure | > 0 |
| `processor.reorgs_detected` | Chain reorgs detected | > 5/hour |
| `persister.write_latency_p99` | Database write latency | > 100ms |
| `dlq.unresolved_count` | Failed events pending | > 100 |
| `backfill.gaps_pending` | Sequence gaps to fill | > 10 |

### Health Endpoints

```
GET /health/ready    # All connections established
GET /health/live     # Pipeline processing
GET /metrics         # Prometheus metrics
```

## Development

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=info cargo run

# Format code
cargo fmt

# Lint
cargo clippy
```

## License

MIT License - see [LICENSE](LICENSE)
