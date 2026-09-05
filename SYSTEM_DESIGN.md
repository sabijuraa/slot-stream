# slot-stream System Design

## Overview

slot-stream is a high-throughput, reorg-immune Solana indexing pipeline that ingests Geyser/gRPC streams and persists structured events to Postgres with exactly-once semantics.

## Architecture

```
                                    ┌─────────────────────────────────────────────────────────┐
                                    │                    slot-stream                          │
                                    │                                                         │
┌──────────────┐                    │  ┌─────────────┐     ┌─────────────┐     ┌───────────┐ │
│   Solana     │    gRPC/Geyser     │  │             │     │             │     │           │ │
│  Validator   │───────────────────▶│  │  Ingester   │────▶│  Processor  │────▶│ Persister │ │
│              │                    │  │             │     │             │     │           │ │
└──────────────┘                    │  └─────────────┘     └──────┬──────┘     └─────┬─────┘ │
                                    │         │                   │                  │       │
                                    │         │            ┌──────┴──────┐           │       │
                                    │         │            │             │           │       │
                                    │         │            │   Reorg     │           │       │
                                    │         │            │  Detector   │           │       │
                                    │         │            │             │           │       │
                                    │         │            └──────┬──────┘           │       │
                                    │         │                   │                  │       │
                                    │         │  ┌────────────────┴──────────────────┘       │
                                    │         │  │                                           │
┌──────────────┐                    │  ┌──────▼──▼───┐     ┌─────────────┐                   │
│   Solana     │    JSON-RPC        │  │             │     │             │                   │
│     RPC      │◀───────────────────│  │  Backfill   │     │     DLQ     │                   │
│              │                    │  │   Engine    │     │             │                   │
└──────────────┘                    │  └─────────────┘     └─────────────┘                   │
                                    │                                                         │
                                    └────────────────────────────────────────┬────────────────┘
                                                                             │
                                                                             ▼
                                                                    ┌─────────────────┐
                                                                    │                 │
                                                                    │    PostgreSQL   │
                                                                    │                 │
                                                                    └─────────────────┘
```

## Data Flow

### 1. Ingestion Path

```
gRPC Stream → Parser → Sequence Tracker → Bounded Buffer → Processor
     │            │            │                │
     │            │            │                └─► Backpressure signals
     │            │            │
     │            │            └─► Gap detection → Backfill queue
     │            │
     │            └─► Parse errors → DLQ
     │
     └─► Connection errors → Reconnect with backoff
```

### 2. Processing Path

```
Bounded Buffer → Event Handler → Slot Chain Tracker → Output
       │              │                  │
       │              │                  └─► Fork detected?
       │              │                            │
       │              │                     ┌──────┴──────┐
       │              │                     │             │
       │              │                    Yes           No
       │              │                     │             │
       │              │                     ▼             │
       │              │              Create Rollback      │
       │              │                  Plan             │
       │              │                     │             │
       │              │                     ▼             │
       │              │              Mark Orphaned        │
       │              │                  Slots            │
       │              │                     │             │
       │              │                     ▼             │
       │              │              Invalidate in DB     │
       │              │                     │             │
       │              └─────────────────────┴─────────────┘
       │
       └─► Handler errors → DLQ
```

### 3. Persistence Path

```
Processed Events → Batch Writer → Transaction → Commit
        │               │              │            │
        │               │              │            └─► Update cursor
        │               │              │
        │               │              └─► Constraint violation?
        │               │                        │
        │               │                  ┌─────┴─────┐
        │               │                  │           │
        │               │                 Yes         No
        │               │                  │           │
        │               │                  ▼           │
        │               │              Skip (idempotent)
        │               │                  │           │
        │               └──────────────────┴───────────┘
        │
        └─► Write errors (after retry) → DLQ
```

## Reorg Detection Algorithm

### Slot-Parent Chain Tracking

```
Canonical Chain:
  Slot 100 (parent: 99) → Slot 101 (parent: 100) → Slot 102 (parent: 101)

Fork Scenario:
  Slot 100 (parent: 99) → Slot 101 (parent: 100) → Slot 102 (parent: 101)
                                                          ↑
                                                    Current head

  New block arrives:
  Slot 102 (parent: 100)  ← Different parent!
           ↑
      Fork detected!
```

### Detection Steps

1. **Receive new slot S with parent P**
2. **Check existing record for slot S**
   - If exists with parent P' ≠ P, fork detected
   - If exists with parent P, status update (ignore)
   - If not exists, insert new record

3. **On fork detection:**
   - Find common ancestor (divergence point)
   - Identify slots to rollback (S > divergence AND on old chain)
   - Create rollback plan

### Rollback Execution

```
Before Rollback:
  events table:
  | slot | seq | is_valid |
  |------|-----|----------|
  | 100  | 1   | true     |
  | 101  | 2   | true     |
  | 102  | 3   | true     |  ← To be invalidated

After Rollback:
  events table:
  | slot | seq | is_valid | invalidated_at |
  |------|-----|----------|----------------|
  | 100  | 1   | true     | NULL           |
  | 101  | 2   | true     | NULL           |
  | 102  | 3   | false    | 2024-01-01...  |  ← Soft deleted
```

## Sequence Number Ordering

### Invariants

1. **Monotonically increasing**: seq(n+1) > seq(n) within a stream
2. **Gap = missed events**: If we see seq 5 then seq 8, we missed 6,7
3. **Regression = reorg or reconnect**: If current is 100 and we see 50

### Handling

```
Sequence Tracker State Machine:

  INITIAL ──[first event]──▶ TRACKING
      │                          │
      │                    [next in order]
      │                          │
      │                          ▼
      │                      TRACKING
      │                          │
      │                    [gap detected]
      │                          │
      │                          ▼
      │                    LOG GAP + TRACKING
      │                          │
      │                    [regression]
      │                          │
      │                          ▼
      │                      REORG?
      │                          │
      └──────────────────────────┘
```

## Backpressure Model

### Bounded Channel Architecture

```
Ingester                  Buffer                    Processor
   │                        │                          │
   │──────push event───────▶│                          │
   │                        │                          │
   │                        │◀────pull event───────────│
   │                        │                          │
   │──────push event───────▶│ [buffer filling]         │
   │                        │                          │
   │                        │ [buffer FULL]            │
   │                        │                          │
   │◀─────backpressure──────│                          │
   │                        │                          │
   │  [apply policy]        │                          │
   │                        │                          │
```

### Overflow Policies

| Policy      | Behavior                    | Use Case                      |
|-------------|-----------------------------|------------------------------ |
| DropOldest  | Remove oldest, add new      | Best-effort real-time         |
| DropNewest  | Reject incoming events      | Preserve history              |
| Block       | Wait for space              | Guaranteed delivery           |

### Buffer Sizing

```
Recommended buffer size = (max_events_per_slot × expected_lag_slots × 2)

Example:
  - Max events/slot: 1000
  - Expected lag: 50 slots
  - Buffer: 1000 × 50 × 2 = 100,000 events
```

## DLQ Flow

### When Events Go to DLQ

```
Event Sources for DLQ:

1. Parse Errors
   gRPC message ──▶ Parser ──[malformed]──▶ DLQ

2. Validation Errors
   Parsed event ──▶ Validator ──[schema mismatch]──▶ DLQ

3. Handler Errors
   Valid event ──▶ Handler ──[processing failed]──▶ DLQ

4. Persistence Errors (after retry)
   Processed event ──▶ Writer ──[DB error × 3]──▶ DLQ
```

### Replay Mechanism

```
DLQ Entry Lifecycle:

  PENDING ──[retry]──▶ RETRYING ──[success]──▶ RESOLVED
     │                     │
     │                     │──[failure]──▶ PENDING (retry++)
     │                     │
     │                     └──[exhausted]──▶ EXHAUSTED
     │
     └──[manual resolve]──▶ RESOLVED
```

## Backfill Merge Strategy

### Dual Cursor System

```
Timeline:
|----[HISTORICAL]----[MERGE ZONE]----[REAL-TIME]----|
         ↑                                 ↑
    Backfill                          Real-time
    Cursor                             Cursor

Merge Zone:
  - Both backfill and real-time may produce events
  - Deduplication by (slot, sequence)
  - Merge strategy: KeepFirst (default)
```

### Cursor Management

```
Cursor States:

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  backfill   │     │   merge     │     │  realtime   │
│   cursor    │     │   cursor    │     │   cursor    │
│             │     │             │     │             │
│ slot: 1000  │ ──▶ │ slot: 5000  │ ──▶ │ slot: 10000 │
│             │     │ (overlap)   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
```

## Postgres Schema Design

### Events Table

```sql
CREATE TABLE events (
    id UUID PRIMARY KEY,
    sequence BIGINT NOT NULL,           -- Ordering
    slot BIGINT NOT NULL,               -- Chain position
    parent_slot BIGINT,                 -- Fork detection
    kind VARCHAR(50) NOT NULL,          -- Event type
    data JSONB NOT NULL,                -- Payload
    event_hash VARCHAR(64) NOT NULL,    -- Deduplication
    received_at TIMESTAMPTZ NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL,
    is_valid BOOLEAN DEFAULT true,      -- Soft delete
    invalidated_at TIMESTAMPTZ,

    UNIQUE (slot, sequence)             -- Idempotent upserts
);
```

### Indexes Strategy

| Index                          | Purpose                    |
|--------------------------------|----------------------------|
| (slot, sequence) UNIQUE        | Primary lookup + upserts   |
| (slot) WHERE is_valid          | Slot-based queries         |
| (sequence)                     | Sequence-based ordering    |
| (event_hash)                   | Deduplication checks       |
| (kind) WHERE is_valid          | Type filtering             |

## Failure Modes and Recovery

### Connection Failure

```
Scenario: gRPC connection lost

Recovery:
1. Log connection error
2. Enter reconnection loop with exponential backoff
3. On reconnect, check last cursor position
4. Resume from cursor (may have gaps)
5. Gaps detected → queue for backfill
```

### Processing Failure

```
Scenario: Handler throws error

Recovery:
1. Check error type (retryable? DLQ-worthy? fatal?)
2. If retryable: retry with backoff
3. If DLQ-worthy: send to DLQ, continue
4. If fatal: stop pipeline, alert operator
```

### Database Failure

```
Scenario: Postgres unavailable

Recovery:
1. Buffer events in memory
2. Retry connection with backoff
3. If buffer fills: apply overflow policy
4. On reconnect: flush buffer
5. Update metrics/alerts
```

### Reorg During Recovery

```
Scenario: Reorg while catching up

Recovery:
1. Detect fork via slot-parent mismatch
2. Pause current processing
3. Execute rollback plan
4. Reset cursor to divergence point
5. Resume processing
```

## Observability

### Key Metrics

| Metric                         | Type      | Alert Threshold        |
|--------------------------------|-----------|------------------------|
| ingester.events_received       | Counter   | -                      |
| ingester.events_dropped        | Counter   | > 0                    |
| processor.events_processed     | Counter   | -                      |
| processor.reorgs_detected      | Counter   | > 5/hour               |
| persister.write_latency        | Histogram | p99 > 100ms            |
| dlq.unresolved_count           | Gauge     | > 100                  |
| backfill.gaps_pending          | Gauge     | > 10                   |

### Health Checks

```
/health/ready    - All connections established
/health/live     - Pipeline processing events
/metrics         - Prometheus endpoint
```

## Configuration

### Critical Parameters

```toml
[ingester]
buffer_capacity = 100000       # Events in memory
overflow_policy = "drop_oldest"
reconnect_max_attempts = 10

[processor]
worker_count = 4
batch_size = 100
max_latency_ms = 5000

[persister]
max_connections = 20
batch_size = 1000
batch_timeout_ms = 100

[backfill]
batch_size = 100
rate_limit_rps = 100
max_concurrent = 4

[dlq]
max_retries = 3
auto_retry = false
```

## Deployment Topology

```
Production Setup:

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Validator  │     │  Validator  │     │  Validator  │
│   Node 1    │     │   Node 2    │     │   Node 3    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────▼──────┐
                    │   HAProxy   │
                    │ (failover)  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼──────┐  ┌──▼───┐  ┌─────▼─────┐
       │ slot-stream │  │ ...  │  │slot-stream│
       │  Instance 1 │  │      │  │ Instance N│
       └──────┬──────┘  └──────┘  └─────┬─────┘
              │                         │
              └────────────┬────────────┘
                           │
                    ┌──────▼──────┐
                    │  PostgreSQL │
                    │   Primary   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  PostgreSQL │
                    │   Replica   │
                    └─────────────┘
```
