# ADR 006: PostgreSQL Schema Design

## Status

Accepted

## Context

The persister writes high volumes of events to PostgreSQL:
- Write rate: 1,000-10,000 events/second sustained
- Query patterns: By slot, by sequence, by event type
- Rollback support: Soft delete for reorg handling
- Idempotency: Upserts must not create duplicates

Schema design directly impacts:
- Write throughput
- Query performance
- Storage efficiency
- Operational complexity

## Decision

### Events Table

```sql
CREATE TABLE events (
    -- Identity
    id UUID PRIMARY KEY,
    sequence BIGINT NOT NULL,
    slot BIGINT NOT NULL,
    parent_slot BIGINT,

    -- Classification
    kind VARCHAR(50) NOT NULL,

    -- Payload
    data JSONB NOT NULL,
    event_hash VARCHAR(64) NOT NULL,

    -- Timestamps
    received_at TIMESTAMPTZ NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Soft delete support
    is_valid BOOLEAN NOT NULL DEFAULT true,
    invalidated_at TIMESTAMPTZ,

    -- Idempotency constraint
    UNIQUE (slot, sequence)
);
```

### Index Strategy

#### Primary Lookups

```sql
-- Idempotent upserts and slot+sequence queries
CREATE UNIQUE INDEX idx_events_slot_seq ON events (slot, sequence);
```

#### Slot-Based Queries

```sql
-- Get all events for a slot (filtering invalid)
CREATE INDEX idx_events_slot_valid ON events (slot)
WHERE is_valid = true;
```

#### Sequence-Based Ordering

```sql
-- Get events in sequence order
CREATE INDEX idx_events_sequence ON events (sequence);
```

#### Event Type Filtering

```sql
-- Query by event kind
CREATE INDEX idx_events_kind ON events (kind)
WHERE is_valid = true;
```

#### Deduplication

```sql
-- Check for duplicate event content
CREATE INDEX idx_events_hash ON events (event_hash);
```

### Idempotent Upsert Pattern

```sql
INSERT INTO events (
    id, sequence, slot, parent_slot, kind,
    data, event_hash, received_at, indexed_at, is_valid
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, true)
ON CONFLICT (slot, sequence) DO UPDATE SET
    -- Only update if new data has higher sequence
    data = CASE WHEN EXCLUDED.sequence > events.sequence
           THEN EXCLUDED.data ELSE events.data END,
    updated_at = NOW()
RETURNING (xmax = 0) AS inserted;
```

The `xmax = 0` trick detects whether this was an INSERT or UPDATE.

### Cursors Table

```sql
CREATE TABLE cursors (
    name VARCHAR(100) PRIMARY KEY,
    slot BIGINT NOT NULL,
    sequence BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Used for:
- Real-time cursor position
- Backfill cursor position
- Recovery checkpoints

### Dead Letter Queue Table

```sql
CREATE TABLE dead_letter_queue (
    id UUID PRIMARY KEY,
    raw_payload BYTEA NOT NULL,
    sequence BIGINT,
    slot BIGINT,
    kind VARCHAR(50),
    error_message TEXT NOT NULL,
    error_category VARCHAR(50) NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    failed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ,
    last_retry_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    is_resolved BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_unresolved ON dead_letter_queue (failed_at)
WHERE is_resolved = false;
```

### Slots Table (Chain Tracking)

```sql
CREATE TABLE slots (
    slot BIGINT PRIMARY KEY,
    parent_slot BIGINT NOT NULL,
    block_hash VARCHAR(88),
    status VARCHAR(20) NOT NULL,
    block_time BIGINT,
    transaction_count INTEGER NOT NULL DEFAULT 0,
    received_at TIMESTAMPTZ NOT NULL,
    confirmed_at TIMESTAMPTZ,
    rooted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_slots_parent ON slots (parent_slot);
CREATE INDEX idx_slots_status ON slots (status);
```

### Partitioning Consideration

For very large deployments, partition by slot range:

```sql
CREATE TABLE events (
    -- columns same as above
) PARTITION BY RANGE (slot);

CREATE TABLE events_0_1m PARTITION OF events
    FOR VALUES FROM (0) TO (1000000);

CREATE TABLE events_1m_2m PARTITION OF events
    FOR VALUES FROM (1000000) TO (2000000);
```

**Note**: Not implemented by default. Add when table exceeds 100M rows.

### Batch Write Optimization

Write in batches within transactions:

```sql
BEGIN;
INSERT INTO events VALUES (...), (...), (...);
-- Up to 1000 rows per batch
COMMIT;
```

Benefits:
- Reduced round trips
- Atomic batch success/failure
- Better WAL efficiency

### Vacuum and Maintenance

Recommended settings for high-write tables:

```sql
ALTER TABLE events SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_cost_limit = 1000
);
```

## Consequences

### Positive

- **High write throughput**: Batch inserts with upserts
- **Query flexibility**: JSONB payload supports varied queries
- **Rollback support**: Soft delete preserves audit trail
- **Idempotency**: UNIQUE constraint prevents duplicates

### Negative

- **JSONB overhead**: Larger storage than binary formats
- **Index maintenance**: Multiple indexes slow writes slightly
- **Vacuum pressure**: High write rate requires aggressive vacuuming

### Mitigations

- JSONB compression via TOAST
- Partial indexes reduce index size
- Autovacuum tuning for workload

## Alternatives Considered

### 1. Separate Tables per Event Kind

Create `account_updates`, `transactions`, etc.

**Rejected because**: Complicates rollback (must update multiple tables), schema changes harder.

### 2. Binary Payload Storage

Store protobuf/bincode instead of JSONB.

**Rejected because**: Loses queryability, debugging harder.

### 3. Time-Series Database (TimescaleDB)

Use TimescaleDB hypertables.

**Rejected because**: Adds operational complexity, partitioning handles scale needs.

## Migration Strategy

```sql
-- 001_initial_schema.sql
CREATE TABLE events (...);
CREATE TABLE cursors (...);
CREATE TABLE dead_letter_queue (...);
CREATE TABLE slots (...);

-- 002_add_indexes.sql
CREATE INDEX CONCURRENTLY ...;
```

Use `sqlx migrate` for versioned migrations.

## References

- PostgreSQL UPSERT: https://www.postgresql.org/docs/current/sql-insert.html
- Partial indexes: https://www.postgresql.org/docs/current/indexes-partial.html
- JSONB performance: https://www.postgresql.org/docs/current/datatype-json.html
