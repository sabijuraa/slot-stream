-- slot-stream initial schema.
--
-- Two things here are load-bearing and worth reading before changing:
--
-- 1. Row identity is (slot, source_seq), not (slot, seq). `seq` is the ordering
--    number this pipeline assigns as it commits, so a replayed event gets a new
--    one; using it as the key would let crash recovery insert duplicates.
--    `source_seq` comes from the stream and is stable across replays.
--
-- 2. Reorg rollback is a soft delete (is_valid = false). Readers filter on
--    is_valid, and the upsert path resets it, so re-applying an event that was
--    previously orphaned brings the row back rather than silently leaving it
--    invisible. See ADR-001 and ADR-007.

CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,

    -- Ordering assigned by the processor. Monotonic, resumed from MAX on restart.
    seq BIGINT NOT NULL,

    -- The stream's own sequence. Stable identity for idempotent writes.
    source_seq BIGINT NOT NULL,

    slot BIGINT NOT NULL,
    parent_slot BIGINT,

    kind VARCHAR(32) NOT NULL,
    data JSONB NOT NULL,
    event_hash VARCHAR(64) NOT NULL,

    received_at TIMESTAMPTZ NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Soft delete for reorg rollback.
    is_valid BOOLEAN NOT NULL DEFAULT true,
    invalidated_at TIMESTAMPTZ,

    CONSTRAINT events_identity UNIQUE (slot, source_seq)
);

-- The read path is almost always "valid events in this slot range, in order".
CREATE INDEX IF NOT EXISTS idx_events_slot_seq_valid
    ON events (slot, seq) WHERE is_valid = true;

CREATE INDEX IF NOT EXISTS idx_events_seq_valid
    ON events (seq) WHERE is_valid = true;

-- Rollback and its inverse both address whole slots.
CREATE INDEX IF NOT EXISTS idx_events_slot ON events (slot);

CREATE INDEX IF NOT EXISTS idx_events_kind_valid
    ON events (kind, slot) WHERE is_valid = true;

CREATE INDEX IF NOT EXISTS idx_events_hash ON events (event_hash);

-- Lookups by signature/account go through the payload.
CREATE INDEX IF NOT EXISTS idx_events_data_gin ON events USING GIN (data jsonb_path_ops);

-- The events table is write-heavy and reorg churn creates dead tuples quickly.
ALTER TABLE events SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.005
);

-- Chain structure, so the fork detector can be rebuilt after a restart instead
-- of starting blind and mistaking the first slot for a fresh chain.
CREATE TABLE IF NOT EXISTS slots (
    slot BIGINT PRIMARY KEY,
    parent_slot BIGINT NOT NULL,
    block_hash VARCHAR(88),
    status VARCHAR(20) NOT NULL,
    is_canonical BOOLEAN NOT NULL DEFAULT true,
    block_time BIGINT,
    transaction_count INTEGER NOT NULL DEFAULT 0,
    received_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_slots_parent ON slots (parent_slot);
CREATE INDEX IF NOT EXISTS idx_slots_canonical ON slots (slot) WHERE is_canonical = true;

-- Commit position, read on startup to resume without gaps or duplicates.
CREATE TABLE IF NOT EXISTS cursors (
    name VARCHAR(100) PRIMARY KEY,
    slot BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    source_seq BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Audit trail of reorgs, so an operator can answer "what happened at 03:14".
CREATE TABLE IF NOT EXISTS reorgs (
    id UUID PRIMARY KEY,
    fork_slot BIGINT NOT NULL,
    divergence_point BIGINT NOT NULL,
    expected_parent BIGINT NOT NULL,
    actual_parent BIGINT NOT NULL,
    rollback_depth INTEGER NOT NULL,
    events_invalidated BIGINT NOT NULL DEFAULT 0,
    slots_rolled_back BIGINT[] NOT NULL DEFAULT '{}',
    divergence_is_bound BOOLEAN NOT NULL DEFAULT false,
    detected_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_reorgs_detected_at ON reorgs (detected_at DESC);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id UUID PRIMARY KEY,
    raw_payload BYTEA NOT NULL,
    sequence BIGINT,
    slot BIGINT,
    parent_slot BIGINT,
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

CREATE INDEX IF NOT EXISTS idx_dlq_unresolved
    ON dead_letter_queue (failed_at) WHERE is_resolved = false;

CREATE INDEX IF NOT EXISTS idx_dlq_category
    ON dead_letter_queue (error_category) WHERE is_resolved = false;

CREATE INDEX IF NOT EXISTS idx_dlq_slot
    ON dead_letter_queue (slot) WHERE is_resolved = false;
