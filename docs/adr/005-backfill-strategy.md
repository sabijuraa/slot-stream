# ADR 005: Historical Backfill and Real-time Merge Strategy

## Status

Accepted

## Context

A complete indexing pipeline must support:
1. **Real-time streaming**: Live events from Geyser/gRPC
2. **Historical backfill**: Catching up on past data
3. **Gap filling**: Recovering missed events from sequence gaps

The challenge: these sources produce overlapping data that must be merged correctly:
- Duplicates must be eliminated
- Ordering must be preserved
- Neither source can block the other

## Decision

### Dual Cursor System

We maintain two logical cursors:

```
Timeline:
|----[HISTORICAL]----[MERGE ZONE]----[REAL-TIME]----|
         ↑                                 ↑
    Backfill                          Real-time
    Cursor                             Cursor
```

- **Backfill Cursor**: Moves forward through historical slots
- **Real-time Cursor**: Tracks live stream position
- **Merge Zone**: Overlap where deduplication occurs

### Merge Strategy

Events from both sources flow through the EventMerger:

```rust
pub struct EventMerger {
    strategy: MergeStrategy,
    buffer: BTreeMap<(u64, u64), IndexedEvent>, // (slot, seq) -> event
}

pub enum MergeStrategy {
    KeepFirst,           // First event wins (default)
    KeepHigherSequence,  // Higher sequence number wins
    KeepNewest,          // Most recent received_at wins
}
```

Default strategy: **KeepFirst**
- Simple and predictable
- Backfill events don't overwrite live events
- Live events don't overwrite already-persisted backfill

### Deduplication Key

Events are deduplicated by `(slot, sequence)`:

```sql
UNIQUE (slot, sequence) -- In events table
```

This ensures:
- Same event from different sources merges correctly
- Database upserts are idempotent
- No duplicate entries possible

### Backfill Queue Priority

Gap fills take priority over full historical backfill:

```rust
pub struct BackfillRange {
    start_slot: u64,
    end_slot: u64,
    priority: u32,        // Lower = higher priority
    is_gap_fill: bool,
}

impl BackfillQueue {
    pub fn push(&mut self, range: BackfillRange) {
        self.ranges.push(range);
        self.ranges.sort_by_key(|r| r.priority);
    }
}
```

Priority levels:
- 1: Gap fill (detected by sequence tracker)
- 10: Normal backfill (initial historical load)

### Backfill Rate Limiting

RPC endpoints have rate limits. We respect them:

```rust
pub struct BackfillConfig {
    batch_size: usize,           // Slots per request
    batch_delay: Duration,       // Delay between batches
    max_concurrent: usize,       // Parallel requests
    rate_limit: Option<u32>,     // Requests per second cap
}
```

Default: 100 RPS, 100 slots/batch, 4 concurrent requests

### Cursor Persistence

Both cursors are persisted for recovery:

```sql
INSERT INTO cursors (name, slot, sequence, updated_at)
VALUES ('realtime', $1, $2, NOW())
ON CONFLICT (name) DO UPDATE SET
    slot = EXCLUDED.slot,
    sequence = EXCLUDED.sequence,
    updated_at = NOW();
```

On restart:
1. Load cursors from database
2. Resume real-time from `realtime` cursor
3. Resume backfill from `backfill` cursor

### Merge Zone Handling

When backfill catches up to real-time:

```
Phase 1: Backfill far behind
|---[backfill]------------------------[realtime]---|
    No overlap, both process independently

Phase 2: Approaching merge zone
|------------------[backfill]----[realtime]--------|
    Start deduplication, expect some duplicates

Phase 3: Caught up
|----------------------------[both]----------------|
    Backfill pauses, only real-time active
    Backfill resumes on detected gaps
```

## Consequences

### Positive

- **Complete coverage**: Historical + real-time = full index
- **Resumable**: Cursor persistence enables restart recovery
- **Gap healing**: Sequence gaps trigger automatic backfill
- **Order preserved**: BTreeMap maintains slot/sequence order

### Negative

- **Complexity**: Two systems running in parallel
- **Resource usage**: Backfill competes with real-time for DB writes
- **RPC dependency**: Backfill requires functional RPC endpoint

### Mitigations

- Backfill rate limiting prevents overwhelming the database
- Separate write paths can use different connection pools
- RPC failover to multiple endpoints

## Alternatives Considered

### 1. Sequential Only

Backfill first, then switch to real-time.

**Rejected because**: Can't start indexing until backfill complete (could be hours/days).

### 2. Real-time Only

Never backfill, start from "now".

**Rejected because**: No historical data, gaps from downtime never filled.

### 3. External Coordination

Use a coordinator service to manage merge.

**Rejected because**: Unnecessary complexity for this scale.

## Implementation Flow

```
Startup:
1. Load cursors from DB
2. Start real-time ingestion from cursor
3. Start backfill engine from cursor
4. Both feed into EventMerger

Runtime:
1. Real-time: gRPC → Parser → Merger → Persister
2. Backfill: RPC → Parser → Merger → Persister
3. Merger deduplicates by (slot, seq)
4. Cursors updated after successful persist

Gap Detection:
1. Sequence tracker detects gap
2. Gap range added to backfill queue (priority 1)
3. Backfill fetches missing events
4. Events merge via standard path
```

## References

- Change Data Capture patterns: https://debezium.io/documentation/reference/stable/connectors/index.html
- Solana RPC API: https://docs.solana.com/developing/clients/jsonrpc-api
