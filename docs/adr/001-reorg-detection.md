# ADR 001: Chain Reorg Detection Strategy

## Status

Accepted

## Context

Solana is a high-throughput blockchain that occasionally experiences chain reorganizations (reorgs). A reorg occurs when the validator network switches from one fork of the chain to another, invalidating previously "confirmed" blocks.

For an indexing pipeline, reorgs present a critical challenge:
- Events from invalidated blocks must not remain in the index as valid data
- Queries against the index must not return data from orphaned forks
- The pipeline must self-heal without manual intervention

Solana's reorg characteristics:
- Slots are produced every ~400ms
- Reorgs typically affect 1-5 slots, rarely more
- "Confirmed" status is not final; only "rooted" slots are irreversible
- Parent slot references create a linked chain structure

## Decision

We will implement slot-parent chain tracking with soft-delete rollback:

### Detection Algorithm

1. **Maintain slot->parent mapping in memory**
   - Store recent slots (configurable window, default 10,000)
   - Each slot records: slot number, parent slot, status, block hash

2. **On receiving a new slot S with parent P**:
   ```
   if S exists in our map:
       if map[S].parent != P:
           FORK DETECTED
       else:
           update status (processing->confirmed->rooted)
   else:
       insert new slot record
   ```

3. **On fork detection**:
   - Compute divergence point (common ancestor)
   - Identify all slots on the orphaned branch
   - Generate rollback plan

### Rollback Strategy

We use **soft delete** rather than hard delete:

```sql
UPDATE events SET is_valid = false, invalidated_at = NOW()
WHERE slot IN (orphaned_slots)
```

Rationale:
- Preserves data for debugging/forensics
- Avoids expensive DELETE operations
- Allows potential re-validation if fork resolution was incorrect
- Queries filter by `WHERE is_valid = true`

### State Machine

```
              ┌──────────────────────────────────────┐
              │                                      │
              ▼                                      │
          ┌───────┐    fork    ┌──────────┐         │
          │NORMAL │───detect──▶│ PLANNING │         │
          └───────┘            └────┬─────┘         │
              ▲                     │               │
              │                     ▼               │
              │              ┌─────────────┐        │
              │              │ROLLING_BACK │        │
              │              └──────┬──────┘        │
              │                     │               │
              │                     ▼               │
              │              ┌─────────────┐        │
              └──────────────│REPROCESSING │────────┘
                             └─────────────┘
```

## Consequences

### Positive

- **Self-healing**: Pipeline automatically recovers from reorgs
- **Audit trail**: Invalidated events preserved for analysis
- **Query safety**: `is_valid` filter prevents stale data exposure
- **Memory bounded**: Slot window prevents unbounded memory growth

### Negative

- **Storage overhead**: Invalidated events consume space until purged
- **Query overhead**: Every query must include `is_valid = true` filter
- **Complexity**: State machine adds operational complexity

### Mitigations

- Periodic purge of old invalidated events (`slot < rooted_slot - retention`)
- Partial index on `is_valid = true` for query performance
- Comprehensive metrics and alerting on reorg frequency

## Alternatives Considered

### 1. Wait for Rooted Status

Wait for slots to be finalized before processing.

**Rejected because**: Adds 30+ seconds of latency, defeating real-time indexing purpose.

### 2. Hard Delete on Rollback

Delete invalidated events immediately.

**Rejected because**: Loses forensic data, and DELETEs are expensive in Postgres.

### 3. Separate Tables per Fork

Maintain parallel tables for each potential fork.

**Rejected because**: Extreme complexity, most forks are short-lived.

## References

- Solana confirmation levels: https://docs.solana.com/cluster/commitments
- PostgreSQL partial indexes: https://www.postgresql.org/docs/current/indexes-partial.html
