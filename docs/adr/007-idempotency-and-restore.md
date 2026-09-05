# ADR 007: Idempotent writes, and why rollback needs an inverse

Status: accepted. Implemented in `crates/persister/src/writer.rs` and
`crates/persister/src/rollback.rs`.

## Context

Two things repeat in this system. Events are re-delivered — after a reconnect,
after a crash, when a source replays from a resume point — and slots change sides
when the cluster switches branches. The write path has to make both harmless.

## Decision

### The upsert says what it means

```sql
ON CONFLICT (slot, source_seq) DO UPDATE SET
    data           = EXCLUDED.data,
    parent_slot    = EXCLUDED.parent_slot,
    event_hash     = EXCLUDED.event_hash,
    is_valid       = true,
    invalidated_at = NULL,
    updated_at     = NOW()
```

`ON CONFLICT DO NOTHING` is idempotent in the narrow sense and wrong here. Once a
reorg has marked a row invalid, re-delivery of that same event — which is exactly
what happens when the cluster switches back — finds the row present and changes
nothing, leaving it invalid forever. The row exists, the event is real, and no
reader will ever see it.

Re-delivering an event is an assertion that it is real. The write has to record
that assertion, which means clearing the invalidation.

`RETURNING (xmax = 0)` distinguishes an insert from an update, which is how the
statistics tell new events from re-delivered ones.

### Rollback carries a restore set

A rollback plan holds two lists: slots to invalidate, and slots to restore. Both
are applied in one transaction, along with the `is_canonical` updates on `slots`
and the audit row.

The restore half exists because re-adoption cannot rely on re-delivery. When a
reorg adopts a branch an earlier reorg orphaned, the tracker makes those slots
canonical again — but the stream has long since moved past them and nothing is
going to send those events a second time. Without an explicit restore, the slots
are canonical in the chain and invisible in the database.

This is not hypothetical. It was found by `repeated_reorgs_leave_state_correct`,
which compared persisted state against a canonical replay after several forks and
found rows missing that the replay said should be there.

It is also the reason rollback is a soft delete: `UPDATE ... SET is_valid = false`
has an inverse and `DELETE` does not.

## Consequences

The upsert is a write even when nothing changed, so a full replay of a completed
run touches every row. It is idempotent in effect, not in cost.
`replaying_a_completed_run_changes_nothing` asserts the effect.

`seq` is absent from the conflict update on purpose. It is the order readers page
through, and moving a redelivered row to a new position would slide it past a
cursor that had already gone by. The row keeps its place; only its content and
its validity are refreshed.

Invalid rows accumulate until an operator purges them. See ADR 006.

## What was rejected

*Hard delete on rollback.* Simpler, cheaper, and loses data permanently the first
time a cluster switches back to a branch it abandoned.

*Re-requesting orphaned slots from RPC on re-adoption.* Correct in principle, but
turns a local `UPDATE` into a network round trip per slot at exactly the moment
the pipeline is already behind — and the data is still sitting in the table.

*Comparing `event_hash` before writing.* An extra read on the hot path to avoid a
write that is already cheap and already correct.
