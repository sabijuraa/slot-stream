# slot-stream: system design

This describes how the pipeline works and why it is built this way. The parts
that matter are the reorg model, the ordering that makes it safe, the idempotency
that makes replays harmless, and the crash-recovery model that ties the three
together. Everything else is ordinary.

## Shape

```
stream source ──▶ ingester ──▶ processor ──▶ persister ──▶ PostgreSQL
                  (bounded)    (ordering,     (batched,
                               fork           idempotent,
                               detection)     rollbacks)
                                   │
                                   └──▶ DLQ

Solana JSON-RPC ──▶ backfill ──────┘   (re-enters through the ingester)

PostgreSQL ──▶ read API
```

Two bounded channels connect the stages, and that is the whole flow-control
story: a slow database fills the persist channel, which stalls the processor,
which fills the raw channel, which stalls the ingester, which stops reading the
source. Nothing is buffered outside those two channels, so memory is capped by
their capacities regardless of how far behind the database falls.

The stages are separate crates, but they only become a pipeline in
`crates/pipeline`, which is a library rather than code in the binary. The
integration tests build the pipeline through that same function. This is
deliberate: a test that assembles its own wiring proves things about an
arrangement that never ships.

## The reorg model

### What a reorg looks like from here

Every slot names its parent. The pipeline keeps a slot to parent map for a
bounded window — 4096 slots by default, about half an hour of Solana — plus the
set of slots it currently believes are canonical, and the head.

For each incoming slot with parent `P`:

1. If `P` is the current head, the chain grew. Insert and move on. This is
   almost every slot.
2. If the slot is already known and still canonical with the same parent, it is a
   retransmit. Update its status, change nothing else.
3. Otherwise, walk back from `P` through the parent map until the walk lands on a
   slot that is canonical. That slot is the common ancestor.

Case 3 is the fork. Everything canonical above the common ancestor is orphaned;
the slots walked through on the way are the branch being adopted, and they become
canonical along with the incoming slot.

The walk is bounded by the size of the map, so a corrupt parent cycle terminates
rather than spinning.

### The case that is not a fork

The walk can leave the window without ever reaching a canonical slot. That
happens when the stream had a gap: the parent names a slot we never received. We
then have no evidence that anything was orphaned — the branch we hold and the
branch being announced may well be the same chain with a hole in the middle.

Rolling back on that evidence would delete correct data every time a connection
blipped. So nothing is rolled back, the incoming branch is adopted alongside what
is already there, and the event is logged and counted as a *gap join* rather than
a reorg. Keeping it out of the reorg count matters: an operator watching reorg
depth is watching for a real signal, and filling that metric with events in which
no state changed is how a signal gets ignored.

`crates/pipeline/tests/reorg_proof.rs::a_branch_reaching_into_a_gap_keeps_what_it_cannot_disprove`
pins this behaviour.

### The head can move backwards

A reorg does not necessarily advance the tip. Branch A may have reached slot 111
when the cluster switches to a branch whose tip is 107. If the head is tracked as
a running maximum it stays at 111, every subsequent slot looks like a fork
against a head that no longer exists, and the indexer produces phantom reorgs
forever.

So the head is derived, not accumulated: it is the largest slot in the canonical
set, recomputed after every change. This was a real bug, found by the depth-32
proof, and `a_reorg_can_move_the_head_backwards` in `crates/common/src/slot.rs`
exists to keep it fixed.

### Rollback is a soft delete, and it has an inverse

Orphaned rows are marked `is_valid = false` with an `invalidated_at` timestamp
rather than deleted. Two reasons: the fork stays auditable, and — more
importantly — the operation is reversible.

Reversibility is not optional. Clusters switch back to branches they abandoned.
When a later reorg re-adopts slots an earlier one orphaned, nothing is going to
re-deliver those events; the stream has moved on. If rollback were a delete, that
data would simply be gone. So a rollback plan carries two sets: the slots to
invalidate, and the slots to restore, and both are applied in one transaction.

This too was found by a test rather than by reasoning:
`repeated_reorgs_leave_state_correct` failed because re-adopted slots became
canonical in the tracker while their rows stayed invalid in the database.

### Rooted slots do not revert

If a rollback set contains a slot the cluster has rooted, our view and the
cluster's have genuinely diverged, and no automatic action is correct. The
tracker refuses with `RollbackFailed` rather than proceeding. Likewise, a
rollback deeper than `max_rollback_depth` (512 by default) is refused: a reorg of
hundreds of slots is a symptom, and guessing would corrupt state a human could
still repair.

## Ordering

### One channel, one order

For a fork the processor emits, in this order: the events of the branch that is
about to be abandoned, then the rollback that invalidates them, then the events
of the branch that replaced it. The persister applies commands in the order it
receives them and flushes any pending batch before executing a rollback, so the
database passes through the same sequence of states the processor saw.

This only works because all three kinds of command travel one channel. Putting
rollbacks on a separate channel — which looks tidier — destroys it: two channels
have no order relative to each other, and a rollback that overtakes the events
that replaced the orphaned branch invalidates the wrong rows.

### Two sequence numbers, doing different jobs

Each event carries two, and conflating them breaks something:

- `source_seq` is the stream's own number. It is stable for a given event across
  redelivery and restarts, so it is what row identity keys on.
- `seq` is assigned by the processor as it commits, resumed from the database
  high-water mark on startup. It defines the order readers see.

Keying identity on `seq` would mean a replayed event got a new identity and
inserted a duplicate row. Ordering by `source_seq` would mean readers saw the
validator's arrival order, which is not monotonic across reconnects or forks.

Backfilled and replayed events carry sequence numbers from outside the live
stream — backfill derives its own from `slot << 20 | index` — so they are exempt
from the ingester's sequence tracking. Judging them against the live sequence
would discard a replay as a rewind and report a backfill as a gap of millions.

## Idempotency

Row identity is `(slot, source_seq)`, and the write is:

```sql
INSERT INTO events (...) VALUES (...)
ON CONFLICT (slot, source_seq) DO UPDATE SET
    data           = EXCLUDED.data,
    parent_slot    = EXCLUDED.parent_slot,
    event_hash     = EXCLUDED.event_hash,
    is_valid       = true,
    invalidated_at = NULL,
    updated_at     = NOW()
```

The two lines that matter are `is_valid = true` and `invalidated_at = NULL`. A
plain `DO NOTHING` would be idempotent in the narrow sense and wrong in practice:
after a reorg invalidated a row, re-delivery of that same event — which happens
whenever the cluster switches back — would leave it invalid forever, because the
row exists and nothing updates it. Re-delivering an event is an assertion that it
is real, and the write has to say so.

`RETURNING (xmax = 0)` distinguishes an insert from an update, which is how the
statistics separate new events from re-delivered ones.

## Crash recovery

The commit cursor is written in the same transaction as the batch it describes.
There is no window in which the batch is committed and the cursor is not, so the
cursor never claims progress that was not made. It records the slot, the assigned
`seq`, and the `source_seq` — the last being what the source is asked to resume
from.

On startup the pipeline reads three things:

- the cursor, giving the stream position to resume from;
- `MAX(seq)`, so newly assigned sequences stay above everything committed;
- the canonical slot/parent pairs, so the fork detector is rebuilt.

The third is the one that is easy to miss. A processor that starts with an empty
chain treats the next slot as the beginning of a fresh chain and silently fails to
detect a reorg that spans the restart. `a_crash_during_a_reorg_still_recovers_to_the_canonical_chain`
covers exactly that case.

Because writes are idempotent, resuming slightly behind is harmless: the overlap
refreshes rows that already exist, without moving them in the read order —
`seq` is deliberately not part of the conflict update, so a redelivered row keeps
the position a reader may already have paged past. The cursor upsert uses `GREATEST` so it can
never travel backwards.

The property is proven twice. `crates/pipeline/tests/recovery.rs` abandons a
pipeline mid-stream inside the test process, and
`scripts/crash_recovery_proof.sh` does it at the process level — the shipped
binaries, a real gRPC socket, and a real `SIGKILL`, so there is no unwinding and
no chance to flush.

## Backpressure

The ingester's buffer is a bounded channel with a configurable policy:

- `block` waits for room. The source stops being read, which for a gRPC stream
  closes the flow-control window and slows the server down. Nothing is lost. For
  an indexer, where a gap is worse than a delay, this is the only sensible
  default.
- `drop_newest` and `drop_oldest` shed load instead. `drop_oldest` cannot be
  honoured precisely on a bounded channel — there is no way to evict from the far
  end — so it behaves as `drop_newest` while recording the drop under its own
  label, because the operator's intent differs even when the mechanism cannot.

Either way memory is bounded, because nothing is held outside the channel.

The buffer reports how often a push actually had to wait. Under the blocking
policy the drop count stays at zero by construction, so it answers nothing during
an incident; "how often did we block" is the number that does.

### Shutdown is explicit, not a side effect of dropping

The processor's loop ends when the raw channel closes, which happens when the
last sender is dropped. Leaving that to `Drop` makes clean shutdown depend on
nobody else holding a handle to the ingester — and something reasonable always
does eventually, a metrics sampler or an operator endpoint. So the ingester
releases its sender explicitly on `shutdown()`. A push already in flight holds
its own clone and finishes normally.

## Backfill

Backfill fetches historical slots over Solana JSON-RPC and feeds them into the
same ingester the live stream uses, so they are parsed and written by the same
code. It skips slots that already hold valid events, which is what makes a
repeated backfill a no-op.

Backfilled events are marked with their origin, and the processor does not run
them through fork detection. This is not an optimisation. A backfilled slot is
history: it was fetched because it is already on the confirmed chain, and it
arrives with the head far past it. Fork detection would read "slot 6 builds on
slot 5" as a branch diverging at slot 5 and orphan everything above — the exact
opposite of filling a gap. The slot is recorded, the chain is left alone.

## Dead-letter queue

An event that cannot be parsed, or whose write fails, goes to the DLQ with its
payload, its slot, its parent slot, and a category. Nothing is dropped silently.

Replay reconstructs the stream event and pushes it into the ingester, so a
replayed event travels the ordinary path rather than a private one that could
drift from it. Two outcomes are both correct:

- The failure was transient — a database blip — and the event indexes normally.
- The payload is still malformed, so it fails again and is quarantined again,
  with the original entry marked resolved and a fresh entry recording the new
  failure. Nothing is written and nothing is lost, which is what makes automatic
  retry safe to run.

The parent slot is retained precisely so the replayed event is a faithful
reconstruction. Without it the event comes back parentless, the chain tracker has
nothing to check, and the slot quietly stops being part of the chain the indexer
knows.

## Schema

Five tables. `migrations/001_initial_schema.sql` is the authority; this is the
reasoning.

**`events`** — one row per indexed event. `UNIQUE (slot, source_seq)` is the
identity the idempotent write keys on. `is_valid` and `invalidated_at` implement
the soft-delete rollback. Indexes are partial on `is_valid = true`, because every
read path filters on it and the invalid rows are dead weight in an index that
only serves readers. A GIN index over the JSONB payload serves the signature and
account lookups. Autovacuum is tuned aggressively on this table: it is
write-heavy, and reorg churn creates dead tuples quickly.

**`slots`** — the chain structure, with `is_canonical`. Its purpose is startup:
without it the fork detector begins blind.

**`cursors`** — the commit position, written in the batch's transaction.

**`reorgs`** — an audit row per reorg that changed state, with the divergence
point, the depth, the slots rolled back, and whether the divergence point was an
observed slot or only a lower bound. This is what answers "what happened at
03:14".

**`dead_letter_queue`** — quarantined events with enough context to replay them.

## Observability

Prometheus metrics on the same port as the read API. The ones worth alerting on:

| Metric | Why |
|--------|-----|
| `processor.reorgs`, `processor.rollback_depth` | A deep reorg is a cluster event, not routine |
| `processor.gap_joins` | The stream is losing slots |
| `ingester.buffer_blocked` | Backpressure is engaging; the database is the bottleneck |
| `ingester.buffer_dropped` | Data is being lost, if a shedding policy is configured |
| `ingester.sequence_gaps` | Candidates for backfill |
| `persister.write_errors` | The obvious one |

`/health/live` reports the process is running; `/health/ready` reports the
database answers. They are separate because a container orchestrator should
restart on the first and stop routing traffic on the second.

## What is not here

Aggregation and analytics belong downstream. There is no archive beyond the
configured window: the chain tracker's window bounds how deep a reorg can be
resolved exactly, and slots below it are reported as ignored rather than guessed
at. Multi-writer deployment is out of scope — the commit cursor assumes one
writer per database.
