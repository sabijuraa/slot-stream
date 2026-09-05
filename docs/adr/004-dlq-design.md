# ADR 004: The dead-letter queue

Status: accepted. Implemented in `crates/dlq`.

## Context

Events fail. A payload is malformed, a schema assumption is wrong, a write fails
because the database went away for four seconds. Dropping them loses data
silently, and stopping the pipeline on one bad event means one bad event can halt
indexing.

## Decision

A failed event is written to `dead_letter_queue` in PostgreSQL, with its payload,
sequence, slot, parent slot, kind, error message, and a category. Processing
continues.

The categories are the ones that imply different responses: `malformed`,
`validation`, `persistence`, `processing`, `unknown`. They are produced by one
function in `common` used by every path, after an early bug where one code path
wrote `Malformed` and another queried for `malformed`, so entries were silently
invisible to the listing they should have appeared in.

The queue is in PostgreSQL rather than in memory because its entire purpose is to
outlive the process that created it.

## Replay

Replay reconstructs the stream event from the stored payload and pushes it into
the ingester. It travels the ordinary path — parse, fork detection, idempotent
write — rather than a private one that could drift from it.

Both outcomes are correct:

- The failure was transient. The event indexes normally and the entry resolves.
- The payload is still bad. It fails again, the original entry is marked resolved
  and a new entry records the new failure. Nothing is written, nothing is lost,
  and the retry count grows so an operator can see what is not converging.

That second case is why replay is safe to run on a schedule. It is covered by
`malformed_events_are_quarantined_and_stay_quarantined_on_replay`.

The parent slot is stored specifically so the replayed event is a faithful
reconstruction. Without it the event returns parentless, the chain tracker has
nothing to check, and the slot quietly stops being part of the chain the indexer
knows about.

Entries carry a retry limit and become `Exhausted` when they hit it, which is the
signal that a human is needed.

## Consequences

Gaps in the index are visible and explicable rather than mysterious: every
missing event has a row saying why.

A systematic failure — a schema change upstream, say — fills the queue instead of
the index. The queue depth is a metric for exactly that reason.

## What was rejected

*A file-based queue.* Adds a second durability story and a second thing to back
up, for a table that is small.

*Retrying inline with backoff.* Blocks the pipeline on the failing event, which
is the outcome the DLQ exists to avoid.

*Dropping malformed events with a log line.* Logs are sampled, rotated, and not
queryable. "How many events did we lose at 03:14, and which" has to be answerable.
