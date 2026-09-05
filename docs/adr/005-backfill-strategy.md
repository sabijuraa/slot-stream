# ADR 005: Backfill, and merging it with live data

Status: accepted. Implemented in `crates/backfill`.

## Context

The index needs to be filled from two directions. Historical slots bootstrap it
or repair a gap left by a disconnect; the live stream keeps it current. They
arrive out of order relative to each other and can overlap, and the result has to
be one coherent index rather than two interleaved ones with duplicates at the
seam.

## Decision

Backfill fetches blocks over Solana JSON-RPC — `getBlocks`, then `getBlock` per
slot — and feeds the resulting events into the same ingester the live stream
uses. They are parsed and written by the same code. There is no separate write
path, because a separate write path is a second place for the invariants to be
wrong.

Deduplication is by slot, not by event: before fetching a slot, the backfiller
asks whether that slot already holds valid events, and skips it if so. Slots are
the unit the RPC works in and the unit a gap is measured in, and it avoids
fetching a block only to discard every event in it.

Sequence numbers are derived as `slot << 20 | index`. Each slot's range is
disjoint from its neighbours', ordered by slot, and unmistakably not a live
stream number.

Gap fills are prioritised ahead of bootstrap ranges in the queue: a gap in
otherwise current data is a correctness problem, while a bootstrap range is
merely unfinished work.

## Backfill is not chain news

A backfilled event is marked with its origin, and the processor does not run it
through fork detection.

This is a correctness requirement, not an optimisation. A backfilled slot was
fetched *because* it is on the confirmed chain, and it arrives with the head far
past it. Fork detection would read "slot 6 builds on slot 5" as a branch
diverging at slot 5 and orphan everything above it — the exact opposite of
filling a gap. The slot is recorded; the chain is left alone.

The same marking exempts it from the ingester's sequence tracking, which would
otherwise see the stride-based number as a gap of millions.

`a_backfilled_gap_merges_without_disturbing_the_live_chain` drives this against a
real JSON-RPC endpoint over a real socket, and asserts that no reorg is recorded
and nothing is invalidated.

## Consequences

Running a backfill twice is a no-op, because every slot in range is already
present. The test asserts this directly.

Backfill is bounded by the RPC provider's rate limits, and `batch_delay` exists
to stay inside them. The retry schedule treats 429 and 5xx as retryable and
everything else as fatal for that call; a skipped or pruned slot (`-32007`,
`-32009`) is a normal answer rather than a failure.

Backfilled events carry the RPC block's transaction body, which is shaped
differently from a live stream payload. Readers see both, which is honest — they
came from different places.

## What was rejected

*Writing backfilled rows directly to PostgreSQL.* Faster, and a second write path
whose idempotency and ordering would have to be maintained in parallel with the
real one.

*Per-event deduplication.* Requires fetching the block first, and events within a
backfilled block have no stable stream identity to compare against.

*Backfilling through fork detection with special cases inside the tracker.* Moves
the distinction into the component that should not have to know where its input
came from. The origin belongs on the event.
