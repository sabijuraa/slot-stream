# ADR 002: Two sequence numbers

Status: accepted. Implemented in `crates/common/src/sequence.rs` and
`crates/common/src/event.rs`.

## Context

Readers need a stable total order over indexed events. The stream supplies a
sequence number, but it describes the validator's view: it is not monotonic
across reconnects, it rewinds when a source replays from a resume point, and it
says nothing about which branch an event was on.

Writes also need an identity, so that re-delivering an event updates a row rather
than inserting a second one. It is tempting to use the same number for both.

## Decision

Carry two, doing different jobs.

`source_seq` is the stream's own number. It is stable for a given event across
redelivery and restarts, so row identity is `(slot, source_seq)`.

`seq` is assigned by the processor as it commits, from an atomic counter resumed
from `MAX(seq)` in the database on startup. It defines the order readers see.

## Why not one

Keying identity on the assigned `seq` means a replayed event gets a fresh
identity and inserts a duplicate row — the exact failure the identity exists to
prevent.

Ordering by `source_seq` means readers see arrival order, which is not monotonic.
After a reconnect the source replays from the resume point and the numbers go
backwards; a reader paging by sequence would see events it had already passed.

## Out-of-band events

Backfilled and replayed events carry sequences from outside the live stream.
Backfill derives its own as `slot << 20 | index`, which keeps each slot's range
disjoint from its neighbours' and ordered by slot, but bears no relation to the
live numbering.

These are exempt from the ingester's sequence tracking. Judging a replay against
the live sequence discards it as a rewind; judging a backfill against it reports
a gap of millions and queues a backfill for the gap it just filled. The event's
origin says which rules apply.

## Consequences

A sequence regression from the live stream is logged and the event forwarded
anyway, because downstream writes are idempotent and a rewind after a reconnect
is normal.

The assigner is an `AtomicU64`, so ordering does not serialise the processor.

Replaying an event does *not* move it in the read order. `seq` is set on insert
and deliberately left out of the conflict update, so a row keeps the position it
was first given. The alternative — reassigning it — would slide a row forward
past a reader that had already paged beyond its old position, and that reader
would never see it. The content and validity are refreshed; the place is not.
