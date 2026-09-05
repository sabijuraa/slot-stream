# ADR 001: How a fork is detected

Status: accepted. Implemented in `crates/common/src/slot.rs`.

## Context

Solana slots reference a parent. Most of the time the parent is the slot just
processed. Occasionally the cluster switches to a branch that diverged several
slots back, and everything indexed above the divergence point describes a chain
that no longer exists.

The original implementation here compared each slot's parent against `slot - 1`
and reported a fork when they differed. That catches a skipped slot, which is not
a fork, and misses the ordinary reorg entirely — a branch of two slots off a
shared parent has consecutive slot numbers, so `parent == slot - 1` holds
throughout while the chain has plainly changed.

## Decision

Keep a slot to parent map for a bounded window, plus the set of canonical slots
and the head. For each incoming slot:

- parent equals the head: the chain grew.
- the slot is known and still canonical with the same parent: a retransmit.
- otherwise: walk back from the parent through the map until the walk reaches a
  canonical slot. That is the common ancestor. Everything canonical above it is
  orphaned; the slots walked through are the branch being adopted.

Three details are load-bearing.

**The head is derived, not accumulated.** It is the maximum of the canonical set,
recomputed after every change. A reorg can move the head *down* — branch A
reaches 111, the cluster switches to a branch whose tip is 107 — and a head
tracked as a running maximum stays at 111 and turns every subsequent slot into a
phantom fork.

**The duplicate check is gated on still being canonical.** Re-delivery of a slot
we had orphaned is the cluster switching back to that branch, which is a reorg. If
it were treated as a duplicate, the abandoned branch would stay canonical and the
returning one would be invisible.

**The walk is bounded by the size of the map**, so a corrupt parent cycle
terminates instead of spinning.

## When the walk finds nothing

If the walk leaves the window without reaching a canonical slot, the parent names
a slot we never received. We have no evidence that anything was orphaned. Nothing
is rolled back; the branch is adopted alongside what is there, and the event is
counted as a gap join rather than a reorg.

This is a deliberate asymmetry. Reorgs are rare and gaps are not, so a rule that
destroys data whenever a connection blips is worse than one that occasionally
retains a slot it cannot prove is orphaned.

## Consequences

Reorg depth is bounded by the window, 4096 slots by default. Below it the tracker
reports a slot as ignored rather than guessing. The window is also the memory
bound: a slot entry is small, and 4096 of them is trivial.

A rollback that would orphan a rooted slot is refused with an error rather than
executed. The cluster does not revert rooted slots, so if one is in the rollback
set the two views have genuinely diverged and no automatic action is right.

## What was rejected

*Block-hash comparison.* More precise, but requires a hash for every slot, and
the stream does not always supply one. Parent links are always present.

*Waiting for finality before indexing.* Correct by construction and useless: the
point of a live index is that it is live.

*Deleting orphaned rows.* Loses the audit trail, and — decisively — cannot be
undone when the cluster switches back to a branch it abandoned. See ADR 007.
