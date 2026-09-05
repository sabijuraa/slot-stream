# ADR 003: Bounded channels, and what happens when they fill

Status: accepted. Implemented in `crates/ingester/src/buffer.rs`.

## Context

A Solana stream can produce tens of thousands of events a second, in bursts. The
database cannot always keep up. Something has to give, and the choice of what is
the difference between an indexer that degrades and one that dies.

An unbounded queue does not solve this; it converts a throughput problem into an
out-of-memory kill, and does so at the worst possible moment.

## Decision

Two bounded `tokio::sync::mpsc` channels — source to processor, processor to
persister — and nothing buffered outside them. Backpressure is then the natural
consequence of the chain: a slow database fills the persist channel, which stalls
the processor, which fills the raw channel, which stalls the ingester, which stops
reading the source.

The ingester's buffer takes a policy:

- **`block`** waits for room. For a gRPC stream, not reading closes the
  flow-control window and slows the server down. Nothing is lost, memory stays
  flat. This is the default, because for an index a gap is worse than a delay.
- **`drop_newest`** rejects the incoming event and records the drop.
- **`drop_oldest`** records a drop under its own label and then behaves as
  `drop_newest`, because a bounded channel offers no way to evict from the far
  end. Kept as a separate policy because the operator's intent differs even where
  the mechanism cannot, and the metric label is what an operator reads.

## Measuring it

Under the blocking policy the drop count is zero by construction, so it answers
nothing during an incident. The buffer therefore also counts how often a push
actually had to wait, and reports its true depth from the channel's own capacity
rather than from an inference.

`crates/pipeline/tests/backpressure.rs` asserts on that counter, so the test
cannot pass vacuously: if the consumer was not actually slower, nothing waited,
and the test fails rather than reporting a bound that was never tested.

## Shutdown

The processor's loop ends when the raw channel closes, which happens when the
last sender is dropped. Relying on `Drop` for that makes clean shutdown depend on
nobody else holding a handle to the ingester, and something reasonable eventually
does — a metrics sampler, an operator endpoint. The failure mode is not an error;
it is a hang.

So the sending half is held in an `Option` and released explicitly by
`shutdown()`. A push already in flight holds its own clone and completes
normally, after which the channel closes.

## Consequences

Peak memory is `channel_capacity × average event size`, plus the persister's
batch. With the defaults that is a few tens of megabytes.

Sustained overload under the blocking policy means falling behind the chain
rather than losing data. That is the intended trade; recovering from lag is a
capacity problem, recovering from a gap requires a backfill.

## What was rejected

*An unbounded channel with a memory watchdog.* The watchdog fires after the
allocation, which is too late, and the failure is abrupt rather than gradual.

*Spilling to disk.* Converts a memory bound into a disk bound and adds a
durability question the pipeline does not otherwise have.

*One channel end to end.* The persister batches; the processor does not. Separate
channels let each stage have the capacity that suits it.
