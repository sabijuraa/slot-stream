# ADR 006: The PostgreSQL schema

Status: accepted. `migrations/001_initial_schema.sql` is the authority.

## Context

The schema has to support four things at once: idempotent writes keyed on a
stable identity, rollback and its inverse, resumption after a crash, and a read
path that never returns orphaned rows. Several of those pull in different
directions.

## Decision

Five tables.

### `events`

One row per indexed event, with `UNIQUE (slot, source_seq)` as the identity the
upsert keys on, and `is_valid` / `invalidated_at` implementing the soft-delete
rollback.

Both sequence numbers are stored. `seq` is the read order; `source_seq` is the
identity. ADR 002 covers why they are not the same column.

The payload is `JSONB`. Event shapes differ by kind and change with the cluster,
and a normalised schema would mean a migration every time upstream adds a field.
A GIN index with `jsonb_path_ops` serves the signature and account lookups.

Indexes on the read path are partial on `is_valid = true`. Every read filters on
it, so invalid rows in those indexes are dead weight — and after a deep reorg
there can be a lot of them.

Autovacuum is tuned aggressively on this table (`vacuum_scale_factor = 0.01`).
It is write-heavy, and a rollback updates whole slots at once, so dead tuples
accumulate faster than the defaults assume.

### `slots`

The chain structure: slot, parent, status, `is_canonical`. Its purpose is
startup. Without it the fork detector begins with an empty chain, treats the
first slot after the restart as the beginning of a fresh one, and silently misses
a reorg spanning the restart.

### `cursors`

The commit position, written in the same transaction as the batch it describes,
so it can never claim progress that was not made. The upsert uses `GREATEST` so
it cannot travel backwards.

### `reorgs`

An audit row per reorg *that changed state*: fork slot, divergence point,
expected and actual parent, depth, the slots rolled back, and whether the
divergence point was an observed slot or only a lower bound. This is what answers
"what happened at 03:14".

A divergence that rolls back nothing and restores nothing does not get a row. It
is a gap join, not a reorg, and filling this table with rows describing no change
is how an audit trail stops being read.

### `dead_letter_queue`

Quarantined events with enough context to replay them, including the parent slot.
ADR 004 covers why.

## Consequences

Invalidated rows accumulate. `purge_invalid_below` exists to remove rows below a
watermark once they are old enough to be beyond dispute; it is a deliberate
operator action rather than something the pipeline does on its own, because the
audit trail is the reason the rows are soft-deleted in the first place.

`JSONB` costs space against a normalised layout, and queries into the payload are
slower than a real column would be. Both are accepted in exchange for not
migrating the schema whenever an upstream payload changes.

## What was rejected

*A single `sequence` column.* See ADR 002.

*`CREATE INDEX CONCURRENTLY` in a migration.* It cannot run inside a transaction,
and sqlx runs migrations transactionally. The initial schema creates its indexes
normally; a later index on a live table is an operator task, not a migration.

*Hard deletes on rollback.* See ADR 007.

*Partitioning `events` by slot range.* Worth doing at volume, and premature
before there is a retention policy to partition along.
