# Running slot-stream

## What it needs

- PostgreSQL 16, and a database it may create its schema in. Migrations are
  applied on startup; there is no separate migrate step and no sqlx-cli
  requirement.
- A stream source speaking the gRPC protocol in `proto/slot_stream.proto`. In
  production that is a Geyser-side source; `slot-stream-source` serves a
  described chain over the same protocol for demos and tests.
- Optionally a Solana JSON-RPC endpoint, for backfill.
- Rust 1.88 and `protoc` to build.

## Configuration

Settings come from a TOML file named by `CONFIG_FILE`, and the environment is
layered over whatever the file says. A container image ships a file and a
deployment overrides `DATABASE_URL`; it does not have to rewrite the file. Every
section has defaults, so a config file names only what it changes:

```toml
[database]
url = "postgres://slotstream:secret@db:5432/slot_stream"
max_connections = 16

[ingester]
channel_capacity = 10000
overflow_policy = "block"

[persister]
batch_size = 500
batch_timeout_ms = 100
```

Environment overrides: `DATABASE_URL`, `GRPC_ENDPOINT`, `RPC_ENDPOINT`,
`API_PORT`, `API_ENABLED`, `METRICS_PORT`, `METRICS_ENABLED`, `LOG_LEVEL`,
`LOG_FORMAT`. A malformed numeric override is an error at startup rather than a
silent fallback to the default.

### The settings that matter

`ingester.channel_capacity` bounds how far ingestion runs ahead of the database.
`ingester.overflow_policy` decides what happens at that bound: `block` applies
backpressure and never loses an event, which is what an index wants;
`drop_newest` and `drop_oldest` shed load. `processor.chain_tracker_max_slots`
bounds how deep a reorg can be resolved exactly — below the window the tracker
reports a slot as ignored rather than guessing. `persister.batch_size` and
`batch_timeout_ms` trade write throughput against how far behind a reader is.

## Deploying

Single writer per database. The commit cursor assumes one process is advancing
it; two would interleave and both would resume from the wrong place.

```
docker compose up --build                      # the whole stack
docker compose --profile monitoring up --build # with Prometheus and Grafana
```

Without containers:

```
export DATABASE_URL=postgres://user:pass@localhost:5432/slot_stream
export GRPC_ENDPOINT=http://source-host:10000
cargo run --release --bin slot-stream
```

The process handles SIGINT and SIGTERM: it stops reading the source, drains what
is in flight, commits the final batch with its cursor, and exits. Give it a few
seconds of termination grace. A `SIGKILL` is also safe — the cursor is never
ahead of the data — but loses whatever was in flight and costs a re-read on
startup.

## Checking it is working

```
curl localhost:8080/health/ready     # 200 means the database answers
curl localhost:8080/v1/status        # counts, chain head, uptime
curl localhost:8080/v1/chain/head
```

A healthy indexer has a chain head that keeps rising, `events.invalidated` that
grows occasionally rather than never (reorgs happen; never seeing one usually
means fork detection is not running), and a stable buffer utilisation.

## Metrics worth alerting on

| Metric | What it means |
|--------|---------------|
| `processor.reorgs`, `processor.rollback_depth` | A deep reorg is a cluster event, not routine |
| `processor.gap_joins` | The stream is losing slots; consider a backfill |
| `ingester.buffer_blocked` | Backpressure is engaging — the database is the bottleneck |
| `ingester.buffer_dropped` | Data is being lost. Should be zero under `block` |
| `ingester.sequence_gaps` | Candidates for backfill |
| `persister.write_errors` | The obvious one |

## When something is wrong

**The indexer is falling behind.** Check `ingester.buffer_blocked`. If it is
rising, the database is the constraint: raise `persister.batch_size`, give
PostgreSQL more connections, or check whether autovacuum is keeping up on
`events`. Raising `channel_capacity` buys time, not throughput.

**Rows are missing.** Query the DLQ:

```sql
SELECT slot, error_category, error_message, retry_count
FROM dead_letter_queue
WHERE NOT is_resolved
ORDER BY failed_at DESC
LIMIT 50;
```

Every gap has a row saying why. Replaying an entry pushes it back through the
ordinary path; if it fails again it is re-quarantined rather than lost, so a
retry loop is safe to run.

If a slot has no DLQ row either, the stream never delivered it. Queue a backfill
for that range; backfill skips slots that already hold valid events, so a
generous range is cheap.

**A reorg looks wrong.** The audit trail:

```sql
SELECT fork_slot, divergence_point, rollback_depth, events_invalidated,
       slots_rolled_back, divergence_is_bound, detected_at
FROM reorgs ORDER BY detected_at DESC LIMIT 20;
```

`divergence_is_bound` says whether the divergence point was an observed slot or
only a lower bound — the latter means the walk left the retained window, and the
rollback set is what could be proven rather than what may have happened.

Note that a divergence which changed nothing does not appear here; it is logged
as a gap join and counted under `processor.gap_joins`.

**A rollback was refused.** Two causes, both deliberate. A rooted slot in the
rollback set means our view and the cluster's have genuinely diverged, and no
automatic action is right. A rollback deeper than `processor.max_rollback_depth`
means the fork reaches further back than the window can explain. Both are logged
with the slot and the reason, and both want a human.

**The invalidated rows are piling up.** They are kept for audit. Once they are
old enough to be beyond dispute, `purge_invalid_below` removes rows below a
watermark. It is an operator action rather than something the pipeline does on
its own, precisely because the audit trail is the reason they are soft-deleted.

## Backups

`events` is reconstructible from the chain, given enough backfill. `cursors` is
not worth backing up — losing it costs a re-read. `dead_letter_queue` is the one
that matters: it holds payloads that failed and are not otherwise recorded
anywhere.
