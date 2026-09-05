# slot-stream

A Solana indexing pipeline whose defining property is that it survives chain
reorganisations. When the cluster abandons a branch, the data already written for
that branch is invalidated and the replacement branch is indexed in its place —
so what a reader sees always matches what a from-scratch replay of the canonical
chain would have produced.

That property is not asserted here. It is proven by tests that drive real chain
shapes, forks included, through the wired pipeline into a real PostgreSQL
database and compare the result against an independently computed replay. See
[VERIFICATION.md](VERIFICATION.md) for the evidence.

## The problem

A Solana slot names its parent. Most of the time the parent is the slot you just
processed and the chain simply grows. Occasionally it is not: the cluster
switches to a branch that diverged some slots back, and everything you indexed
above the divergence point describes a chain that no longer exists.

An indexer that ignores this accumulates rows nobody can distinguish from real
ones. An indexer that deletes on any surprise loses data the first time a stream
has a gap. The interesting work is in telling those two situations apart.

## How it handles a fork

The processor keeps a slot to parent map for a bounded window of recent slots.
For each new slot it asks one question: does its parent lie on the chain we
believe in?

- The parent is the current head. The chain grew; nothing else to do.
- The parent is somewhere else. Walk back from that parent until the walk
  reaches a slot that is on our canonical chain. That slot is the common
  ancestor. Everything canonical above it is orphaned, and everything walked
  through on the way is the branch being adopted.
- The walk leaves the window without finding an ancestor. We cannot prove
  anything was orphaned, so nothing is rolled back. This is what a gap in the
  stream looks like, and destroying data on that evidence would be worse than
  keeping it.

A rollback is a soft delete. Orphaned rows are marked `is_valid = false` rather
than removed, so the fork stays auditable, and every read path filters on that
column. If the cluster later switches back to a branch it had abandoned — which
happens — the rows are restored by the inverse update rather than re-fetched,
because nothing is going to re-deliver those events.

The rollback and the writes travel the same channel, in the order the processor
decided on. Two channels would have no order relative to each other, and a
rollback arriving after the events that replaced the orphaned branch would
invalidate the wrong rows.

## Layout

| Crate | What it does |
|-------|--------------|
| `common` | Types, config, errors, the chain tracker, sequence assignment |
| `ingester` | Stream sources and the bounded buffer where backpressure lives |
| `processor` | Ordering, fork detection, rollback planning |
| `persister` | Idempotent writes, rollback execution, the commit cursor |
| `backfill` | RPC-driven historical fetch, merged with live data |
| `dlq` | Dead-letter storage, inspection, replay |
| `api` | The read API over indexed data |
| `pipeline` | The composition root: where the above become a running system |
| `cli` | The `slot-stream` binary and a scriptable `slot-stream-source` |

`pipeline` is a library rather than code inside the binary so the integration
tests assemble the pipeline the same way the binary does. A test that wired its
own would be testing an arrangement nothing ships.

## Running it

The compose stack brings up PostgreSQL, a chain source that serves a scripted
chain containing a reorg, and the indexer:

```
docker compose up --build
```

Then:

```
curl localhost:8080/v1/status
curl 'localhost:8080/v1/events?limit=10'
curl localhost:8080/v1/chain/head
```

Add `--profile monitoring` for Prometheus and Grafana.

Against a real stream, point `GRPC_ENDPOINT` at a Geyser gRPC source instead.

### Without Docker

You need PostgreSQL 16 and a database the indexer may create its schema in.
Migrations are applied on startup, so there is no separate migrate step.

```
export DATABASE_URL=postgres://user:pass@localhost:5432/slot_stream
export GRPC_ENDPOINT=http://localhost:10000

cargo run --release --bin slot-stream-source &   # or a real Geyser source
cargo run --release --bin slot-stream
```

## Configuration

Settings come from a TOML file named by `CONFIG_FILE`, from the environment, or
from both — the environment is layered over the file, so a deployment overrides
`DATABASE_URL` without rewriting the file it ships with. Every section has
defaults, so a config file names only what it changes.

| Variable | Meaning |
|----------|---------|
| `DATABASE_URL` | PostgreSQL connection string |
| `GRPC_ENDPOINT` | The stream source |
| `RPC_ENDPOINT` | Solana JSON-RPC, used by backfill |
| `API_PORT`, `API_ENABLED` | The read API |
| `METRICS_PORT`, `METRICS_ENABLED` | Prometheus exposition |
| `LOG_LEVEL`, `LOG_FORMAT` | `info`, and `json` or `pretty` |

The settings worth understanding are in `[ingester]`: `channel_capacity` bounds
how far ingestion may run ahead of the database, and `overflow_policy` decides
what happens when it hits that bound. `block` applies real backpressure and never
loses an event, which is what an indexer wants. `drop_newest` and `drop_oldest`
shed load instead; they are there because some deployments genuinely prefer a
fresh partial view to a complete late one, but for a chain index they are the
wrong answer.

## The read API

Every query filters on `is_valid = true`, which is what makes the API
reorg-aware without any caller knowing forks exist.

| Endpoint | Returns |
|----------|---------|
| `GET /health/live` | Liveness |
| `GET /health/ready` | Readiness, meaning the database answers |
| `GET /metrics` | Prometheus exposition |
| `GET /v1/status` | Counts, chain head, uptime |
| `GET /v1/events` | Events, filtered by slot or sequence range and kind |
| `GET /v1/events/slot/:slot` | Events in one slot |
| `GET /v1/events/signature/:signature` | Events carrying a signature |
| `GET /v1/events/account/:account` | Events touching an account |
| `GET /v1/slots/:slot` | A slot's chain record, canonical or not |
| `GET /v1/chain/head` | The canonical tip |

## Tests

The integration tests need PostgreSQL. They create and drop a database per test,
so point `TEST_DATABASE_URL` at one whose role may `CREATEDB`:

```
export TEST_DATABASE_URL=postgres://slotstream:slotstream@127.0.0.1:5432/slot_stream_test
cargo test --workspace
```

The suites worth knowing about:

- `crates/pipeline/tests/reorg_proof.rs` — reorgs at depths 1, 5 and 32,
  competing forks, repeated reorgs, re-adoption of an abandoned branch. Each
  asserts persisted state equals an independently computed canonical replay.
- `crates/pipeline/tests/recovery.rs` — a pipeline abandoned mid-stream, then
  restarted, including a crash landing in the middle of a reorg.
- `crates/pipeline/tests/backpressure.rs` — a deliberately slow persister, with
  assertions that nothing was dropped and the queue never exceeded its bound.
- `crates/pipeline/tests/dlq_and_backfill.rs` — quarantine and replay, and a
  backfilled gap merging with live data.
- `crates/pipeline/tests/read_api.rs` — the API served over a real socket
  against an index that has been through a reorg.
- `scripts/crash_recovery_proof.sh` — the same crash-recovery property at the
  process level: the real binaries, a real gRPC socket, and a real `SIGKILL`.
- `scripts/coverage.sh` — line coverage, using the toolchain's own LLVM
  instrumentation.

## Documentation

- [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md) — the reorg model, ordering, idempotency,
  crash recovery, and the schema.
- [docs/adr](docs/adr) — the decisions and what they cost.
- [docs/SCOPE.md](docs/SCOPE.md) — what this was built to.
- [VERIFICATION.md](VERIFICATION.md) — what is proven, and how.
- [BLOCKERS.md](BLOCKERS.md) — what is not, and why.
