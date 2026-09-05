# slot-stream

A Solana indexing pipeline that stays correct across chain reorganisations.

When the cluster abandons a branch, the data already written for that branch is
invalidated and the replacement branch is indexed in its place. What a reader
sees always matches what a from-scratch replay of the canonical chain would have
produced — at any reorg depth, through competing forks, and across a crash.

## Proven, not asserted

Reorg-immunity is the whole point of this project, so it is not left as a claim.
Every scenario below runs the **production pipeline** — the same composition root
the binary uses — against **real PostgreSQL 16**, and compares the resulting
database against a canonical replay computed independently of the fork detector.

**Reorg immunity — 9 scenarios, all passing.** Reorgs at depths 1, 5 and 32;
three branches competing off the same ancestor; six consecutive reorgs; a branch
whose ancestry reaches into a gap; and re-adoption of a branch the cluster had
already abandoned. In each, persisted state equals the canonical replay exactly —
same events, same slots, same order — with no duplicate rows.

```
--- reorg depth 32 ---
expected (canonical replay): 90 events
actual   (persisted state) : 90 events
expected slots: [100..119, 152..161]
actual   slots: [100..119, 152..161]
rows: 90 valid, 96 invalidated
```

**Crash recovery — real SIGKILL, real processes.** The shipped binaries talk over
a real gRPC socket; the indexer is killed with `SIGKILL` mid-stream, then
restarted. No unwinding, no chance to flush.

```
SIGKILL delivered with 81 rows committed (of 248)
restarted run resumed from source_seq 84 and ran to completion

PASS  no duplicate (slot, source_seq) rows
PASS  no canonical slot missing
PASS  canonical event count (248)
PASS  chain head (67)
PASS  reorg detected and recorded across the restart
```

**Backpressure — bounded under sustained overload.** With the consumer
deliberately crippled to one transaction per event: 1000 of 1000 events
persisted, **zero dropped**, 942 pushes blocked waiting for room, and a queue
that never exceeded its capacity of 32 across 2839 samples taken mid-run. RSS
moved 1 MB.

**The rest.** 103 tests pass. Idempotent writes, DLQ quarantine and replay, and
backfill merging with live data are each proven the same way — through the wired
pipeline, against real PostgreSQL and real HTTP endpoints. Coverage is 89% on the
fork detector and 84% across the reorg machinery.

Reproduce all of it — the role needs `CREATEDB`, since each integration test
creates and drops its own database:

```
export TEST_DATABASE_URL=postgres://user:pass@localhost:5432/postgres

cargo test --workspace              # 103 tests
scripts/crash_recovery_proof.sh     # the SIGKILL proof, on the real binaries
scripts/coverage.sh summary         # the coverage numbers above
```

*One honest gap: the Docker compose stack is defined but **unverified**. This
machine's WSL2 kernel ships no loadable modules, so Docker's bridge driver cannot
initialise and the daemon will not start. CI builds the image; the compose
networking is untested. See [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md#containers).*

## The problem

A Solana slot names its parent. Most of the time the parent is the slot you just
processed and the chain simply grows. Occasionally it is not: the cluster
switches to a branch that diverged some slots back, and everything indexed above
the divergence point describes a chain that no longer exists.

An indexer that ignores this accumulates rows nobody can distinguish from real
ones. An indexer that deletes on any surprise loses data the first time a stream
has a gap. The interesting work is telling those two situations apart.

## How it handles a fork

The processor keeps a slot to parent map over a bounded window of recent slots.
For each new slot it asks one question: does its parent lie on the chain we
believe in?

- The parent is the current head. The chain grew; nothing else to do.
- The parent is somewhere else. Walk back from that parent until the walk reaches
  a slot on our canonical chain. That slot is the common ancestor. Everything
  canonical above it is orphaned, and everything walked through on the way is the
  branch being adopted.
- The walk leaves the window without finding an ancestor. We cannot prove
  anything was orphaned, so nothing is rolled back. This is what a gap in the
  stream looks like, and destroying data on that evidence would be worse than
  keeping it.

A rollback is a soft delete: orphaned rows are marked `is_valid = false` rather
than removed, so the fork stays auditable and every read path filters on that
column. Crucially the operation has an inverse. Clusters switch back to branches
they abandoned, and nothing will re-deliver those events — so a rollback plan
carries both the slots to invalidate and the slots to restore, applied in one
transaction.

Rollbacks and writes travel the same channel, in the order the processor decided
on. Two channels would have no order relative to each other, and a rollback
arriving after the events that replaced the orphaned branch would invalidate the
wrong rows.

## Architecture

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
source. Nothing is buffered outside those two channels.

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

`pipeline` is a library rather than code inside the binary so the tests assemble
the pipeline the same way the binary does. A test that wired its own would be
testing an arrangement nothing ships.

## Running it

Requires Rust 1.88, `protoc`, and PostgreSQL 16. Migrations are applied on
startup — there is no separate migrate step.

```
docker compose up --build          # PostgreSQL, a chain source, the indexer
```

Or directly:

```
export DATABASE_URL=postgres://user:pass@localhost:5432/slot_stream
export GRPC_ENDPOINT=http://localhost:10000

cargo run --release --bin slot-stream-source &   # or a real Geyser source
cargo run --release --bin slot-stream
```

Then:

```
curl localhost:8080/v1/status
curl 'localhost:8080/v1/events?limit=10'
curl localhost:8080/v1/chain/head
```

## Configuration

Settings come from a TOML file named by `CONFIG_FILE`, from the environment, or
both — the environment is layered over the file, so a deployment overrides
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

The settings worth understanding are in `[ingester]`. `channel_capacity` bounds
how far ingestion may run ahead of the database, and `overflow_policy` decides
what happens at that bound: `block` applies real backpressure and never loses an
event, which is what an index wants, while `drop_newest` and `drop_oldest` shed
load instead.

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

## Design decisions

The three that shape everything else, covered properly in
[SYSTEM_DESIGN.md](SYSTEM_DESIGN.md) and the [ADRs](docs/adr):

**Fork detection walks to the true common ancestor** rather than comparing a
slot's parent against `slot - 1`. The latter catches a skipped slot, which is not
a fork, and misses the ordinary reorg entirely. The head is derived from the
canonical set rather than accumulated as a maximum, because a reorg can move the
head *backwards* — and a stale head turns every subsequent slot into a phantom
fork. ([ADR 001](docs/adr/001-reorg-detection.md))

**Two sequence numbers, doing different jobs.** `source_seq` is the stream's own
number, stable across redelivery, so row identity is `(slot, source_seq)`. `seq`
is assigned by the processor and defines the order readers see. Conflating them
breaks either idempotency or ordering. ([ADR 002](docs/adr/002-sequence-ordering.md))

**Writes are idempotent, and the upsert clears invalidation.** `ON CONFLICT DO
NOTHING` is idempotent in the narrow sense and wrong here: after a reorg has
marked a row invalid, re-delivery of that same event would leave it invisible
forever. Re-delivering an event is an assertion that it is real, and the write
records that. ([ADR 007](docs/adr/007-idempotency-and-restore.md))

## Documentation

- [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md) — the reorg model, ordering, idempotency,
  crash recovery, and the schema.
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — deploying it, and what to do when
  something is wrong.
- [docs/adr](docs/adr) — the decisions and what they cost.
