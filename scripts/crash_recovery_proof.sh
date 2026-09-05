#!/usr/bin/env bash
#
# Crash recovery, at the process level.
#
# The integration tests simulate a crash by abandoning a pipeline inside the
# test process. That proves the recovery logic, but it does not prove the
# shipped binary recovers: a test process still unwinds, still has a chance to
# flush. This does not. It starts the real `slot-stream` binary against the real
# `slot-stream-source` binary over a real gRPC socket, SIGKILLs the indexer
# mid-stream, restarts it, and checks the database.
#
# What must hold afterwards:
#   * no gaps    — every canonical slot is present
#   * no dupes   — (slot, source_seq) is unique, so replay after the restart
#                  overwrote rather than doubled
#   * canonical  — the persisted chain equals the chain the source described
#
# Usage: scripts/crash_recovery_proof.sh
set -euo pipefail

cd "$(dirname "$0")/.."

ADMIN_URL="${TEST_DATABASE_URL:-postgres://slotstream:slotstream@127.0.0.1:5432/slot_stream_test}"
DB_NAME="ss_crash_$$"
DB_URL="${ADMIN_URL%/*}/$DB_NAME"
SOURCE_PORT="${SOURCE_PORT:-19011}"
WORK_DIR="$(mktemp -d)"

SCRIPT_SLOTS="${SCRIPT_SLOTS:-60}"
SCRIPT_EVENTS_PER_SLOT="${SCRIPT_EVENTS_PER_SLOT:-4}"
SCRIPT_REORG_DEPTH="${SCRIPT_REORG_DEPTH:-5}"
# Paced so the run lasts long enough to be interrupted in the middle.
SOURCE_PACE_MS="${SOURCE_PACE_MS:-6}"

# The canonical chain the source describes, derived here rather than read back
# from the indexer: slots 1..(head-depth), then the fork run past the tip.
HEAD=$SCRIPT_SLOTS
DIVERGENCE=$((HEAD - SCRIPT_REORG_DEPTH))
FORK_LAST=$((HEAD + SCRIPT_REORG_DEPTH + 2))
EXPECTED_SLOTS=$((DIVERGENCE + SCRIPT_REORG_DEPTH + 2))
EXPECTED_EVENTS=$((EXPECTED_SLOTS * SCRIPT_EVENTS_PER_SLOT))

SOURCE_PID=""
INDEXER_PID=""

cleanup() {
    [ -n "$INDEXER_PID" ] && kill -9 "$INDEXER_PID" 2>/dev/null || true
    [ -n "$SOURCE_PID" ] && kill -9 "$SOURCE_PID" 2>/dev/null || true
    psql "$ADMIN_URL" -q -c "DROP DATABASE IF EXISTS \"$DB_NAME\" WITH (FORCE)" >/dev/null 2>&1 || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

sql() { psql "$DB_URL" -tAq -c "$1"; }

echo "=== crash recovery proof (process level) ==="
echo "source     : $SCRIPT_SLOTS slots x $SCRIPT_EVENTS_PER_SLOT events, reorg depth $SCRIPT_REORG_DEPTH"
echo "canonical  : slots 1..$DIVERGENCE then $((HEAD + 1))..$FORK_LAST  ($EXPECTED_SLOTS slots, $EXPECTED_EVENTS events)"
echo "database   : $DB_NAME"
echo

echo "--- building the real binaries ---"
cargo build --bin slot-stream --bin slot-stream-source 2>&1 | tail -1
BIN=target/debug

psql "$ADMIN_URL" -q -c "CREATE DATABASE \"$DB_NAME\"" >/dev/null

cat > "$WORK_DIR/config.toml" <<EOF
[database]
url = "$DB_URL"
max_connections = 8
min_connections = 1

[ingester]
channel_capacity = 256
overflow_policy = "block"

[persister]
batch_size = 16
batch_timeout_ms = 25

[grpc]
endpoint = "http://127.0.0.1:$SOURCE_PORT"

[api]
enabled = false

[observability]
metrics_enabled = false
EOF

echo "--- starting the source ---"
SOURCE_BIND="127.0.0.1:$SOURCE_PORT" \
SOURCE_PACE_MS="$SOURCE_PACE_MS" \
SCRIPT_SLOTS="$SCRIPT_SLOTS" \
SCRIPT_EVENTS_PER_SLOT="$SCRIPT_EVENTS_PER_SLOT" \
SCRIPT_REORG_DEPTH="$SCRIPT_REORG_DEPTH" \
    "$BIN/slot-stream-source" > "$WORK_DIR/source.log" 2>&1 &
SOURCE_PID=$!
# Nothing waits on the source; disowning keeps the shell from announcing the
# kill in the cleanup trap, which reads like a failure when it is not.
disown "$SOURCE_PID" 2>/dev/null || true

for _ in $(seq 1 50); do
    grep -q "serving chain source" "$WORK_DIR/source.log" && break
    sleep 0.2
done
echo "source pid $SOURCE_PID on 127.0.0.1:$SOURCE_PORT"

start_indexer() {
    CONFIG_FILE="$WORK_DIR/config.toml" \
    GRPC_ENDPOINT="http://127.0.0.1:$SOURCE_PORT" \
    LOG_LEVEL=info \
        "$BIN/slot-stream" > "$1" 2>&1 &
    INDEXER_PID=$!
}

echo
echo "--- run 1: start, then SIGKILL mid-stream ---"
start_indexer "$WORK_DIR/indexer1.log"
echo "indexer pid $INDEXER_PID"

# Wait until it is demonstrably mid-stream: some rows committed, but not all.
KILL_AT=0
for _ in $(seq 1 200); do
    KILL_AT=$(sql "SELECT COUNT(*) FROM events" 2>/dev/null || echo 0)
    if [ "${KILL_AT:-0}" -ge 40 ]; then break; fi
    sleep 0.1
done

if [ "${KILL_AT:-0}" -lt 40 ]; then
    echo "FAIL: the indexer never committed enough rows to interrupt (saw $KILL_AT)"
    echo "--- indexer log ---"; tail -30 "$WORK_DIR/indexer1.log"
    echo "--- source log ---"; tail -30 "$WORK_DIR/source.log"
    exit 1
fi
if [ "$KILL_AT" -ge "$EXPECTED_EVENTS" ]; then
    echo "FAIL: the run finished before it could be interrupted; lower SOURCE_PACE_MS"
    exit 1
fi

kill -9 "$INDEXER_PID"
wait "$INDEXER_PID" 2>/dev/null || true
KILLED_PID=$INDEXER_PID
INDEXER_PID=""
echo "SIGKILL delivered to $KILLED_PID with $KILL_AT rows committed (of $EXPECTED_EVENTS)"

AFTER_CRASH_ROWS=$(sql "SELECT COUNT(*) FROM events")
AFTER_CRASH_VALID=$(sql "SELECT COUNT(*) FROM events WHERE is_valid")
CURSOR=$(sql "SELECT COALESCE(MAX(source_seq), 0) FROM cursors")
echo "state after the crash: $AFTER_CRASH_ROWS rows ($AFTER_CRASH_VALID valid), cursor at source_seq $CURSOR"
echo "  (no clean shutdown ran: the process was killed, not signalled)"

echo
echo "--- run 2: restart and finish ---"
start_indexer "$WORK_DIR/indexer2.log"
echo "indexer pid $INDEXER_PID"
if ! timeout 180 tail --pid="$INDEXER_PID" -f /dev/null; then
    echo "FAIL: the restarted indexer did not finish within 180s"
    tail -20 "$WORK_DIR/indexer2.log"
    exit 1
fi
INDEXER_PID=""
grep -q "slot-stream stopped" "$WORK_DIR/indexer2.log" || {
    echo "FAIL: the restarted indexer did not stop cleanly"
    tail -20 "$WORK_DIR/indexer2.log"
    exit 1
}
RESUMED_AT=$(grep -o '"resume_source_seq":[0-9]*' "$WORK_DIR/indexer2.log" | head -1 | cut -d: -f2)
echo "restarted run resumed from source_seq ${RESUMED_AT:-unknown} and ran to completion"
if [ "${RESUMED_AT:-0}" = "0" ]; then
    echo "NOTE: the restart began from zero, so this run re-read the whole stream"
fi

echo
echo "--- final database state ---"
FINAL_VALID=$(sql "SELECT COUNT(*) FROM events WHERE is_valid")
FINAL_INVALID=$(sql "SELECT COUNT(*) FROM events WHERE NOT is_valid")
FINAL_SLOTS=$(sql "SELECT COUNT(DISTINCT slot) FROM events WHERE is_valid")
DUPES=$(sql "SELECT COUNT(*) FROM (SELECT slot, source_seq FROM events GROUP BY slot, source_seq HAVING COUNT(*) > 1) d")
REORGS=$(sql "SELECT COUNT(*) FROM reorgs")
HEAD_SLOT=$(sql "SELECT COALESCE(MAX(slot), 0) FROM events WHERE is_valid")
psql "$DB_URL" -c "SELECT COUNT(*) FILTER (WHERE is_valid) AS valid, COUNT(*) FILTER (WHERE NOT is_valid) AS orphaned, MIN(slot) AS min_slot, MAX(slot) FILTER (WHERE is_valid) AS head FROM events"
psql "$DB_URL" -c "SELECT fork_slot, divergence_point, rollback_depth, events_invalidated FROM reorgs ORDER BY detected_at"

# The canonical slot set, computed here, compared against what is stored.
MISSING=$(sql "
    WITH expected AS (
        SELECT generate_series(1, $DIVERGENCE) AS slot
        UNION ALL
        SELECT generate_series($((HEAD + 1)), $FORK_LAST)
    )
    SELECT COUNT(*) FROM expected e
    WHERE NOT EXISTS (SELECT 1 FROM events v WHERE v.is_valid AND v.slot = e.slot)")
EXTRA=$(sql "
    WITH expected AS (
        SELECT generate_series(1, $DIVERGENCE) AS slot
        UNION ALL
        SELECT generate_series($((HEAD + 1)), $FORK_LAST)
    )
    SELECT COUNT(DISTINCT slot) FROM events v
    WHERE v.is_valid AND v.slot NOT IN (SELECT slot FROM expected)")

echo
echo "--- verdict ---"
FAILED=0
check() {
    if [ "$2" = "$3" ]; then
        echo "PASS  $1 ($2)"
    else
        echo "FAIL  $1: expected $3, got $2"
        FAILED=1
    fi
}
check "no duplicate (slot, source_seq) rows" "$DUPES" 0
check "no canonical slot missing"            "$MISSING" 0
check "no non-canonical slot served"         "$EXTRA" 0
check "canonical slot count"                 "$FINAL_SLOTS" "$EXPECTED_SLOTS"
check "canonical event count"                "$FINAL_VALID" "$EXPECTED_EVENTS"
check "chain head"                           "$HEAD_SLOT" "$FORK_LAST"

if [ "$REORGS" -lt 1 ]; then
    echo "FAIL  the reorg was never detected, so this run proved nothing about recovery across a fork"
    FAILED=1
else
    echo "PASS  reorg detected and recorded ($REORGS)"
fi
if [ "$FINAL_INVALID" -lt 1 ]; then
    echo "FAIL  nothing was orphaned, so the fork did not take effect"
    FAILED=1
else
    echo "PASS  orphaned rows retained ($FINAL_INVALID)"
fi

echo
if [ "$FAILED" = 0 ]; then
    echo "RESULT: PASS — killed at $KILL_AT rows, recovered to $FINAL_VALID canonical events with no gaps and no duplicates"
else
    echo "RESULT: FAIL"
fi
exit "$FAILED"
