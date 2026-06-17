#!/usr/bin/env bash
#
# dialog-search-scope-backfill: drive maintenance.dialogsearch_scope_backfill_batch in a loop until
# the worker's DialogId range is exhausted (a batch scans 0 rows).
#
# This is a KEYSET-BATCHED backfill: each batch walks the next slice of the DialogSearch primary key
# (DialogId) and UPDATEs the two new columns in place. Each batch is its own autocommit transaction
# (per-batch commit -> vacuum horizon advances, WAL flushes, fully resumable). No candidate table.
#
# Resumability: the cursor lives in maintenance."DialogSearchScopeBackfill_Checkpoint" keyed by
# WORKER_ID. Kill a worker and restart with the same WORKER_ID -- it resumes from the checkpoint.
#
# Parallelism: run multiple workers, each with a DISTINCT WORKER_ID and a DISJOINT [START_AT, UNTIL)
# DialogId range. (DialogId is a time-ordered uuid; split the range into N chunks -- see the README.)
#   START_AT / UNTIL are only honored on a worker's FIRST batch (before its checkpoint exists);
#   thereafter the checkpoint cursor takes over, bounded by UNTIL.
#
# Connection info (same as the scaffold):
#   PG_CONNECTION_STRING -- libpq URI WITHOUT the password (let libpq read ~/.pgpass, chmod 600).
#   Falls back to PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD or PGSERVICE if unset.
#
# Usage:
#   export PG_CONNECTION_STRING='postgresql://repair_user@db.example.com:5432/dialogporten?sslmode=require'
#   WORKER_ID=w1 BATCH_SIZE=10000 ./repair.sh                       # single worker, whole table
#   WORKER_ID=smoke BATCH_SIZE=1000 MAX_BATCHES=1 ./repair.sh       # smoke test (one batch)
#   WORKER_ID=w1 SLEEP_BETWEEN_BATCHES=1 ./repair.sh                # throttled
#   # parallel (two disjoint halves split at the uuid midpoint):
#   WORKER_ID=a START_AT=00000000-0000-0000-0000-000000000000 UNTIL=80000000-0000-0000-0000-000000000000 ./repair.sh
#   WORKER_ID=b START_AT=80000000-0000-0000-0000-000000000000 ./repair.sh

set -euo pipefail

WORKER_ID="${WORKER_ID:-w1}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
MAX_BATCHES="${MAX_BATCHES:-0}"               # 0 = run until the range is exhausted
SLEEP_BETWEEN_BATCHES="${SLEEP_BETWEEN_BATCHES:-0}"
START_AT="${START_AT:-}"                       # uuid lower bound (exclusive); empty = min uuid
UNTIL="${UNTIL:-}"                             # uuid upper bound (exclusive); empty = no bound
PG_CONNECTION_STRING="${PG_CONNECTION_STRING:-}"
# synchronous_commit=off lets each batch return without waiting on WAL fsync. Safe here because the
# batch is idempotent: a crash that loses the last committed batch just leaves those rows NULL, and
# the next run (resuming from the checkpoint, guarded by IS NULL) redoes them. Set "on" for durable.
SYNC_COMMIT="${SYNC_COMMIT:-off}"
# STATEMENT_TIMEOUT bounds a pathological batch; LOCK_TIMEOUT turns a stall behind an app txn/DDL
# into a visible error instead of a hang. Set either to 0 to disable.
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-15min}"
LOCK_TIMEOUT="${LOCK_TIMEOUT:-10s}"

if [[ ! "${WORKER_ID}" =~ ^[A-Za-z0-9_.:-]+$ ]]; then
    echo "ERROR: WORKER_ID must match [A-Za-z0-9_.:-]+, got: ${WORKER_ID}" >&2
    exit 2
fi
for v in BATCH_SIZE MAX_BATCHES SLEEP_BETWEEN_BATCHES; do
    if [[ ! "${!v}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: ${v} must be a non-negative integer, got: ${!v}" >&2
        exit 2
    fi
done
uuid_re='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
for v in START_AT UNTIL; do
    if [[ -n "${!v}" && ! "${!v}" =~ ${uuid_re} ]]; then
        echo "ERROR: ${v} must be a uuid or empty, got: ${!v}" >&2
        exit 2
    fi
done
if [[ ! "${SYNC_COMMIT}" =~ ^(on|off|local|remote_write|remote_apply)$ ]]; then
    echo "ERROR: SYNC_COMMIT must be on|off|local|remote_write|remote_apply, got: ${SYNC_COMMIT}" >&2
    exit 2
fi
to_re='^[0-9]+(us|ms|s|min|h|d)?$'
for v in STATEMENT_TIMEOUT LOCK_TIMEOUT; do
    if [[ ! "${!v}" =~ ${to_re} ]]; then
        echo "ERROR: ${v} must be a Postgres duration, got: ${!v}" >&2
        exit 2
    fi
done

# uuid env -> SQL literal (or NULL).
START_AT_SQL="NULL"; [[ -n "${START_AT}" ]] && START_AT_SQL="'${START_AT}'::uuid"
UNTIL_SQL="NULL";    [[ -n "${UNTIL}"    ]] && UNTIL_SQL="'${UNTIL}'::uuid"

export PGAPPNAME="maint-dialog-search-scope-backfill-${WORKER_ID}"

PSQL_ARGS=(-X -A -t -v ON_ERROR_STOP=1)
if [[ -n "${PG_CONNECTION_STRING}" ]]; then
    PSQL_ARGS+=(-d "${PG_CONNECTION_STRING}")
fi

ts() { date -u +%FT%TZ; }

cat >&2 <<BANNER
$(ts) dialog-search-scope-backfill worker starting
  worker_id   = ${WORKER_ID}        (application_name=${PGAPPNAME})
  batch_size  = ${BATCH_SIZE}
  range       = (${START_AT:-min}, ${UNTIL:-unbounded})   (honored only on the first batch; then resumes from checkpoint)
  max_batches = ${MAX_BATCHES}      (0 = until exhausted)
  sleep       = ${SLEEP_BETWEEN_BATCHES}s between batches
  sync_commit = ${SYNC_COMMIT}
  timeouts    = statement:${STATEMENT_TIMEOUT} lock:${LOCK_TIMEOUT}
BANNER

batch_num=0

while :; do
    batch_num=$((batch_num + 1))

    # One batch = one autocommit transaction. The function returns (updated, scanned, next_cursor);
    # we emit "scanned|updated" for the driver. SETs apply to this psql connection (one batch).
    result=$(psql "${PSQL_ARGS[@]}" -c "
        SET synchronous_commit = ${SYNC_COMMIT};
        SET statement_timeout  = '${STATEMENT_TIMEOUT}';
        SET lock_timeout       = '${LOCK_TIMEOUT}';
        SELECT scanned || '|' || updated
        FROM maintenance.dialogsearch_scope_backfill_batch(
            '${WORKER_ID}', ${BATCH_SIZE}, ${START_AT_SQL}, ${UNTIL_SQL}
        );
    ")

    scanned="${result%%|*}"
    updated="${result##*|}"
    scanned="${scanned:-0}"; updated="${updated:-0}"
    echo "$(ts) worker=${WORKER_ID} batch=${batch_num} scanned=${scanned} updated=${updated}"

    # Keyset is deterministic (no SKIP LOCKED race), so a single empty batch means the range is done.
    if [[ "${scanned}" -eq 0 ]]; then
        echo "$(ts) worker=${WORKER_ID} done -- range exhausted"
        break
    fi

    if [[ "${MAX_BATCHES}" -gt 0 && "${batch_num}" -ge "${MAX_BATCHES}" ]]; then
        echo "$(ts) worker=${WORKER_ID} stopping after ${MAX_BATCHES} batches"
        break
    fi

    if [[ "${SLEEP_BETWEEN_BATCHES}" -gt 0 ]]; then
        sleep "${SLEEP_BETWEEN_BATCHES}"
    fi
done
