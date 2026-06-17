#!/usr/bin/env bash
#
# dialog-search-scope-backfill: drive maintenance.dialogsearchscopebackfill_run_batch in a loop until the
# candidate table drains.
#
# Each invocation is a single worker. To parallelize, run multiple shells with
# distinct WORKER_ID values -- FOR UPDATE SKIP LOCKED inside the procedure
# ensures workers never claim overlapping candidates.
#
# Connection info:
#   PG_CONNECTION_STRING -- preferred. A libpq URI, e.g.
#       postgresql://user@host:5432/dbname?sslmode=require
#     Do NOT embed the password in this string -- leave it out and let libpq
#     pull it from ~/.pgpass (chmod 600). This avoids having the password in
#     process listings, shell history, or environment dumps.
#   Falls back to individual libpq env vars (PGHOST/PGPORT/PGDATABASE/PGUSER/
#   PGPASSWORD) or PGSERVICE if PG_CONNECTION_STRING is unset.
#
# Usage:
#   export PG_CONNECTION_STRING='postgresql://repair_user@db.example.com:5432/dialogporten?sslmode=require'
#   # ...and have a matching entry in ~/.pgpass:
#   #   db.example.com:5432:dialogporten:repair_user:<password>
#
#   WORKER_ID=w1 BATCH_SIZE=2000 ./repair.sh          # single worker
#   WORKER_ID=w2 BATCH_SIZE=2000 ./repair.sh          # in parallel, another shell
#   WORKER_ID=smoke BATCH_SIZE=100 MAX_BATCHES=1 ./repair.sh   # smoke test
#   WORKER_ID=w1 BATCH_SIZE=1000 SLEEP_BETWEEN_BATCHES=2 ./repair.sh  # rate-limited
#   WORKER_ID=w1 SYNC_COMMIT=on ./repair.sh           # durable commits

set -euo pipefail

WORKER_ID="${WORKER_ID:-w1}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
MAX_BATCHES="${MAX_BATCHES:-0}"               # 0 = run until empty
SLEEP_BETWEEN_BATCHES="${SLEEP_BETWEEN_BATCHES:-0}"
PG_CONNECTION_STRING="${PG_CONNECTION_STRING:-}"
# synchronous_commit=off lets each batch return without waiting on WAL fsync.
# Safe here because the procedure is idempotent: if a crash loses the last
# committed batch, the candidate rows stay NULL/unclaimable and the application
# rows stay in their pre-repair state -- re-running picks them up cleanly. Set
# to "on" to fall back to default durable commits.
SYNC_COMMIT="${SYNC_COMMIT:-off}"
# Per-batch safety valves. STATEMENT_TIMEOUT bounds a pathological plan;
# LOCK_TIMEOUT turns an indefinite stall behind an app txn or DDL into a visible
# error instead of a hang. Set either to 0 to disable.
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-15min}"
LOCK_TIMEOUT="${LOCK_TIMEOUT:-10s}"

# Refuse a worker-id that would break the SQL literal interpolation below.
if [[ ! "${WORKER_ID}" =~ ^[A-Za-z0-9_.:-]+$ ]]; then
    echo "ERROR: WORKER_ID must match [A-Za-z0-9_.:-]+, got: ${WORKER_ID}" >&2
    exit 2
fi
# These three are interpolated into SQL -- enforce plain integers.
for v in BATCH_SIZE MAX_BATCHES SLEEP_BETWEEN_BATCHES; do
    if [[ ! "${!v}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: ${v} must be a non-negative integer, got: ${!v}" >&2
        exit 2
    fi
done
if [[ ! "${SYNC_COMMIT}" =~ ^(on|off|local|remote_write|remote_apply)$ ]]; then
    echo "ERROR: SYNC_COMMIT must be on|off|local|remote_write|remote_apply, got: ${SYNC_COMMIT}" >&2
    exit 2
fi
# Postgres duration literals: a bare number (ms) or number + unit.
to_re='^[0-9]+(us|ms|s|min|h|d)?$'
if [[ ! "${STATEMENT_TIMEOUT}" =~ ${to_re} ]]; then
    echo "ERROR: STATEMENT_TIMEOUT must be a Postgres duration, got: ${STATEMENT_TIMEOUT}" >&2
    exit 2
fi
if [[ ! "${LOCK_TIMEOUT}" =~ ${to_re} ]]; then
    echo "ERROR: LOCK_TIMEOUT must be a Postgres duration, got: ${LOCK_TIMEOUT}" >&2
    exit 2
fi

# application_name makes each worker identifiable in pg_stat_activity (and in
# 04_progress.sql). libpq reads PGAPPNAME for both the URI and component-var
# connection paths, so we don't have to munge the connection string.
export PGAPPNAME="maint-dialog-search-scope-backfill-${WORKER_ID}"

# psql invocation -- prepend the connection string if provided. -d accepts a
# full libpq URI; password is resolved by libpq via .pgpass.
PSQL_ARGS=(-X -A -t -v ON_ERROR_STOP=1)
if [[ -n "${PG_CONNECTION_STRING}" ]]; then
    PSQL_ARGS+=(-d "${PG_CONNECTION_STRING}")
fi

ts() { date -u +%FT%TZ; }

cat >&2 <<BANNER
$(ts) dialog-search-scope-backfill worker starting
  worker_id   = ${WORKER_ID}        (application_name=${PGAPPNAME})
  batch_size  = ${BATCH_SIZE}
  max_batches = ${MAX_BATCHES}      (0 = until empty)
  sleep       = ${SLEEP_BETWEEN_BATCHES}s between batches
  sync_commit = ${SYNC_COMMIT}
  timeouts    = statement:${STATEMENT_TIMEOUT} lock:${LOCK_TIMEOUT}
BANNER

batch_num=0
empty_streak=0

while :; do
    batch_num=$((batch_num + 1))

    # CALL returns claimed/updated/skipped via OUT parameters; we read the
    # latest batch row from the log to report them in one round trip. SET applies
    # for the lifetime of this psql connection (one batch); each iteration opens
    # a fresh connection, so we re-issue the SETs each time.
    result=$(psql "${PSQL_ARGS[@]}" -c "
        SET synchronous_commit = ${SYNC_COMMIT};
        SET statement_timeout  = '${STATEMENT_TIMEOUT}';
        SET lock_timeout       = '${LOCK_TIMEOUT}';
        CALL maintenance.dialogsearchscopebackfill_run_batch(
            ${BATCH_SIZE}, '${WORKER_ID}', NULL::int, NULL::int, NULL::int
        );
        SELECT 'claimed=' || \"Claimed\" ||
               ',updated=' || \"Updated\" ||
               ',skipped=' || \"Skipped\"
        FROM maintenance.\"DialogSearchScopeBackfill_BatchLog\"
        WHERE \"WorkerId\" = '${WORKER_ID}'
        ORDER BY \"BatchId\" DESC
        LIMIT 1;
    ")

    echo "$(ts) worker=${WORKER_ID} batch=${batch_num} ${result}"

    claimed=$(printf '%s' "$result" | sed -n 's/.*claimed=\([0-9]*\).*/\1/p')
    claimed="${claimed:-0}"

    if [[ "${claimed}" -eq 0 ]]; then
        # Two consecutive empty claims = done. The guard handles the rare case
        # where SKIP LOCKED returned empty only because other workers had
        # everything locked at that instant -- pause briefly so the recheck is
        # meaningful rather than instantaneous.
        empty_streak=$((empty_streak + 1))
        if [[ "${empty_streak}" -ge 2 ]]; then
            echo "$(ts) worker=${WORKER_ID} done -- no rows claimed in 2 successive batches"
            break
        fi
        sleep 2
    else
        empty_streak=0
    fi

    if [[ "${MAX_BATCHES}" -gt 0 && "${batch_num}" -ge "${MAX_BATCHES}" ]]; then
        echo "$(ts) worker=${WORKER_ID} stopping after ${MAX_BATCHES} batches"
        break
    fi

    if [[ "${SLEEP_BETWEEN_BATCHES}" -gt 0 ]]; then
        sleep "${SLEEP_BETWEEN_BATCHES}"
    fi
done
