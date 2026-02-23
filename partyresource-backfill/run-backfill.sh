#!/usr/bin/env bash
set -euo pipefail

PG_CONNECTION_STRING="${PG_CONNECTION_STRING:?set PG_CONNECTION_STRING}"
WORKERS="${WORKERS:-8}"
BATCH_SIZE="${BATCH_SIZE:-20000}"
IDLE_SLEEP_SECONDS="${IDLE_SLEEP_SECONDS:-0.5}"
PROGRESS_INTERVAL_SECONDS="${PROGRESS_INTERVAL_SECONDS:-2}"
PSQL_CONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT:-10}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found in PATH" >&2
  exit 1
fi

if [[ ! "$WORKERS" =~ ^[1-9][0-9]*$ ]]; then
  echo "WORKERS must be a positive integer, got: $WORKERS" >&2
  exit 1
fi

if [[ ! "$BATCH_SIZE" =~ ^[1-9][0-9]*$ ]]; then
  echo "BATCH_SIZE must be a positive integer, got: $BATCH_SIZE" >&2
  exit 1
fi

if [[ ! "$IDLE_SLEEP_SECONDS" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "IDLE_SLEEP_SECONDS must be numeric, got: $IDLE_SLEEP_SECONDS" >&2
  exit 1
fi

if [[ ! "$PROGRESS_INTERVAL_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "PROGRESS_INTERVAL_SECONDS must be a positive integer, got: $PROGRESS_INTERVAL_SECONDS" >&2
  exit 1
fi

if [[ ! "$PSQL_CONNECT_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
  echo "PSQL_CONNECT_TIMEOUT must be a positive integer, got: $PSQL_CONNECT_TIMEOUT" >&2
  exit 1
fi

psql_cmd() {
  PGCONNECT_TIMEOUT="$PSQL_CONNECT_TIMEOUT" psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 "$@"
}

if [[ "$PG_CONNECTION_STRING" != postgresql://* && "$PG_CONNECTION_STRING" != postgres://* ]]; then
  echo "PG_CONNECTION_STRING must be a native PostgreSQL URL (postgresql://...)." >&2
  exit 1
fi

progress_aggregator() {
  local total_rows=0
  local start_epoch
  local last_print_epoch
  local now
  local elapsed
  local rows_per_sec
  local processed

  start_epoch="$(date +%s)"
  last_print_epoch="$start_epoch"

  while IFS= read -r processed; do
    if [[ "$processed" =~ ^[0-9]+$ ]]; then
      total_rows=$((total_rows + processed))
    fi

    now="$(date +%s)"
    if (( now - last_print_epoch >= PROGRESS_INTERVAL_SECONDS )); then
      elapsed=$((now - start_epoch))
      if (( elapsed < 1 )); then
        elapsed=1
      fi

      rows_per_sec=$((total_rows / elapsed))
      echo "progress total_rows=${total_rows} rows_per_sec=${rows_per_sec}"
      echo "${total_rows}|${rows_per_sec}" > "$progress_state_file"
      last_print_epoch="$now"
    fi
  done

  now="$(date +%s)"
  elapsed=$((now - start_epoch))
  if (( elapsed < 1 )); then
    elapsed=1
  fi

  rows_per_sec=$((total_rows / elapsed))
  echo "progress total_rows=${total_rows} rows_per_sec=${rows_per_sec}"
  echo "${total_rows}|${rows_per_sec}" > "$progress_state_file"
}

run_worker() {
  local wid="$1"

  while true; do
    local row
    if ! row=$(psql_cmd -At -F '|' -c "
      SELECT
        \"AllShardsCompleted\",
        COALESCE(\"ShardId\"::text, ''),
        \"ProcessedRows\"
      FROM partyresource.backfill_dialog_partyresource_batch(${BATCH_SIZE});
    " 2>&1); then
      echo "worker=${wid} psql_error=1" >&2
      echo "$row" >&2
      return 1
    fi

    if [[ -z "$row" ]]; then
      echo "worker=${wid} received empty response from backfill function" >&2
      return 1
    fi

    local all_done
    local shard
    local processed
    IFS='|' read -r all_done shard processed <<< "$row"

    printf '%s\n' "${processed:-0}" >&3

    [[ "$all_done" == "t" ]] && break
    [[ -z "$shard" || "$processed" == "0" ]] && sleep "$IDLE_SLEEP_SECONDS"
  done
}

progress_fifo="$(mktemp -u "${TMPDIR:-/tmp}/partyresource-progress.XXXXXX")"
progress_state_file="$(mktemp "${TMPDIR:-/tmp}/partyresource-state.XXXXXX")"
mkfifo "$progress_fifo"
echo "0|0" > "$progress_state_file"

progress_aggregator < "$progress_fifo" &
progress_pid="$!"
exec 3> "$progress_fifo"

worker_pids=()
for ((i = 1; i <= WORKERS; i++)); do
  run_worker "$i" &
  worker_pids+=("$!")
done

cleanup() {
  exec 3>&- || true

  for pid in "${worker_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done

  if [[ -n "${progress_pid:-}" ]]; then
    kill "$progress_pid" >/dev/null 2>&1 || true
  fi

  rm -f "$progress_fifo" "$progress_state_file"
}
trap cleanup EXIT INT TERM

worker_failed=0
for pid in "${worker_pids[@]}"; do
  if ! wait "$pid"; then
    worker_failed=1
  fi
done

if (( worker_failed != 0 )); then
  echo "Backfill failed: one or more workers exited with an error." >&2
  exec 3>&- || true
  wait "$progress_pid" || true
  exit 1
fi

exec 3>&-
wait "$progress_pid"

IFS='|' read -r total_rows rows_per_sec < "$progress_state_file"
echo "Backfill completed. total_rows=${total_rows} rows_per_sec=${rows_per_sec}"
