#!/usr/bin/env bash
set -euo pipefail

PG_CONNECTION_STRING="${PG_CONNECTION_STRING:?set PG_CONNECTION_STRING}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
MIN_BATCH_SIZE="${MIN_BATCH_SIZE:-1000}"
MAX_BATCH_SIZE="${MAX_BATCH_SIZE:-20000}"
TARGET_BATCH_SECONDS="${TARGET_BATCH_SECONDS:-3}"
IDLE_SLEEP_SECONDS="${IDLE_SLEEP_SECONDS:-0.5}"
PROGRESS_INTERVAL_SECONDS="${PROGRESS_INTERVAL_SECONDS:-2}"
PSQL_CONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT:-10}"
VERBOSE="${VERBOSE:-0}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found in PATH" >&2
  exit 1
fi

if [[ "$PG_CONNECTION_STRING" != postgresql://* && "$PG_CONNECTION_STRING" != postgres://* ]]; then
  echo "PG_CONNECTION_STRING must be a native PostgreSQL URL (postgresql://...)." >&2
  exit 1
fi

for var_name in BATCH_SIZE MIN_BATCH_SIZE MAX_BATCH_SIZE TARGET_BATCH_SECONDS PROGRESS_INTERVAL_SECONDS PSQL_CONNECT_TIMEOUT; do
  value="${!var_name}"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$var_name must be a positive integer, got: $value" >&2
    exit 1
  fi
done

if [[ ! "$IDLE_SLEEP_SECONDS" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "IDLE_SLEEP_SECONDS must be numeric, got: $IDLE_SLEEP_SECONDS" >&2
  exit 1
fi

if [[ "$VERBOSE" != "0" && "$VERBOSE" != "1" ]]; then
  echo "VERBOSE must be 0 or 1, got: $VERBOSE" >&2
  exit 1
fi

if (( MIN_BATCH_SIZE > MAX_BATCH_SIZE )); then
  echo "MIN_BATCH_SIZE must be <= MAX_BATCH_SIZE" >&2
  exit 1
fi

if (( BATCH_SIZE < MIN_BATCH_SIZE )); then
  BATCH_SIZE="$MIN_BATCH_SIZE"
fi

if (( BATCH_SIZE > MAX_BATCH_SIZE )); then
  BATCH_SIZE="$MAX_BATCH_SIZE"
fi

psql_cmd() {
  PGCONNECT_TIMEOUT="$PSQL_CONNECT_TIMEOUT" psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 "$@"
}

current_batch_size="$BATCH_SIZE"
total_rows=0
start_epoch="$(date +%s)"
last_print_epoch="$start_epoch"
last_batch_seconds=0

while true; do
  batch_started_epoch="$(date +%s)"
  row="$(psql_cmd -At -F '|' -c "
    SELECT
      \"AllCompleted\",
      \"ProcessedRows\"
    FROM partyresource.backfill_dialog_partyresource_batch(${current_batch_size});
  ")"
  batch_finished_epoch="$(date +%s)"

  if [[ -z "$row" ]]; then
    echo "received empty response from backfill function" >&2
    exit 1
  fi

  IFS='|' read -r completed processed <<< "$row"

  if [[ ! "$processed" =~ ^[0-9]+$ ]]; then
    echo "invalid ProcessedRows value from backfill function: $processed" >&2
    exit 1
  fi

  total_rows=$((total_rows + processed))

  batch_elapsed=$((batch_finished_epoch - batch_started_epoch))
  if (( batch_elapsed < 1 )); then
    batch_elapsed=1
  fi
  last_batch_seconds="$batch_elapsed"

  if (( processed > 0 )); then
    if (( batch_elapsed > TARGET_BATCH_SECONDS * 2 )) && (( current_batch_size > MIN_BATCH_SIZE )); then
      current_batch_size=$((current_batch_size / 2))
      if (( current_batch_size < MIN_BATCH_SIZE )); then
        current_batch_size="$MIN_BATCH_SIZE"
      fi
    elif (( batch_elapsed < TARGET_BATCH_SECONDS / 2 )) && (( current_batch_size < MAX_BATCH_SIZE )); then
      current_batch_size=$((current_batch_size * 2))
      if (( current_batch_size > MAX_BATCH_SIZE )); then
        current_batch_size="$MAX_BATCH_SIZE"
      fi
    fi
  fi

  now="$(date +%s)"
  if (( now - last_print_epoch >= PROGRESS_INTERVAL_SECONDS )); then
    elapsed=$((now - start_epoch))
    if (( elapsed < 1 )); then
      elapsed=1
    fi

    rows_per_sec=$((total_rows / elapsed))
    if [[ "$VERBOSE" == "1" ]]; then
      echo "progress total_rows=${total_rows} rows_per_sec=${rows_per_sec} batch_size=${current_batch_size} last_batch_seconds=${last_batch_seconds}"
    else
      echo "progress total_rows=${total_rows} rows_per_sec=${rows_per_sec}"
    fi
    last_print_epoch="$now"
  fi

  if [[ "$completed" == "t" ]]; then
    break
  fi

  if [[ "$processed" == "0" ]]; then
    sleep "$IDLE_SLEEP_SECONDS"
  fi
done

end_epoch="$(date +%s)"
elapsed=$((end_epoch - start_epoch))
if (( elapsed < 1 )); then
  elapsed=1
fi
rows_per_sec=$((total_rows / elapsed))

if [[ "$VERBOSE" == "1" ]]; then
  echo "Backfill completed. total_rows=${total_rows} rows_per_sec=${rows_per_sec} batch_size=${current_batch_size} last_batch_seconds=${last_batch_seconds}"
else
  echo "Backfill completed. total_rows=${total_rows} rows_per_sec=${rows_per_sec}"
fi
