#!/usr/bin/env bash
set -euo pipefail

PG_CONNECTION_STRING="${PG_CONNECTION_STRING:?set PG_CONNECTION_STRING}"
BATCH_SIZE="${BATCH_SIZE:-20000}"
IDLE_SLEEP_SECONDS="${IDLE_SLEEP_SECONDS:-0.5}"
PROGRESS_INTERVAL_SECONDS="${PROGRESS_INTERVAL_SECONDS:-2}"
PSQL_CONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT:-10}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found in PATH" >&2
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

if [[ "$PG_CONNECTION_STRING" != postgresql://* && "$PG_CONNECTION_STRING" != postgres://* ]]; then
  echo "PG_CONNECTION_STRING must be a native PostgreSQL URL (postgresql://...)." >&2
  exit 1
fi

psql_cmd() {
  PGCONNECT_TIMEOUT="$PSQL_CONNECT_TIMEOUT" psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 "$@"
}

total_rows=0
start_epoch="$(date +%s)"
last_print_epoch="$start_epoch"

while true; do
  row="$(psql_cmd -At -F '|' -c "
    SELECT
      \"AllCompleted\",
      \"ProcessedRows\"
    FROM partyresource.backfill_dialog_partyresource_batch(${BATCH_SIZE});
  ")"

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

  now="$(date +%s)"
  if (( now - last_print_epoch >= PROGRESS_INTERVAL_SECONDS )); then
    elapsed=$((now - start_epoch))
    if (( elapsed < 1 )); then
      elapsed=1
    fi

    rows_per_sec=$((total_rows / elapsed))
    echo "progress total_rows=${total_rows} rows_per_sec=${rows_per_sec}"
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

echo "Backfill completed. total_rows=${total_rows} rows_per_sec=${rows_per_sec}"
