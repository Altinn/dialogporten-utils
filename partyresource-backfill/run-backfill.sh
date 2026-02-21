#!/usr/bin/env bash
set -euo pipefail

PG_CONNECTION_STRING="${PG_CONNECTION_STRING:?set PG_CONNECTION_STRING}"
WORKERS="${WORKERS:-8}"
BATCH_SIZE="${BATCH_SIZE:-50000}"
DEADLOCK_RETRY_SLEEP_SECONDS="${DEADLOCK_RETRY_SLEEP_SECONDS:-1}"

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

if [[ ! "$DEADLOCK_RETRY_SLEEP_SECONDS" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "DEADLOCK_RETRY_SLEEP_SECONDS must be numeric, got: $DEADLOCK_RETRY_SLEEP_SECONDS" >&2
  exit 1
fi

run_worker() {
  local wid="$1"
  while true; do
    # all_done|shard|processed|pairs
    local row
    if ! row=$(psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -At -F '|' -c "
      SELECT
        \"AllShardsCompleted\",
        COALESCE(\"ShardId\"::text, ''),
        \"ProcessedRows\",
        \"InsertedPairs\"
      FROM partyresource.backfill_dialog_partyresource_batch(${BATCH_SIZE});
    " 2>&1); then
      if grep -qi "deadlock detected" <<< "$row"; then
        echo "worker=${wid} deadlock_detected=1 action=retry" >&2
        sleep "$DEADLOCK_RETRY_SLEEP_SECONDS"
        continue
      fi

      echo "worker=${wid} psql_error=1" >&2
      echo "$row" >&2
      return 1
    fi

    if [[ -z "$row" ]]; then
      echo "worker=${wid} received empty response from backfill function" >&2
      return 1
    fi

    IFS='|' read -r all_done shard processed pairs <<< "$row"
    echo "worker=${wid} all_done=${all_done} shard=${shard} processed=${processed} pairs=${pairs}"

    [[ "$all_done" == "t" ]] && break
    [[ "$processed" == "0" ]] && sleep 0.5
  done
}

worker_pids=()
for ((i = 1; i <= WORKERS; i++)); do
  run_worker "$i" &
  worker_pids+=("$!")
done

worker_failed=0
for pid in "${worker_pids[@]}"; do
  if ! wait "$pid"; then
    worker_failed=1
  fi
done

if (( worker_failed != 0 )); then
  echo "Backfill failed: one or more workers exited with an error." >&2
  exit 1
fi

echo "Backfill completed."
