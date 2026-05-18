#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
database_url="${1:-${DATABASE_URL:-}}"

if [[ -z "$database_url" ]]; then
    echo "Usage: $0 <database-url>"
    echo "Alternatively set DATABASE_URL."
    exit 2
fi

batch_size="${BATCH_SIZE:-100000}"
sleep_seconds="${SLEEP_SECONDS:-0}"
repair_name="${REPAIR_NAME:-has_unopened_content_repair_2026_02_25_2026_03_20}"

while true; do
    set +e
    output="$(
        psql "$database_url" \
            -v ON_ERROR_STOP=1 \
            -v repair_name="$repair_name" \
            -v batch_size="$batch_size" \
            -f "$script_dir/02_discover_candidates.sql" 2>&1
    )"
    status=$?
    set -e

    printf '%s\n' "$output"

    if [[ "$status" -ne 0 ]]; then
        exit "$status"
    fi

    if grep -q "discovery complete" <<< "$output" || grep -q "discovery already complete" <<< "$output"; then
        break
    fi

    if [[ "$sleep_seconds" != "0" ]]; then
        sleep "$sleep_seconds"
    fi
done
