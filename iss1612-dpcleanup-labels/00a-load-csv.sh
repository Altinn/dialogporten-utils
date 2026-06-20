#!/usr/bin/env bash
#
# iss1612-dpcleanup-labels step 00a: load the source CSV into staging.
#
# Implemented as a shell wrapper (not a plain .sql) because psql's \copy takes its
# whole argument line literally -- it does NOT expand psql variables -- so the file
# path can't be parameterised from inside a .sql. We pipe the CSV via STDIN
# instead, which works regardless of where the file lives client-side.
#
# Input: ISS1612_DPCleanup.csv, header row, e.g.
#   "dialogid","TimeStamp","ActorId","SystemLabel"
#   "019a2b3b-...",2026-01-17 02:53:52.533 +0100,urn:altinn:organization:identifier-no:986762469,Archive
# The TimeStamp ("2026-01-17 02:53:52.533 +0100") parses straight into timestamptz.
#
# Connection: same convention as repair.sh -- set PG_CONNECTION_STRING (password in
# ~/.pgpass), or fall back to PG* env vars. Run 00_setup.sql first.
#
# Usage:
#   ./00a-load-csv.sh /mnt/backfilldata/corr-1612/ISS1612_DPCleanup.csv
#   CSV_PATH=/some/where/file.csv ./00a-load-csv.sh

set -euo pipefail

CSV_PATH="${1:-${CSV_PATH:-/mnt/backfilldata/corr-1612/ISS1612_DPCleanup.csv}}"
PG_CONNECTION_STRING="${PG_CONNECTION_STRING:-}"

if [[ ! -r "${CSV_PATH}" ]]; then
    echo "ERROR: CSV not readable: ${CSV_PATH}" >&2
    exit 2
fi

PSQL_ARGS=(-X -v ON_ERROR_STOP=1)
if [[ -n "${PG_CONNECTION_STRING}" ]]; then
    PSQL_ARGS+=(-d "${PG_CONNECTION_STRING}")
fi
psql_run() { psql "${PSQL_ARGS[@]}" "$@"; }

STG='maintenance."Iss1612DpcleanupLabels_Staging"'

echo "Loading CSV from ${CSV_PATH}"
psql_run -c "TRUNCATE ${STG};"
psql_run -c "\copy ${STG} (\"dialog_id\",\"source_ts\",\"actor_id\",\"system_label\") FROM STDIN WITH (FORMAT csv, HEADER true)" < "${CSV_PATH}"

# Report what landed and which labels appear. Confirm only expected labels
# (Archive/Bin/MarkedAsUnopened) show up; Default/Sent/unknown are out of scope
# and will be excluded by 01_build_candidates.sql.
psql_run <<'SQL'
\echo ''
\echo '-- Rows loaded:'
SELECT COUNT(*) AS staging_rows FROM maintenance."Iss1612DpcleanupLabels_Staging";

\echo ''
\echo '-- Distinct system_label values (verify against the in-scope set):'
SELECT "system_label", COUNT(*) AS rows
FROM maintenance."Iss1612DpcleanupLabels_Staging"
GROUP BY "system_label"
ORDER BY rows DESC;

\echo ''
\echo '-- Distinct actor urn prefixes (sanity-check the identifier scheme):'
SELECT split_part("actor_id", ':', 1) || ':' || split_part("actor_id", ':', 2)
         || ':' || split_part("actor_id", ':', 3) || ':' || split_part("actor_id", ':', 4)
         AS actor_prefix,
       COUNT(*) AS rows
FROM maintenance."Iss1612DpcleanupLabels_Staging"
GROUP BY actor_prefix
ORDER BY rows DESC;
SQL
