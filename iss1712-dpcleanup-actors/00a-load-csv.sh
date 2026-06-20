#!/usr/bin/env bash
#
# iss1712-dpcleanup-actors step 00a: load the source CSV(s) into staging.
#
# Implemented as a shell wrapper (not a plain .sql) because psql's \copy takes its
# whole argument line literally -- it does NOT expand psql variables -- so the file
# path can't be parameterised from inside a .sql. We pipe each CSV via STDIN
# instead, which works regardless of where the file lives client-side.
#
# Input: ISS1712_DPCleanup.csv and/or ISS1951_DPCleanup.csv -- identical shape,
# header row, columns in this order:
#   DialogId,DialogActivityId,Timestamp,ActorId,ActorName,ActivityType
#   "<dialog-uuid>","<activity-uuid>","2026-01-09T08:46:52.4370000Z",
#       "urn:altinn:person:identifier-no:<redacted>","<LAST FIRST MIDDLE>","CorrespondenceOpened"
# The ISO-8601 Timestamp parses straight into timestamptz and equals
# DialogActivity."CreatedAt" exactly (verified). The ActorId (a national identity
# number) and ActorName are personal data -- do not paste real rows into the repo.
# Names may contain non-ASCII, so we force UTF-8 on the client side.
#
# Connection: same convention as repair.sh -- set PG_CONNECTION_STRING (password in
# ~/.pgpass), or fall back to PG* env vars. Run 00_setup.sql first.
#
# Accepts one OR MORE CSV paths. Staging is TRUNCATEd once up front, then every
# path is appended -- so you can load both files in one combined run, or run this
# once per dataset for the sequential warm-up (ISS-1712 first, then ISS-1951).
#
# Usage:
#   ./00a-load-csv.sh /mnt/backfilldata/corr-1712/ISS1712_DPCleanup.csv
#   ./00a-load-csv.sh /mnt/.../ISS1712_DPCleanup.csv /mnt/.../ISS1951_DPCleanup.csv
#   CSV_PATH=/some/where/file.csv ./00a-load-csv.sh

set -euo pipefail

# Names contain non-ASCII; make the client encoding explicit regardless of locale.
export PGCLIENTENCODING=UTF8

if [[ "$#" -gt 0 ]]; then
    CSV_PATHS=("$@")
else
    CSV_PATHS=("${CSV_PATH:-/mnt/backfilldata/corr-1712/ISS1712_DPCleanup.csv}")
fi
PG_CONNECTION_STRING="${PG_CONNECTION_STRING:-}"

for p in "${CSV_PATHS[@]}"; do
    if [[ ! -r "${p}" ]]; then
        echo "ERROR: CSV not readable: ${p}" >&2
        exit 2
    fi
done

PSQL_ARGS=(-X -v ON_ERROR_STOP=1)
if [[ -n "${PG_CONNECTION_STRING}" ]]; then
    PSQL_ARGS+=(-d "${PG_CONNECTION_STRING}")
fi
psql_run() { psql "${PSQL_ARGS[@]}" "$@"; }

STG='maintenance."Iss1712DpcleanupActors_Staging"'
COLS='("dialog_id","dialog_activity_id","source_ts","actor_id","actor_name","activity_type")'

echo "Truncating staging"
psql_run -c "TRUNCATE ${STG};"
for p in "${CSV_PATHS[@]}"; do
    echo "Loading CSV from ${p}"
    psql_run -c "\copy ${STG} ${COLS} FROM STDIN WITH (FORMAT csv, HEADER true)" < "${p}"
done

# Normalize legacy organization urns to Dialogporten's canonical scheme. The CSV
# carries org performers as Correspondence's urn:altinn:organizationnumber:<orgno>,
# but Dialogporten only knows urn:altinn:organization:identifier-no:<orgno>
# (NorwegianOrganizationIdentifier.Prefix). Rewriting here -- before resolution --
# means orgs match existing ActorName rows and we never create rows in a foreign
# scheme. (Persons already arrive as urn:altinn:person:identifier-no, unchanged.)
echo "Normalizing legacy organizationnumber urns -> organization:identifier-no"
psql_run -c "UPDATE ${STG} SET \"actor_id\" = 'urn:altinn:organization:identifier-no:' || split_part(\"actor_id\", ':', 4) WHERE \"actor_id\" LIKE 'urn:altinn:organizationnumber:%';"

# Helper index for the candidate-build (01) chunked range scans and the resolution
# (00b) joins. Built AFTER the load so the bulk COPY isn't slowed by index
# maintenance. NB: no (actor_id, actor_name) index -- resolution scans staging once
# with DISTINCT and the build keys on dialog_activity_id, so it would not pay for
# itself at 140M rows.
echo "Building dialog_activity_id index + ANALYZE (heavy at ISS-1951 scale)"
psql_run -c "CREATE INDEX IF NOT EXISTS \"IX_Iss1712DpcleanupActors_Staging_ActivityId\" ON ${STG} (\"dialog_activity_id\");"
psql_run -c "ANALYZE ${STG};"

# Report what landed. Confirm only the two expected activity types appear and the
# actor urns look like persons (the bug is recipient-defaulted person performers).
psql_run <<'SQL'
\echo ''
\echo '-- Rows loaded:'
SELECT COUNT(*) AS staging_rows FROM maintenance."Iss1712DpcleanupActors_Staging";

\echo ''
\echo '-- Distinct activity_type values (expect only CorrespondenceOpened / CorrespondenceConfirmed):'
SELECT "activity_type", COUNT(*) AS rows
FROM maintenance."Iss1712DpcleanupActors_Staging"
GROUP BY "activity_type"
ORDER BY rows DESC;

\echo ''
\echo '-- Distinct actor urn prefixes (sanity-check the identifier scheme):'
SELECT split_part("actor_id", ':', 1) || ':' || split_part("actor_id", ':', 2)
         || ':' || split_part("actor_id", ':', 3) || ':' || split_part("actor_id", ':', 4)
         AS actor_prefix,
       COUNT(*) AS rows
FROM maintenance."Iss1712DpcleanupActors_Staging"
GROUP BY actor_prefix
ORDER BY rows DESC;
SQL
