#!/usr/bin/env bash
#
# iss1612-dpcleanup-labels: resolve an actor name for every distinct actor urn in
# the staged CSV, filling maintenance."Iss1612DpcleanupLabels_ActorNames".
#
# Resolution order, mirroring how the application would label the actor:
#   1. 'existing' -- reuse an ActorName row already present for this actor urn
#                    (latest non-null name), so we don't create a duplicate.
#   2. 'brreg'    -- otherwise look the org name up in the Brønnøysund register
#                    (field "navn"), trying /enheter then /underenheter.
#   3. 'unresolved' -- neither found; those candidates are skipped & reported
#                    later (00b / 01 leave them out).
#
# This is a PREP step: it writes the resolution table and (only the 'existing'
# linkage) reads public."ActorName". It does NOT yet create any ActorName rows --
# 00b_ensure_actornames.sql does that. Run AFTER 00a_load_csv.sql.
#
# Connection: same convention as repair.sh -- set PG_CONNECTION_STRING (libpq URI,
# password in ~/.pgpass), or fall back to PG* env vars.
#
# Usage:
#   export PG_CONNECTION_STRING='postgresql://repair_user@host:5432/dialogporten?sslmode=require'
#   ./resolve-actornames.sh

set -euo pipefail

PG_CONNECTION_STRING="${PG_CONNECTION_STRING:-}"
BRREG_BASE="${BRREG_BASE:-https://data.brreg.no/enhetsregisteret/api}"
ORG_PREFIX='urn:altinn:organization:identifier-no:'

command -v jq   >/dev/null || { echo "ERROR: jq is required" >&2; exit 2; }
command -v curl >/dev/null || { echo "ERROR: curl is required" >&2; exit 2; }

PSQL_ARGS=(-X -v ON_ERROR_STOP=1)
if [[ -n "${PG_CONNECTION_STRING}" ]]; then
    PSQL_ARGS+=(-d "${PG_CONNECTION_STRING}")
fi
psql_run() { psql "${PSQL_ARGS[@]}" "$@"; }

echo "== 1. Seed resolution table + reuse existing ActorName rows =="
psql_run <<'SQL'
-- One row per distinct actor urn from the staged CSV.
INSERT INTO maintenance."Iss1612DpcleanupLabels_ActorNames" ("actor_id")
SELECT DISTINCT "actor_id" FROM maintenance."Iss1612DpcleanupLabels_Staging"
ON CONFLICT ("actor_id") DO NOTHING;

-- Reuse an existing ActorName (latest non-null name) for the same actor urn,
-- matching how PopulateActorNameInterceptor dedupes on (ActorId, Name).
WITH existing AS (
    SELECT DISTINCT ON (an."ActorId")
           an."ActorId", an."Id", an."Name"
    FROM public."ActorName" an
    WHERE an."ActorId" IN (SELECT "actor_id"
                           FROM maintenance."Iss1612DpcleanupLabels_ActorNames")
      AND an."Name" IS NOT NULL
    ORDER BY an."ActorId", an."CreatedAt" DESC
)
UPDATE maintenance."Iss1612DpcleanupLabels_ActorNames" r
SET "name"                 = e."Name",
    "actor_name_entity_id" = e."Id",
    "source"               = 'existing'
FROM existing e
WHERE r."actor_id" = e."ActorId"
  AND r."source" IS DISTINCT FROM 'existing';
SQL

# --- 2. brreg lookup for whatever is still unresolved ---------------------------
tmp_csv="$(mktemp)"
trap 'rm -f "$tmp_csv"' EXIT

mapfile -t pending < <(psql_run -A -t -c \
    "SELECT \"actor_id\" FROM maintenance.\"Iss1612DpcleanupLabels_ActorNames\" WHERE \"source\" IS NULL ORDER BY \"actor_id\";")

echo "== 2. brreg lookup for ${#pending[@]} unresolved actor(s) =="
brreg_hits=0
for actor in "${pending[@]}"; do
    [[ -z "$actor" ]] && continue
    # Only organization urns are resolvable via brreg.
    if [[ "$actor" != "${ORG_PREFIX}"* ]]; then
        continue
    fi
    orgnr="${actor##*:}"
    if [[ ! "$orgnr" =~ ^[0-9]{9}$ ]]; then
        continue
    fi

    navn=""
    for kind in enheter underenheter; do
        resp="$(curl -fsS "${BRREG_BASE}/${kind}/${orgnr}" 2>/dev/null || true)"
        if [[ -n "$resp" ]]; then
            navn="$(printf '%s' "$resp" | jq -r '.navn // empty')"
            [[ -n "$navn" ]] && break
        fi
    done

    if [[ -n "$navn" ]]; then
        jq -rn --arg a "$actor" --arg n "$navn" '[$a,$n]|@csv' >> "$tmp_csv"
        brreg_hits=$((brreg_hits + 1))
    fi
done
echo "   brreg resolved ${brreg_hits} name(s)"

# Bulk-load the brreg results and apply them (CSV avoids any SQL-escaping issues).
if [[ -s "$tmp_csv" ]]; then
    psql_run <<SQL
CREATE TEMP TABLE _brreg_tmp ("actor_id" text, "name" text) ON COMMIT DROP;
\copy _brreg_tmp ("actor_id","name") FROM '${tmp_csv}' WITH (FORMAT csv)
UPDATE maintenance."Iss1612DpcleanupLabels_ActorNames" r
SET "name"   = t."name",
    "source" = 'brreg'
FROM _brreg_tmp t
WHERE r."actor_id" = t."actor_id"
  AND t."name" IS NOT NULL
  AND r."source" IS NULL;
SQL
fi

# --- 3. Anything still without a source is unresolved; report the breakdown ----
echo "== 3. Summary =="
psql_run <<'SQL'
UPDATE maintenance."Iss1612DpcleanupLabels_ActorNames"
SET "source" = 'unresolved'
WHERE "source" IS NULL;

SELECT COALESCE("source", '(null)') AS source, COUNT(*) AS actors
FROM maintenance."Iss1612DpcleanupLabels_ActorNames"
GROUP BY "source"
ORDER BY "source";
SQL
echo "Done. Next: 00b_ensure_actornames.sql, then 01_build_candidates.sql."
