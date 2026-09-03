#!/usr/bin/env bash
# =========================================================================
# dialog-created-counts.sh — count dialogs created per day/org/resource
# WITHOUT a CreatedAt index and WITHOUT hurting the database.
#
# Two modes
#   createdat (default)
#     "How many dialogs in the database have CreatedAt in month M?" Counts per
#     Org using IX_Dialog_Org_CreatedAt_Id, which is index-only for
#     "Org" = x AND "CreatedAt" in range. Includes everything, migrated data
#     too. Org list comes from a skip scan. No resource breakdown (the index
#     lacks ServiceResource).
#   id
#     Dialog."Id" is UUIDv7, so its first 48 bits are the creation time in
#     milliseconds. A range on "Id" is a range on creation time that the
#     covering index on Id serves index-only, INCLUDING ServiceResource and
#     Org, so this mode gives a per-resource breakdown. It holds for every
#     dialog EXCEPT migrated correspondence (*-migratedcorrespondence-*), whose
#     Id carries the migration time, not the backdated CreatedAt; the report
#     lists those separately.
#   Month boundaries are UTC midnight in both modes.
#
#   The range is walked in chunks, one psql statement per chunk, each under a
#   15 s statement_timeout. A chunk that times out is split in half and
#   retried, down to a 2-minute minimum. Every successful chunk is appended to
#   a local CSV together with a coverage record, so the run can be interrupted
#   and resumed; already-covered chunks are skipped. Nothing is written to the
#   database and no statement holds a snapshot for more than 15 s.
#
# Usage
#   ./dialog-created-counts.sh [--by createdat|id] ENV FROM TO [OUTDIR]
#     ENV     test | yt01 | staging | prod   (tunnel must be up, see forward.sh)
#     FROM/TO dates on month boundaries, TO exclusive, e.g. 2024-05-01 2024-09-01
#     OUTDIR  where results land (default ./dialog-counts-ENV-MODE)
#   Then:
#   ./dialog-created-counts.sh report OUTDIR      # monthly totals via sqlite3
#
# Requires: psql, python3, sqlite3 (all present on macOS), and an active PIM
# activation for staging/prod. Runs as the Entra readonly group, so it is
# fully audited and attributed to you.
# =========================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STMT_TIMEOUT="${STMT_TIMEOUT:-15s}"   # override for testing only
MIN_CHUNK_MS=$((2 * 60 * 1000))       # never split below 2 minutes
START_CHUNK_MS=$((24 * 60 * 60 * 1000)) # start at one day
PAUSE=0.2                             # seconds between statements

env_port() {
    case "$1" in
        test) echo 25432 ;; yt01) echo 35432 ;; staging) echo 45432 ;; prod) echo 55432 ;;
        *) echo "unknown env: $1" >&2; exit 2 ;;
    esac
}
env_group() {
    case "$1" in
        test|yt01) echo altinn-dialogporten-test-postgresql-readonly ;;
        staging|prod) echo altinn-dialogporten-prod-postgresql-readonly ;;
    esac
}

to_ms() { python3 -c "import datetime,sys; d=datetime.datetime.fromisoformat(sys.argv[1]).replace(tzinfo=datetime.timezone.utc); print(int(d.timestamp()*1000))" "$1"; }
fmt_ms() { python3 -c "import datetime,sys; print(datetime.datetime.fromtimestamp(int(sys.argv[1])/1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M'))" "$1"; }

# ---------------------------------------------------------------- report ---
if [[ "${1:-}" == "report" ]]; then
    OUT="${2:?OUTDIR required}"
    DB="$OUT/counts.sqlite"
    rm -f "$DB"
    MODE=$(cat "$OUT/mode" 2>/dev/null || echo id)
    if [[ "$MODE" == id ]]; then
        sqlite3 "$DB" <<SQL
CREATE TABLE r(from_ms INTEGER, to_ms INTEGER, org TEXT, service_resource TEXT, n INTEGER);
.mode csv
.import '$OUT/results.csv' r
CREATE TABLE cov(org TEXT, from_ms INTEGER, to_ms INTEGER, seconds REAL);
.import '$OUT/coverage.csv' cov
SQL
    else
        sqlite3 "$DB" <<SQL
CREATE TABLE r(from_ms INTEGER, to_ms INTEGER, org TEXT, n INTEGER);
.mode csv
.import '$OUT/results.csv' r
CREATE TABLE cov(org TEXT, from_ms INTEGER, to_ms INTEGER, seconds REAL);
.import '$OUT/coverage.csv' cov
SQL
    fi
    echo "Coverage: $(sqlite3 "$DB" "SELECT count(*) || ' chunks, ' || coalesce(round(sum(seconds)),0) || ' s total, longest ' || coalesce(round(max(seconds),1),0) || ' s' FROM cov")"
    sqlite3 "$DB" "CREATE TABLE failed(org TEXT, from_ms INTEGER, to_ms INTEGER, at TEXT)"
    [[ -s "$OUT/failed.csv" ]] && sqlite3 "$DB" ".mode csv" ".import '$OUT/failed.csv' failed"
    # a failed leaf only matters if no later (resumed) chunk covers it
    STILL_FAILED=$(sqlite3 "$DB" "SELECT count(*) FROM failed f WHERE NOT EXISTS (SELECT 1 FROM cov c WHERE c.org = f.org AND c.from_ms <= f.from_ms AND c.to_ms >= f.to_ms)")
    if [[ "$STILL_FAILED" != "0" ]]; then
        echo "WARNING: $STILL_FAILED chunk(s) failed even at the minimum size and are NOT covered; months below are incomplete:"
        sqlite3 -column "$DB" "SELECT DISTINCT org, at FROM failed f WHERE NOT EXISTS (SELECT 1 FROM cov c WHERE c.org = f.org AND c.from_ms <= f.from_ms AND c.to_ms >= f.to_ms) ORDER BY 2"
    fi
    echo
    if [[ "$MODE" == id ]]; then
        echo "Dialogs created per month (by Id time). 'alle' excludes migrated correspondence,"
        echo "which is listed separately because its Id time is the migration time, not CreatedAt."
        sqlite3 -header -column "$DB" "
SELECT strftime('%Y-%m', from_ms/1000, 'unixepoch')                                         AS month,
       sum(CASE WHEN service_resource NOT LIKE '%migratedcorrespondence%' THEN n ELSE 0 END) AS alle,
       sum(CASE WHEN service_resource GLOB '*_a2-*'                        THEN n ELSE 0 END) AS a2_skjema,
       sum(CASE WHEN service_resource LIKE '%migratedcorrespondence%'      THEN n ELSE 0 END) AS migrert_korrespondanse
FROM r GROUP BY 1 ORDER BY 1;"
    else
        echo "Dialogs in the database with CreatedAt in the month (UTC), all resources, migrated data included:"
        sqlite3 -header -column "$DB" "SELECT strftime('%Y-%m', from_ms/1000, 'unixepoch') AS month, sum(n) AS dialogs, count(DISTINCT org) AS orgs FROM r GROUP BY 1 ORDER BY 1;"
        echo
        echo "Top orgs over the whole range:"
        sqlite3 -header -column "$DB" "SELECT org, sum(n) AS dialogs FROM r GROUP BY 1 ORDER BY 2 DESC LIMIT 10;"
    fi
    echo
    echo "Detail is in $DB (table r), e.g. per org per month:"
    echo "  sqlite3 -header -column $DB \"SELECT strftime('%Y-%m', from_ms/1000,'unixepoch') m, org, sum(n) FROM r GROUP BY 1,2 ORDER BY 1,3 DESC\""
    exit 0
fi

# ------------------------------------------------------------------ run -----
MODE=createdat
if [[ "${1:-}" == "--by" ]]; then MODE="${2:?mode}"; shift 2; fi
[[ "$MODE" == createdat || "$MODE" == id ]] || { echo "--by must be createdat or id" >&2; exit 2; }
ENV="${1:?ENV required (test|yt01|staging|prod)}"
FROM="${2:?FROM date required}"
TO="${3:?TO date required (exclusive)}"
OUT="${4:-./dialog-counts-$ENV-$MODE}"
mkdir -p "$OUT"
touch "$OUT/results.csv" "$OUT/coverage.csv" "$OUT/failed.csv"
if [[ -s "$OUT/mode" && "$(cat "$OUT/mode")" != "$MODE" ]]; then echo "$OUT was produced with --by $(cat "$OUT/mode"); use another OUTDIR" >&2; exit 2; fi
echo "$MODE" > "$OUT/mode"

PORT=$(env_port "$ENV"); GROUP=$(env_group "$ENV")
nc -z localhost "$PORT" 2>/dev/null || { echo "No tunnel on localhost:$PORT. Start it: ./forward.sh -e $ENV -t postgres" >&2; exit 1; }
export PGPASSWORD; PGPASSWORD="$("$SCRIPT_DIR/pg-token.sh" "$ENV")"
CONN="host=localhost port=$PORT dbname=dialogporten user=$GROUP sslmode=require"

# One chunk = one psql statement. Returns 0 ok, 3 timeout, other = hard error.
CUR_ORG=""   # createdat mode: the org being walked
chunk_sql() {
    local lo=$1 hi=$2
    if [[ "$MODE" == id ]]; then
        cat <<SQL
/*+ IndexOnlyScan("Dialog" "IX_Dialog_Id_Covering_V2") */
SELECT $lo, $hi, "Org", "ServiceResource", count(*)
FROM public."Dialog"
WHERE "Id" >= (lpad(to_hex(${lo}::bigint),12,'0') || '00007000800000000000')::uuid
  AND "Id" <  (lpad(to_hex(${hi}::bigint),12,'0') || '00007000800000000000')::uuid
GROUP BY 3, 4
SQL
    else
        cat <<SQL
SELECT $lo, $hi, '$CUR_ORG', count(*)
FROM public."Dialog"
WHERE "Org" = '$CUR_ORG'
  AND "CreatedAt" >= to_timestamp(${lo}/1000.0)
  AND "CreatedAt" <  to_timestamp(${hi}/1000.0)
SQL
    fi
}
run_chunk() {
    local lo=$1 hi=$2 err
    err=$(mktemp)
    if psql "$CONN" -X -q -At -F, -v ON_ERROR_STOP=1 \
        -c "SET statement_timeout = '$STMT_TIMEOUT'" \
        -c "$(chunk_sql "$lo" "$hi")" > "$OUT/.chunk.tmp" 2>"$err"; then
        rm -f "$err"; return 0
    fi
    if grep -q "statement timeout" "$err"; then rm -f "$err"; return 3; fi
    cat "$err" >&2; rm -f "$err"; return 1
}

covered() { grep -q "^$CUR_ORG,$1,$2," "$OUT/coverage.csv"; }

# Recursive: try the interval, split on timeout.
walk() {
    local lo=$1 hi=$2 t0 t1 rc
    if covered "$lo" "$hi"; then return 0; fi
    t0=$(date +%s.%N)
    rc=0; run_chunk "$lo" "$hi" || rc=$?
    t1=$(date +%s.%N)
    if [[ $rc -eq 0 ]]; then
        cat "$OUT/.chunk.tmp" >> "$OUT/results.csv"
        echo "$CUR_ORG,$lo,$hi,$(python3 -c "print(round($t1-$t0,2))")" >> "$OUT/coverage.csv"
        printf '  ok   %-7s %s -> %s  (%ss, %s rows)\n' "$CUR_ORG" "$(fmt_ms "$lo")" "$(fmt_ms "$hi")" "$(python3 -c "print(round($t1-$t0,1))")" "$(wc -l < "$OUT/.chunk.tmp" | tr -d ' ')"
        sleep "$PAUSE"
    elif [[ $rc -eq 3 ]]; then
        if (( hi - lo <= MIN_CHUNK_MS )); then
            echo "$CUR_ORG,$lo,$hi,$(fmt_ms "$lo")" >> "$OUT/failed.csv"
            printf '  FAIL %s -> %s  timed out even at minimum chunk\n' "$(fmt_ms "$lo")" "$(fmt_ms "$hi")"
            return 0
        fi
        printf '  split %s -> %s  (timeout)\n' "$(fmt_ms "$lo")" "$(fmt_ms "$hi")"
        local mid=$(( lo + (hi - lo) / 2 ))
        walk "$lo" "$mid"; walk "$mid" "$hi"
    else
        echo "hard error, aborting" >&2; exit 1
    fi
}

FROM_MS=$(to_ms "$FROM"); TO_MS=$(to_ms "$TO")
echo "Counting dialogs created $FROM .. $TO (exclusive) on $ENV as $GROUP, mode=$MODE"
echo "statement_timeout=$STMT_TIMEOUT per chunk; results in $OUT (resumable)"
if [[ "$MODE" == createdat ]]; then
    # distinct orgs via skip scan (cheap), then per month x org, whole month first, split on timeout
    ORGS=$(psql "$CONN" -X -q -At -c "SET statement_timeout='$STMT_TIMEOUT'" -c "WITH RECURSIVE o AS ((SELECT \"Org\" FROM public.\"Dialog\" ORDER BY 1 LIMIT 1) UNION ALL SELECT n.\"Org\" FROM o CROSS JOIN LATERAL (SELECT \"Org\" FROM public.\"Dialog\" d WHERE d.\"Org\" > o.\"Org\" ORDER BY 1 LIMIT 1) n) SELECT \"Org\" FROM o" | grep -v '^SET$')
    echo "$(echo "$ORGS" | wc -l | tr -d ' ') orgs"
    cur=$FROM_MS
    while (( cur < TO_MS )); do
        nxt=$(python3 -c "import datetime as d; t=d.datetime.fromtimestamp($cur/1000,d.timezone.utc); m=(t.replace(day=1)+d.timedelta(days=32)).replace(day=1,hour=0,minute=0,second=0,microsecond=0); print(min(int(m.timestamp()*1000),$TO_MS))")
        for CUR_ORG in $ORGS; do walk "$cur" "$nxt"; done
        cur=$nxt
    done
else
    cur=$FROM_MS
    while (( cur < TO_MS )); do
        nxt=$(( cur + START_CHUNK_MS )); (( nxt > TO_MS )) && nxt=$TO_MS
        walk "$cur" "$nxt"
        cur=$nxt
    done
fi
rm -f "$OUT/.chunk.tmp"
echo "Done. Now: $0 report $OUT"
