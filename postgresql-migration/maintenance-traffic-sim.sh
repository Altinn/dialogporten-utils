#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# maintenance-traffic-sim.sh
#
# Simulates sustained traffic against Dialogporten APIM endpoints to validate
# maintenance mode behavior during migration cutover.
#
# Hits all three APIs behind APIM:
#   - ServiceOwner (SO) API: POST + GET (writes are the critical path)
#   - EndUser (EU) API: GET search
#   - GraphQL API: query
#
# Traffic pattern: burst phase, then steady-state polling.
# Use --bypass to add the maintenance bypass header (validates operator access).
#
# Prerequisites:
#   - Bearer tokens for SO and EU (generate via AltinnTestTools)
#   - curl and jq
#
# Usage:
#   ./maintenance-traffic-sim.sh \
#     --so-token "eyJ..." \
#     --eu-token "eyJ..." \
#     [--base-url "https://platform.yt01.altinn.cloud/dialogporten"] \
#     [--party "urn:altinn:person:identifier-no:08895699684"] \
#     [--service-resource "urn:altinn:resource:super-simple-service"] \
#     [--burst-count 5] \
#     [--steady-interval 10] \
#     [--duration 0] \
#     [--bypass "my-secret-bypass-value"] \
#     [--bypass-header "x-dialogporten-maintenance-bypass"] \
#     [--cleanup]
# =============================================================================

# --- Defaults ----------------------------------------------------------------
SO_TOKEN=""              # ServiceOwner bearer token (--so-token, required)
EU_TOKEN=""              # EndUser bearer token (--eu-token, required unless --purge-all)
BASE_URL="https://platform.yt01.altinn.cloud/dialogporten"  # APIM base URL for Dialogporten
GQL_PATH="/graphql"      # GraphQL endpoint path relative to BASE_URL (--graphql-path)
PARTY="urn:altinn:person:identifier-no:08895699684"          # Party URN used for dialog creation and EU search
SERVICE_RESOURCE="urn:altinn:resource:ttd-dialogporten-performance-test-01"  # Service resource for dialog creation
EXTERNAL_REF="maintenance-traffic-sim"  # Stored in externalReference and title for identification
BURST_COUNT=5            # Number of rapid iterations per burst phase
BURST_INTERVAL=120       # Seconds between periodic bursts during steady-state (0 = no periodic bursts)
STEADY_INTERVAL=10       # Seconds between steady-state iterations
DURATION=0               # Total run duration in seconds (0 = run until Ctrl+C)
BYPASS_VALUE=""          # APIM maintenance bypass secret (--bypass, empty = no bypass header sent)
BYPASS_HEADER="x-dialogporten-maintenance-bypass"            # Header name for maintenance bypass
CLEANUP=false            # Purge dialogs created in this session on exit (--cleanup)
PURGE_ALL=false          # Purges only dialogs created by this script (matched by title prefix)

# Track created dialog IDs for cleanup
CREATED_DIALOG_IDS=()

# --- Colors / formatting -----------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
DIM='\033[2m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# --- Parse arguments ---------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --so-token)       SO_TOKEN="$2"; shift 2 ;;
        --eu-token)       EU_TOKEN="$2"; shift 2 ;;
        --base-url)       BASE_URL="$2"; shift 2 ;;
        --party)          PARTY="$2"; shift 2 ;;
        --service-resource) SERVICE_RESOURCE="$2"; shift 2 ;;
        --burst-count)    BURST_COUNT="$2"; shift 2 ;;
        --burst-interval) BURST_INTERVAL="$2"; shift 2 ;;
        --steady-interval) STEADY_INTERVAL="$2"; shift 2 ;;
        --duration)       DURATION="$2"; shift 2 ;;
        --bypass)         BYPASS_VALUE="$2"; shift 2 ;;
        --bypass-header)  BYPASS_HEADER="$2"; shift 2 ;;
        --graphql-path)   GQL_PATH="$2"; shift 2 ;;
        --cleanup)        CLEANUP=true; shift ;;
        --purge-all)      PURGE_ALL=true; shift ;;
        -h|--help)
            sed -n '/^# Usage:/,/^# ====/p' "$0" | head -n -1 | sed 's/^# //'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# --- Validate ----------------------------------------------------------------
if [[ -z "$SO_TOKEN" ]]; then
    echo -e "${RED}ERROR:${NC} --so-token is required (ServiceOwner bearer token)"
    exit 1
fi
if [[ "$PURGE_ALL" != true && -z "$EU_TOKEN" ]]; then
    echo -e "${RED}ERROR:${NC} --eu-token is required (EndUser bearer token)"
    exit 1
fi

for cmd in curl jq; do
    if ! command -v "$cmd" &>/dev/null; then
        echo -e "${RED}ERROR:${NC} '$cmd' is required but not found"
        exit 1
    fi
done

# Strip trailing slash from base URL
BASE_URL="${BASE_URL%/}"

# --- Build common headers ----------------------------------------------------
build_bypass_header() {
    if [[ -n "$BYPASS_VALUE" ]]; then
        echo "-H" "${BYPASS_HEADER}: ${BYPASS_VALUE}"
    fi
}

# --- API call helpers --------------------------------------------------------
ITERATION=0
SO_SEARCH_OK=0; SO_SEARCH_FAIL=0
SO_CREATE_OK=0; SO_CREATE_FAIL=0
EU_SEARCH_OK=0; EU_SEARCH_FAIL=0
GQL_OK=0; GQL_FAIL=0

log_result() {
    local api="$1" method="$2" status="$3" duration_ms="$4" detail="${5:-}"
    local ts
    ts=$(date '+%H:%M:%S')

    if [[ "$status" -ge 200 && "$status" -lt 300 ]]; then
        echo -e "${DIM}${ts}${NC} ${GREEN}${status}${NC} ${BOLD}${api}${NC} ${method} ${DIM}(${duration_ms}ms)${NC} ${detail}"
    elif [[ "$status" -eq 503 || "$status" -eq 429 ]]; then
        # Expected when maintenance mode is active
        echo -e "${DIM}${ts}${NC} ${YELLOW}${status}${NC} ${BOLD}${api}${NC} ${method} ${DIM}(${duration_ms}ms)${NC} ${YELLOW}← maintenance mode${NC} ${detail}"
    else
        echo -e "${DIM}${ts}${NC} ${RED}${status}${NC} ${BOLD}${api}${NC} ${method} ${DIM}(${duration_ms}ms)${NC} ${detail}"
    fi
}

# SO: Create dialog (POST) — the critical write path
so_create_dialog() {
    local payload
    payload=$(cat <<EOF
{
    "serviceResource": "${SERVICE_RESOURCE}",
    "party": "${PARTY}",
    "externalReference": "${EXTERNAL_REF}",
    "status": "New",
    "progress": 10,
    "content": {
        "title": {
            "value": [
                { "languageCode": "nb", "value": "maintenance-traffic-sim ${ITERATION}" },
                { "languageCode": "en", "value": "maintenance-traffic-sim ${ITERATION}" }
            ]
        },
        "summary": {
            "value": [
                { "languageCode": "nb", "value": "Testdialog opprettet av maintenance-traffic-sim.sh for migreringvalidering." },
                { "languageCode": "en", "value": "Test dialog created by maintenance-traffic-sim.sh for migration validation." }
            ]
        }
    }
}
EOF
)

    local start_ms http_code body duration_ms
    start_ms=$(date +%s%N)

    local bypass_args=()
    if [[ -n "$BYPASS_VALUE" ]]; then
        bypass_args=(-H "${BYPASS_HEADER}: ${BYPASS_VALUE}")
    fi

    local tmpfile
    tmpfile=$(mktemp)

    http_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${SO_TOKEN}" \
        -H "Content-Type: application/json" \
        "${bypass_args[@]+"${bypass_args[@]}"}" \
        -d "$payload" \
        "${BASE_URL}/api/v1/serviceowner/dialogs" 2>&1) || true

    duration_ms=$(( ($(date +%s%N) - start_ms) / 1000000 ))
    body=$(cat "$tmpfile" 2>/dev/null || true)
    rm -f "$tmpfile"

    # Try to extract dialog ID from response body
    local dialog_id=""
    if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
        # Try JSON .id first
        dialog_id=$(echo "$body" | jq -r '.id // empty' 2>/dev/null || true)
        if [[ -z "$dialog_id" ]]; then
            # Body might be a bare UUID string
            dialog_id=$(echo "$body" | tr -d '"' | grep -oE '[0-9a-f-]{36}' | head -1 || true)
        fi
        if [[ -n "$dialog_id" ]]; then
            CREATED_DIALOG_IDS+=("$dialog_id")
        fi
        ((SO_CREATE_OK++)) || true
        log_result "SO" "POST /dialogs" "$http_code" "$duration_ms" "id=${dialog_id:-unknown}"
    else
        ((SO_CREATE_FAIL++)) || true
        local err_detail
        err_detail=$(echo "$body" | jq -r '.title // .detail // empty' 2>/dev/null | head -1 || true)
        log_result "SO" "POST /dialogs" "$http_code" "$duration_ms" "${err_detail}"
    fi
}

# SO: Search dialogs (GET)
so_search_dialogs() {
    local start_ms http_code duration_ms
    start_ms=$(date +%s%N)

    local bypass_args=()
    if [[ -n "$BYPASS_VALUE" ]]; then
        bypass_args=(-H "${BYPASS_HEADER}: ${BYPASS_VALUE}")
    fi

    local tmpfile
    tmpfile=$(mktemp)

    http_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
        -H "Authorization: Bearer ${SO_TOKEN}" \
        "${bypass_args[@]+"${bypass_args[@]}"}" \
        "${BASE_URL}/api/v1/serviceowner/dialogs?Limit=1" 2>&1) || true

    duration_ms=$(( ($(date +%s%N) - start_ms) / 1000000 ))
    local body detail=""
    body=$(cat "$tmpfile" 2>/dev/null || true)
    rm -f "$tmpfile"

    if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
        ((SO_SEARCH_OK++)) || true
    else
        ((SO_SEARCH_FAIL++)) || true
        detail=$(echo "$body" | jq -r '.title // .detail // empty' 2>/dev/null | head -1 || true)
        [[ -z "$detail" ]] && detail=$(echo "$body" | head -c 200)
    fi
    log_result "SO" "GET  /dialogs" "$http_code" "$duration_ms" "$detail"
}

# EU: Search dialogs (GET)
eu_search_dialogs() {
    local start_ms http_code duration_ms
    start_ms=$(date +%s%N)

    local bypass_args=()
    if [[ -n "$BYPASS_VALUE" ]]; then
        bypass_args=(-H "${BYPASS_HEADER}: ${BYPASS_VALUE}")
    fi

    local tmpfile
    tmpfile=$(mktemp)

    http_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
        -H "Authorization: Bearer ${EU_TOKEN}" \
        "${bypass_args[@]+"${bypass_args[@]}"}" \
        "${BASE_URL}/api/v1/enduser/dialogs?Limit=1&Party=${PARTY}" 2>&1) || true

    duration_ms=$(( ($(date +%s%N) - start_ms) / 1000000 ))
    local body detail=""
    body=$(cat "$tmpfile" 2>/dev/null || true)
    rm -f "$tmpfile"

    if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
        ((EU_SEARCH_OK++)) || true
    else
        ((EU_SEARCH_FAIL++)) || true
        detail=$(echo "$body" | jq -r '.title // .detail // empty' 2>/dev/null | head -1 || true)
        [[ -z "$detail" ]] && detail=$(echo "$body" | head -c 200)
    fi
    log_result "EU" "GET  /dialogs" "$http_code" "$duration_ms" "$detail"
}

# GraphQL: simple query
gql_query() {
    local query_payload
    query_payload=$(jq -n '{
        query: "{ searchDialogs(input: {}) { items { id } } }"
    }')

    local start_ms http_code duration_ms
    start_ms=$(date +%s%N)

    local bypass_args=()
    if [[ -n "$BYPASS_VALUE" ]]; then
        bypass_args=(-H "${BYPASS_HEADER}: ${BYPASS_VALUE}")
    fi

    local tmpfile
    tmpfile=$(mktemp)

    http_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${EU_TOKEN}" \
        -H "Content-Type: application/json" \
        "${bypass_args[@]+"${bypass_args[@]}"}" \
        -d "$query_payload" \
        "${BASE_URL}${GQL_PATH}" 2>&1) || true

    duration_ms=$(( ($(date +%s%N) - start_ms) / 1000000 ))
    local body
    body=$(cat "$tmpfile" 2>/dev/null || true)
    rm -f "$tmpfile"

    local detail=""
    if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
        ((GQL_OK++)) || true
    else
        ((GQL_FAIL++)) || true
        # Try to extract error detail from response
        detail=$(echo "$body" | jq -r '.errors[0].message // .title // .detail // empty' 2>/dev/null | head -1 || true)
        if [[ -z "$detail" ]]; then
            # Fallback: show first 200 chars of raw body
            detail=$(echo "$body" | head -c 200)
        fi
    fi
    log_result "GQL" "POST /graphql " "$http_code" "$duration_ms" "$detail"
}

# --- Cleanup: purge created test dialogs --------------------------------------
cleanup_dialogs() {
    if [[ ${#CREATED_DIALOG_IDS[@]} -eq 0 ]]; then
        echo -e "\n${DIM}No dialogs to clean up.${NC}"
        return
    fi

    echo -e "\n${CYAN}Purging ${#CREATED_DIALOG_IDS[@]} test dialog(s)...${NC}"

    local bypass_args=()
    if [[ -n "$BYPASS_VALUE" ]]; then
        bypass_args=(-H "${BYPASS_HEADER}: ${BYPASS_VALUE}")
    fi

    local purged=0 failed=0
    for id in "${CREATED_DIALOG_IDS[@]}"; do
        local tmpfile
        tmpfile=$(mktemp)

        local http_code
        http_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
            -X POST -d '' \
            -H "Authorization: Bearer ${SO_TOKEN}" \
            "${bypass_args[@]+"${bypass_args[@]}"}" \
            "${BASE_URL}/api/v1/serviceowner/dialogs/${id}/actions/purge" 2>&1) || true

        local body
        body=$(cat "$tmpfile" 2>/dev/null || true)
        rm -f "$tmpfile"

        if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
            ((purged++)) || true
            echo -e "  ${GREEN}Purged${NC} ${id}"
        else
            ((failed++)) || true
            echo -e "  ${RED}Failed (${http_code})${NC} ${id}"
            echo -e "  ${DIM}${body}${NC}"
        fi
    done
    echo -e "${DIM}Cleanup: ${purged} purged, ${failed} failed.${NC}"
}

# --- Purge all dialogs tagged by this script ---------------------------------
purge_all_tagged() {
    echo -e "${BOLD}Searching for dialogs tagged '${EXTERNAL_REF}'...${NC}"

    local bypass_args=()
    if [[ -n "$BYPASS_VALUE" ]]; then
        bypass_args=(-H "${BYPASS_HEADER}: ${BYPASS_VALUE}")
    fi

    # Paginate through all dialogs for this service resource, filter by title client-side
    local all_ids=()
    local continuation=""
    local page=0

    while true; do
        ((page++)) || true
        local search_url="${BASE_URL}/api/v1/serviceowner/dialogs?ServiceResource=${SERVICE_RESOURCE}&Limit=100"
        if [[ -n "$continuation" ]]; then
            search_url="${search_url}&ContinuationToken=${continuation}"
        fi
        echo -e "${DIM}Page ${page}: GET ${search_url}${NC}"

        local tmpfile
        tmpfile=$(mktemp)

        local http_code
        http_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
            -H "Authorization: Bearer ${SO_TOKEN}" \
            "${bypass_args[@]+"${bypass_args[@]}"}" \
            "$search_url" 2>&1) || true

        if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
            echo -e "${RED}Search failed (${http_code})${NC}"
            cat "$tmpfile"
            rm -f "$tmpfile"
            exit 1
        fi

        local body
        body=$(cat "$tmpfile")
        rm -f "$tmpfile"

        # Filter client-side: only dialogs with title starting with our marker
        while IFS= read -r line; do
            [[ -n "$line" ]] && all_ids+=("$line")
        done < <(echo "$body" | jq -r '
            [.items[]? | select(any(.content.title.value[]?; .value | startswith("maintenance-traffic-sim")))] | .[].id
        ' 2>/dev/null)

        # Check for next page
        local has_next
        has_next=$(echo "$body" | jq -r '.hasNextPage // false' 2>/dev/null)
        if [[ "$has_next" != "true" ]]; then
            break
        fi
        continuation=$(echo "$body" | jq -r '.continuationToken // empty' 2>/dev/null)
        if [[ -z "$continuation" ]]; then
            break
        fi
    done

    local ids=("${all_ids[@]+"${all_ids[@]}"}")

    if [[ ${#ids[@]} -eq 0 ]]; then
        echo -e "${DIM}No dialogs found matching '${EXTERNAL_REF}'.${NC}"
        return
    fi

    echo -e "${CYAN}Found ${#ids[@]} dialog(s) to purge.${NC}"

    local purged=0 failed=0
    for id in "${ids[@]}"; do
        local tmpfile
        tmpfile=$(mktemp)

        local purge_code
        purge_code=$(curl -s -o "$tmpfile" -w "%{http_code}" \
            -X POST -d '' \
            -H "Authorization: Bearer ${SO_TOKEN}" \
            "${bypass_args[@]+"${bypass_args[@]}"}" \
            "${BASE_URL}/api/v1/serviceowner/dialogs/${id}/actions/purge" 2>&1) || true

        local body
        body=$(cat "$tmpfile" 2>/dev/null || true)
        rm -f "$tmpfile"

        if [[ "$purge_code" -ge 200 && "$purge_code" -lt 300 ]]; then
            ((purged++)) || true
            echo -e "  ${GREEN}Purged${NC} ${id}"
        else
            ((failed++)) || true
            echo -e "  ${RED}Failed (${purge_code})${NC} ${id}"
            echo -e "  ${DIM}${body}${NC}"
        fi
    done
    echo -e "${BOLD}Done: ${purged} purged, ${failed} failed.${NC}"
}

# --- Run one full iteration --------------------------------------------------
run_iteration() {
    ((ITERATION++)) || true

    # SO write (the critical one)
    so_create_dialog

    # SO read
    so_search_dialogs

    # EU read
    eu_search_dialogs

    # GraphQL read
    gql_query
}

# --- Summary -----------------------------------------------------------------
print_summary() {
    local total_ok=$((SO_SEARCH_OK + SO_CREATE_OK + EU_SEARCH_OK + GQL_OK))
    local total_fail=$((SO_SEARCH_FAIL + SO_CREATE_FAIL + EU_SEARCH_FAIL + GQL_FAIL))

    echo ""
    echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}  Traffic simulation summary${NC}"
    echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
    echo -e "  Iterations:        ${ITERATION}"
    echo -e "  Bypass header:     ${BYPASS_VALUE:+${GREEN}enabled${NC}}${BYPASS_VALUE:-${DIM}disabled${NC}}"
    echo ""
    echo -e "  ${BOLD}SO  create (POST)${NC}  ${GREEN}${SO_CREATE_OK} ok${NC}  ${RED}${SO_CREATE_FAIL} fail${NC}"
    echo -e "  ${BOLD}SO  search (GET)${NC}   ${GREEN}${SO_SEARCH_OK} ok${NC}  ${RED}${SO_SEARCH_FAIL} fail${NC}"
    echo -e "  ${BOLD}EU  search (GET)${NC}   ${GREEN}${EU_SEARCH_OK} ok${NC}  ${RED}${EU_SEARCH_FAIL} fail${NC}"
    echo -e "  ${BOLD}GQL query  (POST)${NC}  ${GREEN}${GQL_OK} ok${NC}  ${RED}${GQL_FAIL} fail${NC}"
    echo ""
    echo -e "  Total:             ${GREEN}${total_ok} ok${NC}  ${RED}${total_fail} fail${NC}"
    echo -e "  Dialogs created:   ${#CREATED_DIALOG_IDS[@]}"
    echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
}

# --- Signal handler ----------------------------------------------------------
on_exit() {
    if [[ "$PURGE_ALL" == true ]]; then
        return
    fi
    print_summary
    if [[ "$CLEANUP" == true ]]; then
        cleanup_dialogs
    elif [[ ${#CREATED_DIALOG_IDS[@]} -gt 0 ]]; then
        echo -e "\n${YELLOW}Tip:${NC} Re-run with ${BOLD}--purge-all --so-token \$SO_TOKEN${NC} to purge all test dialogs later."
    fi
}
trap on_exit EXIT

# --- Purge-all mode (standalone) ---------------------------------------------
if [[ "$PURGE_ALL" == true ]]; then
    purge_all_tagged
    exit 0
fi

# --- Main loop ---------------------------------------------------------------
echo -e "${BOLD}Dialogporten APIM maintenance mode traffic simulator${NC}"
echo -e "${DIM}Base URL:          ${BASE_URL}${NC}"
echo -e "${DIM}Party:             ${PARTY}${NC}"
echo -e "${DIM}Service resource:  ${SERVICE_RESOURCE}${NC}"
echo -e "${DIM}GraphQL path:      ${GQL_PATH}${NC}"
echo -e "${DIM}Burst count:       ${BURST_COUNT}${NC}"
echo -e "${DIM}Burst interval:    ${BURST_INTERVAL}s (0=no periodic bursts)${NC}"
echo -e "${DIM}Steady interval:   ${STEADY_INTERVAL}s${NC}"
echo -e "${DIM}Duration:          ${DURATION}s (0=indefinite)${NC}"
echo -e "${DIM}Bypass header:     ${BYPASS_VALUE:+enabled (${BYPASS_HEADER})}${BYPASS_VALUE:-disabled}${NC}"
echo -e "${DIM}Cleanup on exit:   ${CLEANUP}${NC}"
echo ""

# Phase 1: Burst
echo -e "${CYAN}▶ Burst phase: ${BURST_COUNT} rapid iterations${NC}"
for ((i = 1; i <= BURST_COUNT; i++)); do
    run_iteration
    sleep 0.5
done

# Phase 2: Steady state with periodic bursts
echo ""
echo -e "${CYAN}▶ Steady-state phase: every ${STEADY_INTERVAL}s, burst of ${BURST_COUNT} every ${BURST_INTERVAL}s (Ctrl+C to stop)${NC}"

START_TIME=$(date +%s)
LAST_BURST_TIME=$(date +%s)
while true; do
    if [[ "$DURATION" -gt 0 ]]; then
        elapsed=$(( $(date +%s) - START_TIME ))
        if [[ "$elapsed" -ge "$DURATION" ]]; then
            echo -e "\n${CYAN}Duration limit reached (${DURATION}s). Stopping.${NC}"
            break
        fi
    fi

    # Check if it's time for a periodic burst
    now=$(date +%s)
    since_last_burst=$(( now - LAST_BURST_TIME ))
    if [[ "$BURST_INTERVAL" -gt 0 && "$since_last_burst" -ge "$BURST_INTERVAL" ]]; then
        echo ""
        echo -e "${CYAN}▶ Periodic burst: ${BURST_COUNT} rapid iterations${NC}"
        for ((i = 1; i <= BURST_COUNT; i++)); do
            run_iteration
            sleep 0.5
        done
        LAST_BURST_TIME=$(date +%s)
        echo -e "${CYAN}▶ Resuming steady-state${NC}"
    else
        run_iteration
    fi

    sleep "$STEADY_INTERVAL"
done