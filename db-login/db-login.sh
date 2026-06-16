#!/usr/bin/env bash
# =========================================================================
# Dialogporten DB Login Helper
#
# Helps developers connect to a Dialogporten PostgreSQL database using
# Microsoft Entra ID GROUP-based access across three tiers (read / write /
# admin). You log in AS the group (group name = PG username) with a personal
# Entra token; all activity is audit-logged and attributed to your individual
# identity via the connection-auth log.
#
# This script does NOT create the SSH tunnel — run forward.sh first (it sets
# up the JIT tunnel to localhost:<port>). This script then prepares the token
# and tells you how to connect with your client of choice.
#
# Recommended client: pgAdmin with the auto-refreshing token (pg-token.sh as
# "Password exec command") — set up once, then it self-serves. Rider / psql /
# other are supported via a one-shot token copied to your clipboard.
# =========================================================================
set -uo pipefail

export AZURE_CORE_COLLECT_TELEMETRY=false
export AZURE_CORE_ONLY_SHOW_ERRORS=true

# =========================================================================
# Configuration  ── EDIT HERE when swapping to the real altinn-platform#3407
#                   groups, or when port / path conventions change.
# =========================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PG_TOKEN_SCRIPT="${SCRIPT_DIR}/pg-token.sh"
readonly DB_NAME="dialogporten"

# Canonical env values (match forward.sh). yt01 = perf, lives in the test
# subscription; low focus for now but included for completeness.
readonly VALID_ENVIRONMENTS=("test" "yt01" "staging" "prod")
readonly VALID_TIERS=("read" "write" "admin")

# NOTE: stock macOS ships bash 3.2 (no associative arrays), and forward.sh is
# bash-3.2 compatible, so we use case-based lookups instead of `declare -A`.

# env -> default local tunnel port (must match how you ran forward.sh).
# Scheme: increasing toward prod (prod on the highest, most-distinct port).
env_port() {
  case "$1" in
    test)    echo 15432 ;;
    yt01)    echo 25432 ;;
    staging) echo 35432 ;;
    prod)    echo 45432 ;;
    *)       echo "" ;;
  esac
}

# env:tier -> Entra group name (the PG username you log in as).
# staging currently REUSES the prod groups. This function is the SINGLE swap
# point — replace the names here for the real altinn-platform#3407 groups.
group_for() {
  case "$1:$2" in
    test:read)     echo "Altinn Product Dialogporten: Developers Dev" ;;
    test:write)    echo "Dialogporten-Test-Operations" ;;
    test:admin)    echo "Dialogporten-Test-UserAdmins" ;;
    # yt01 (perf) shares the test subscription and reuses the TEST groups,
    # both now and after #3407 lands.
    yt01:read)     echo "Altinn Product Dialogporten: Developers Dev" ;;
    yt01:write)    echo "Dialogporten-Test-Operations" ;;
    yt01:admin)    echo "Dialogporten-Test-UserAdmins" ;;
    staging:read)  echo "Altinn Product Dialogporten: Developers Prod" ;;
    staging:write) echo "Dialogporten-Prod-Operations" ;;
    staging:admin) echo "Altinn Product Dialogporten: Admins Prod" ;;
    prod:read)     echo "Altinn Product Dialogporten: Developers Prod" ;;
    prod:write)    echo "Dialogporten-Prod-Operations" ;;
    prod:admin)    echo "Altinn Product Dialogporten: Admins Prod" ;;
    *)             echo "" ;;
  esac
}

# =========================================================================
# pgAdmin servers.json generator (--export-pgadmin)
# =========================================================================
# Emits a pgAdmin Import/Export "Servers" JSON to stdout, with one server per
# env:tier in per-env groups, the token exec command wired to this machine's
# pg-token.sh, and no stored passwords (token comes from the exec command).
#
# Import (CLI, works around the GUI dialog bug pgadmin#9972), additive by default:
#   PYBIN=".../python3.13"; SETUP=".../web/setup.py"
#   "$PYBIN" "$SETUP" load-servers servers.json --sqlite-path ~/.pgadmin/pgadmin4.db
#   (add --replace to overwrite same-named servers; default --no-replace is additive)

# Title Case a single lowercase word (bash 3.2 safe; no ${var^}).
title_case() {
  local w=$1
  printf '%s%s' "$(printf '%s' "${w:0:1}" | tr '[:lower:]' '[:upper:]')" "${w:1}"
}

export_pgadmin() {
  local include_admin=$1     # "yes"/"no"
  local exec_cmd="$PG_TOKEN_SCRIPT"
  # tier -> privilege ordinal (read<write<admin) so servers sort least->most.
  local tiers=("read" "write" "admin")
  local n=0 first=1 e t group port ord envtc tiertc

  printf '{\n  "Servers": {\n'
  for e in "${VALID_ENVIRONMENTS[@]}"; do
    for t in "${tiers[@]}"; do
      [ "$t" = "admin" ] && [ "$include_admin" = "no" ] && continue
      group="$(group_for "$e" "$t")"; [ -z "$group" ] && continue
      port="$(env_port "$e")"; [ -z "$port" ] && continue
      case "$t" in read) ord=1 ;; write) ord=2 ;; admin) ord=3 ;; esac
      # yt01 is an env codename, not a word — display it uppercased.
      if [ "$e" = "yt01" ]; then envtc="YT01"; else envtc="$(title_case "$e")"; fi
      tiertc="$(title_case "$t")"
      n=$((n+1))
      [ "$first" -eq 1 ] && first=0 || printf ',\n'
      printf '    "%s": {\n' "$n"
      printf '      "Name": "%s %s %s",\n' "$ord" "$envtc" "$tiertc"
      printf '      "Group": "Dialogporten %s",\n' "$envtc"
      printf '      "Host": "localhost",\n'
      printf '      "Port": %s,\n' "$port"
      printf '      "MaintenanceDB": "%s",\n' "$DB_NAME"
      printf '      "Username": "%s",\n' "$group"
      printf '      "ConnectionParameters": { "sslmode": "require", "connect_timeout": 10 },\n'
      printf '      "PasswordExecCommand": "%s",\n' "$exec_cmd"
      printf '      "PasswordExecExpiration": 3600\n'
      printf '    }'
    done
  done
  printf '\n  }\n}\n'
}

# =========================================================================
# Colors / logging (mirrors forward.sh)
# =========================================================================
BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}ℹ${NC} $1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
log_error()   { echo -e "${RED}✖${NC} $1" >&2; }
log_title()   { echo -e "\n${BOLD}${CYAN}$1${NC}"; }

# Print a formatted box with title and content (ANSI-aware; mirrors forward.sh
# but builds borders with a printf loop instead of `tr`, which mangles the
# multibyte box chars when LANG is unset / C locale).
print_box() {
  local title="$1" content="$2" width=72 padding=2
  get_visible_length() { local str; str=$(printf "%b" "$1" | sed 's/\x1b\[[0-9;]*m//g'); echo "${#str}"; }
  _rule() { local n=$1 i; for ((i=0; i<n; i++)); do printf '─'; done; }
  _row() { # $1 = content (may contain ANSI); pads to width, never negative
    local vlen pad; vlen=$(get_visible_length "$1"); pad=$((width - vlen - padding))
    [ "$pad" -lt 0 ] && pad=0
    printf "│%-${padding}s%b%*s│\n" " " "$1" "$pad" ""
  }
  printf "╭"; _rule "$width"; printf "╮\n"
  _row "$title"
  _row ""
  while IFS= read -r line; do _row "$line"; done <<< "$content"
  printf "╰"; _rule "$width"; printf "╯\n"
}

# locate az
AZ=""
for cand in "${AZ_BIN:-}" /opt/homebrew/bin/az /usr/local/bin/az "$(command -v az 2>/dev/null || true)"; do
  [ -n "$cand" ] && [ -x "$cand" ] && { AZ="$cand"; break; }
done

# =========================================================================
# Prompt helper (numbered select with optional default)
# =========================================================================
prompt_choice() {
  # $1=label, rest=options; echoes the chosen value.
  # Colored prompt + trailing blank line for breathing room.
  local label=$1; shift
  local options=("$@") i sel
  trap 'echo -e "\nCancelled" >&2; exit 130' INT
  echo -e "${BOLD}${label}${NC}" >&2
  for i in "${!options[@]}"; do echo -e "  ${CYAN}$((i+1)))${NC} ${options[$i]}" >&2; done
  while true; do
    read -rp "Select (1-${#options[@]}): " sel
    if [[ "$sel" =~ ^[0-9]+$ ]] && [ "$sel" -ge 1 ] && [ "$sel" -le "${#options[@]}" ]; then
      echo "" >&2          # breathing room after the block
      echo "${options[$((sel-1))]}"; return
    fi
    log_error "Invalid selection."
  done
}

validate_in() { # $1=value $2..=valid set ; returns 0 if member
  local v=$1; shift
  for x in "$@"; do [ "$v" = "$x" ] && return 0; done
  return 1
}

# =========================================================================
# Help
# =========================================================================
print_help() {
  cat <<EOF
Dialogporten DB Login Helper

Prepares a Microsoft Entra token + connection details for a Dialogporten
PostgreSQL database, using group-based access tiers. Run forward.sh FIRST to
establish the SSH tunnel.

Usage: $0 [-e ENV] [-t TIER] [-c CLIENT]
       $0 --export-pgadmin [--no-admin] > servers.json

  -e, --env         ${VALID_ENVIRONMENTS[*]}
  -t, --tier        ${VALID_TIERS[*]}   (read=SELECT, write=DML, admin=DDL)
  -c, --client      pgadmin | rider | psql | raw   (default: prompt; pgadmin recommended)
  --export-pgadmin  Emit a pgAdmin Import/Export servers JSON to stdout (all envs/tiers,
                    per-env groups, token exec command wired to this machine). No prompts.
  --no-admin        With --export-pgadmin: omit the admin/DDL tier servers.
  -h, --help

Interactive when flags are omitted. Mirrors forward.sh's env-first flow.

Import the generated file (CLI; additive by default), e.g.:
  PYBIN="/Applications/pgAdmin 4.app/Contents/Frameworks/Python.framework/Versions/3.13/bin/python3.13"
  SETUP="/Applications/pgAdmin 4.app/Contents/Resources/web/setup.py"
  "\$PYBIN" "\$SETUP" load-servers servers.json --sqlite-path ~/.pgadmin/pgadmin4.db
EOF
}

# =========================================================================
# Main
# =========================================================================
main() {
  local environment="" tier="" client="" export_mode="" include_admin="yes"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -e|--env)         environment="${2:-}"; shift 2 ;;
      -t|--tier)        tier="${2:-}"; shift 2 ;;
      -c|--client)      client="${2:-}"; shift 2 ;;
      --export-pgadmin) export_mode="yes"; shift ;;
      --no-admin)       include_admin="no"; shift ;;
      -h|--help)        print_help; exit 0 ;;
      *) log_error "Unknown option: $1"; print_help; exit 1 ;;
    esac
  done

  # Generator mode: emit servers.json to stdout and exit (no prompts, no az needed).
  if [ "$export_mode" = "yes" ]; then
    export_pgadmin "$include_admin"
    exit 0
  fi

  log_title "Dialogporten DB Login Helper"

  # --- dependencies & identity -------------------------------------------
  [ -z "$AZ" ] && { log_error "Azure CLI (az) not found. Install it or set AZ_BIN."; exit 1; }
  if ! "$AZ" account show >/dev/null 2>&1; then
    log_error "No active Azure session. Run:  az login"
    exit 1
  fi
  local me; me="$("$AZ" account show --query user.name -o tsv 2>/dev/null)"
  log_info "Active Azure identity: ${BOLD}${me}${NC}  (token will be attributed to this user)"
  echo

  # --- environment --------------------------------------------------------
  if [ -z "$environment" ]; then
    environment=$(prompt_choice "Select environment:" "${VALID_ENVIRONMENTS[@]}")
  fi
  validate_in "$environment" "${VALID_ENVIRONMENTS[@]}" || { log_error "Invalid env: $environment"; exit 1; }

  # --- tier ---------------------------------------------------------------
  if [ -z "$tier" ]; then
    tier=$(prompt_choice "Select access tier:" "${VALID_TIERS[@]}")
  fi
  validate_in "$tier" "${VALID_TIERS[@]}" || { log_error "Invalid tier: $tier"; exit 1; }

  local group port
  group="$(group_for "$environment" "$tier")"
  port="$(env_port "$environment")"; [ -z "$port" ] && port=5432
  [ -z "$group" ] && { log_error "No group configured for ${environment}:${tier}"; exit 1; }

  # --- prod guard ---------------------------------------------------------
  if [ "$environment" = "prod" ]; then
    log_warning "You are targeting ${BOLD}PROD${NC}."
    read -rp "Type 'prod' to continue: " c; [ "$c" = "prod" ] || { log_warning "Aborted."; exit 0; }
  fi

  # --- best-effort membership pre-check (terminal CAN show this) ----------
  local my_oid; my_oid="$("$AZ" ad signed-in-user show --query id -o tsv 2>/dev/null || true)"
  if [ -n "$my_oid" ]; then
    local is_member
    is_member="$("$AZ" ad group member check --group "$group" --member-id "$my_oid" --query value -o tsv 2>/dev/null || true)"
    if [ "$is_member" = "false" ]; then
      log_error "Identity '${me}' is NOT a member of group '${group}'."
      log_info  "You are likely on the wrong Azure account for ${environment}. Switch with: az login"
      exit 1
    elif [ "$is_member" = "true" ]; then
      log_success "Membership confirmed: ${me} ∈ ${group}"
    else
      log_warning "Could not verify group membership (continuing; PostgreSQL will enforce it)."
    fi
    echo
  fi

  # --- client selection ---------------------------------------------------
  if [ -z "$client" ]; then
    client=$(prompt_choice "How will you connect? (pgadmin recommended)" "pgadmin" "rider" "psql" "raw")
  fi

  echo
  case "$client" in
    pgadmin) handoff_pgadmin "$port" "$group" "$environment" "$tier" ;;
    rider)   handoff_clipboard "rider" "$port" "$group" ;;
    psql)    handoff_psql "$port" "$group" ;;
    raw)     handoff_clipboard "raw" "$port" "$group" ;;
    *) log_error "Unknown client: $client"; exit 1 ;;
  esac
}

# --- pgAdmin: setup-helper (the recommended path) --------------------------
handoff_pgadmin() {
  local port=$1 group=$2 environment=$3 tier=$4
  local server_name="Dialogporten ${environment} ${tier}"
  log_success "pgAdmin ${GREEN}(recommended)${NC} — set up a server ${BOLD}once${NC}; it then auto-refreshes the token."
  echo
  print_box "${BOLD}pgAdmin — register a new server${NC}  (${environment} / ${tier})" "\
${CYAN}${BOLD}General tab${NC}
  Name           ${BOLD}${server_name}${NC}
  Connect now?   ${YELLOW}Off${NC}  ${YELLOW}(must be disabled to save with no password)${NC}

${CYAN}${BOLD}Connection tab${NC}
  Host                  ${BOLD}localhost${NC}
  Port                  ${BOLD}${port}${NC}
  Maintenance database  ${BOLD}${DB_NAME}${NC}
  Username              ${BOLD}${group}${NC}
  Password              ${YELLOW}(leave blank)${NC}

${CYAN}${BOLD}Advanced tab${NC}
  Password exec expiration (sec)  ${BOLD}3600${NC}
  Password exec command:
    ${BOLD}${PG_TOKEN_SCRIPT}${NC}"
  echo
  log_info "After saving, connect — pgAdmin runs the token script and re-runs it"
  log_info "before expiry, so you won't need to paste tokens again."
}

# --- psql: offer to launch directly ---------------------------------------
handoff_psql() {
  local port=$1 group=$2
  local token; token="$("$PG_TOKEN_SCRIPT" 2>/dev/null || true)"
  [ -z "$token" ] && { log_error "Failed to get token (az login?)."; exit 1; }
  log_info "Launching psql (token valid ~75 min for this session)..."
  PGPASSWORD="$token" PGUSER="$group" \
    psql "host=localhost port=${port} dbname=${DB_NAME} sslmode=require" || true
}

# --- Rider / raw: copy token to clipboard, never echo it -------------------
handoff_clipboard() {
  local kind=$1 port=$2 group=$3
  local token; token="$("$PG_TOKEN_SCRIPT" 2>/dev/null || true)"
  [ -z "$token" ] && { log_error "Failed to get token (az login?)."; exit 1; }
  print_box "${BOLD}Connection details${NC}  (paste token as the password)" "\
  Host      ${BOLD}localhost${NC}
  Port      ${BOLD}${port}${NC}
  Database  ${BOLD}${DB_NAME}${NC}
  Username  ${BOLD}${group}${NC}
  Password  ${YELLOW}<the token on your clipboard>${NC}"
  echo
  if command -v pbcopy >/dev/null 2>&1; then
    printf '%s' "$token" | pbcopy
    log_success "Token copied to clipboard (not shown). Valid ~75 min."
  else
    log_warning "pbcopy not found; copy the token manually (not shown for safety)."
  fi
  if [ "$kind" = "rider" ]; then
    log_info "Rider: add a PostgreSQL data source with the above, paste the token as"
    log_info "the password. Rider does not auto-refresh — re-run this for a fresh token."
  fi
}

main "$@"
