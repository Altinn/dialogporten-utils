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

# How to activate write/migrator access via Entra PIM. Read is standing membership
# (no PIM); write/migrator are PIM-eligible and activated on demand.
# TODO: replace with the canonical org PIM activation URL / az command.
readonly PIM_ACTIVATION_HINT="Activate it in Entra PIM (Privileged Identity Management → Groups)."

# Canonical env values (match forward.sh). yt01 = perf, lives in the test
# subscription; low focus for now but included for completeness.
readonly VALID_ENVIRONMENTS=("test" "yt01" "staging" "prod")
# Tier vocabulary tracks altinn-platform#3407: read=Readonly, write=Readwrite,
# migrator=Migrator (DDL/schema changes; renamed from the earlier "admin").
readonly VALID_TIERS=("read" "write" "migrator")

# NOTE: stock macOS ships bash 3.2 (no associative arrays), and forward.sh is
# bash-3.2 compatible, so we use case-based lookups instead of `declare -A`.

# env -> human-friendly label (Azure codename in parens). Display only; the
# canonical value (test/yt01/staging/prod) is what the script uses. Note:
# Dialogporten test = AT23 (some other teams use AT22). Keep in sync w/ forward.sh.
env_label() {
  case "$1" in
    test)    echo "test (at23)" ;;
    yt01)    echo "perf (yt01)" ;;
    staging) echo "staging (tt02)" ;;
    prod)    echo "prod" ;;
    *)       echo "$1" ;;
  esac
}

# env -> the Azure subscription that env's DB lives in. Used to auto-switch the
# active az account so the token gets the right identity. Keep in sync w/ forward.sh.
env_subscription() {
  case "$1" in
    test|yt01) echo "Dialogporten-Test" ;;
    staging)   echo "Dialogporten-Staging" ;;
    prod)      echo "Dialogporten-Prod" ;;
    *)         echo "" ;;
  esac
}

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
# staging currently REUSES the prod groups. This function is the SINGLE swap point.
#
# SWAP POINT (altinn-platform#3407): the canonical groups these temp groups map to are
#   read     -> "Altinn Product Dialogporten: PostgreSQL Readonly <Test|Prod>"
#   write    -> "Altinn Product Dialogporten: PostgreSQL Readwrite <Test|Prod>"
#   migrator -> "Altinn Product Dialogporten: PostgreSQL Migrator <Test|Prod>"
# (test/yt01 -> Test groups; staging/prod -> Prod groups). They don't exist as PG roles
# yet, so the values below are the TEMP groups. Replace each string when #3407 lands.
group_for() {
  case "$1:$2" in
    test:read)        echo "Altinn Product Dialogporten: Developers Dev" ;;
    test:write)       echo "Dialogporten-Test-Operations" ;;
    test:migrator)    echo "Dialogporten-Test-UserAdmins" ;;
    # yt01 (perf) shares the test subscription and reuses the TEST groups,
    # both now and after #3407 lands.
    yt01:read)        echo "Altinn Product Dialogporten: Developers Dev" ;;
    yt01:write)       echo "Dialogporten-Test-Operations" ;;
    yt01:migrator)    echo "Dialogporten-Test-UserAdmins" ;;
    staging:read)     echo "Altinn Product Dialogporten: Developers Prod" ;;
    staging:write)    echo "Dialogporten-Prod-Operations" ;;
    staging:migrator) echo "Altinn Product Dialogporten: Admins Prod" ;;
    prod:read)        echo "Altinn Product Dialogporten: Developers Prod" ;;
    prod:write)       echo "Dialogporten-Prod-Operations" ;;
    prod:migrator)    echo "Altinn Product Dialogporten: Admins Prod" ;;
    *)                echo "" ;;
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

# Display: friendly env WORD Title Cased, Azure codename kept lowercase in parens.
# e.g. test -> "Test (at23)", yt01 -> "Perf (yt01)", prod -> "Prod".
env_display() {
  case "$1" in
    test)    echo "Test (at23)" ;;
    yt01)    echo "Perf (yt01)" ;;
    staging) echo "Staging (tt02)" ;;
    prod)    echo "Prod" ;;
    *)       echo "$(title_case "$1")" ;;
  esac
}

# Display env WORD only, Title Cased, no codename (for terse server names).
env_word() {
  case "$1" in
    test) echo "Test" ;; yt01) echo "Perf" ;; staging) echo "Staging" ;; prod) echo "Prod" ;;
    *) echo "$(title_case "$1")" ;;
  esac
}

export_pgadmin() {
  local include_migrator=$1     # "yes"/"no"
  local exec_cmd="$PG_TOKEN_SCRIPT"
  # tier -> privilege ordinal (read<write<migrator) so servers sort least->most.
  local tiers=("read" "write" "migrator")
  local n=0 first=1 e t group port ord

  local genv
  printf '{\n  "Servers": {\n'
  for e in "${VALID_ENVIRONMENTS[@]}"; do
    for t in "${tiers[@]}"; do
      [ "$t" = "migrator" ] && [ "$include_migrator" = "no" ] && continue
      group="$(group_for "$e" "$t")"; [ -z "$group" ] && continue
      port="$(env_port "$e")"; [ -z "$port" ] && continue
      case "$t" in read) ord=1 ;; write) ord=2 ;; migrator) ord=3 ;; esac
      # env ordinal (test<yt01<staging<prod) so groups sort test->prod, not alphabetically.
      case "$e" in test) genv=1 ;; yt01) genv=2 ;; staging) genv=3 ;; prod) genv=4 ;; *) genv=9 ;; esac
      n=$((n+1))
      [ "$first" -eq 1 ] && first=0 || printf ',\n'
      # Title Case the words (Test, Read), keep the env codename (at23) lowercase;
      # ordinal prefixes preserve env (group) and tier (server) sort order.
      printf '    "%s": {\n' "$n"
      printf '      "Name": "%s %s %s",\n' "$ord" "$(env_word "$e")" "$(title_case "$t")"
      printf '      "Group": "%s Dialogporten %s",\n' "$genv" "$(env_display "$e")"
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
       $0 --export-pgadmin [--no-migrator] > servers.json

  -e, --env         ${VALID_ENVIRONMENTS[*]}
  -t, --tier        ${VALID_TIERS[*]}   (read=SELECT, write=DML, migrator=DDL)
  -c, --client      token | psql   (default: prompt; 'token' = copy a token to paste into
                    pgAdmin/Rider/etc. Old values pgadmin|rider|raw are accepted as 'token'.)
  --export-pgadmin  Emit a pgAdmin Import/Export servers JSON to stdout (all envs/tiers,
                    per-env groups, token exec command wired to this machine). No prompts.
                    Recommended for pgAdmin users: import once, then pgAdmin auto-refreshes
                    the token. (Interactive equivalent: the last client menu option.)
  --no-migrator     With --export-pgadmin: omit the migrator/DDL tier servers.
  -h, --help

Interactive when flags are omitted (asks client, then environment, then tier).

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
  local environment="" tier="" client="" export_mode="" include_migrator="yes"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -e|--env)         environment="${2:-}"; shift 2 ;;
      -t|--tier)        tier="${2:-}"; shift 2 ;;
      -c|--client)      client="${2:-}"; shift 2 ;;
      --export-pgadmin) export_mode="yes"; shift ;;
      --no-migrator)    include_migrator="no"; shift ;;
      -h|--help)        print_help; exit 0 ;;
      *) log_error "Unknown option: $1"; print_help; exit 1 ;;
    esac
  done

  # Generator mode: emit servers.json to stdout and exit (no prompts, no az needed).
  if [ "$export_mode" = "yes" ]; then
    export_pgadmin "$include_migrator"
    exit 0
  fi

  log_title "Dialogporten DB Login Helper"

  # --- dependencies & identity -------------------------------------------
  [ -z "$AZ" ] && { log_error "Azure CLI (az) not found. Install it or set AZ_BIN."; exit 1; }

  # Cache the enabled-subscription list once (used for the landscape + auto-switch).
  # Format per line: "<user>\t<subscription>\t<isDefault>"
  AZ_ACCOUNTS="$("$AZ" account list --query "[?state=='Enabled'].[user.name, name, isDefault]" -o tsv 2>/dev/null || true)"

  # Gate on having ANY logged-in account, not on having an ACTIVE one — you can
  # be logged into account A while B (which was active) is logged out, leaving no
  # active account but a usable session. The per-env auto-switch picks the right one.
  if [ -z "$AZ_ACCOUNTS" ]; then
    log_error "No Azure accounts logged in. Run:  az login"
    exit 1
  fi
  local me; me="$("$AZ" account show --query user.name -o tsv 2>/dev/null || true)"
  if [ -n "$me" ]; then
    log_info "Active Azure identity: ${BOLD}${me}${NC}  (token will be attributed to this user)"
  else
    log_warning "No ${BOLD}active${NC} Azure account (one was logged out) — pick an env and it will switch to the right one."
  fi

  # Show which identities are logged in and which Dialogporten subs each holds.
  local dp_accounts
  dp_accounts="$(printf '%s\n' "$AZ_ACCOUNTS" | grep -i 'Dialogporten-' || true)"
  if [ -n "$dp_accounts" ]; then
    log_info "Logged-in accounts (Dialogporten subscriptions):"
    # group by user, mark the active one
    printf '%s\n' "$dp_accounts" | awk -F'\t' '
      { subs[$1] = subs[$1] (subs[$1]?", ":"") $2; if ($3=="True") active[$1]=1 }
      END { for (u in subs) printf "%s\t%s\t%s\n", (u in active?"*":" "), u, subs[u] }
    ' | while IFS=$'\t' read -r mark user subs; do
      if [ "$mark" = "*" ]; then
        echo -e "    ${GREEN}●${NC} ${BOLD}${user}${NC} → ${subs}  ${GREEN}(active)${NC}"
      else
        echo -e "      ${user} → ${subs}"
      fi
    done
  fi
  echo

  # --- client selection (first: option to set up all pgAdmin servers) -----
  if [ -z "$client" ]; then
    local pick
    pick=$(prompt_choice "How will you connect?  (pgAdmin users: option 3 sets up everything once)" \
      "manual connect (token)" "psql (launch now)" "set up all pgAdmin servers (one-time import file)")
    case "$pick" in
      "manual connect"*) client="token" ;;
      "psql"*)           client="psql" ;;
      "set up all"*)     client="export" ;;
    esac
  fi

  # Generator path: write the servers.json file + import instructions, then done.
  # Doesn't need env/tier/membership — it produces all envs and tiers.
  if [ "$client" = "export" ]; then
    interactive_export_pgadmin "$include_migrator"
    exit 0
  fi

  # --- environment (show friendly labels, return canonical value) ---------
  if [ -z "$environment" ]; then
    local env_labels=() ev chosen
    for ev in "${VALID_ENVIRONMENTS[@]}"; do env_labels+=("$(env_label "$ev")"); done
    chosen=$(prompt_choice "Select environment:" "${env_labels[@]}")
    local i
    for i in "${!env_labels[@]}"; do
      [ "${env_labels[$i]}" = "$chosen" ] && { environment="${VALID_ENVIRONMENTS[$i]}"; break; }
    done
  fi
  validate_in "$environment" "${VALID_ENVIRONMENTS[@]}" || { log_error "Invalid env: $environment"; exit 1; }

  # --- auto-switch active Azure account to the env's subscription ----------
  # Each Dialogporten env lives in a known subscription owned by a specific
  # identity; `az account set` flips the active account when that sub belongs to
  # a different logged-in identity. Only switch if the target sub is available
  # and not already active; otherwise leave it and let the membership check guide.
  local target_sub active_sub
  target_sub="$(env_subscription "$environment")"
  active_sub="$("$AZ" account show --query name -o tsv 2>/dev/null || true)"
  if [ -n "$target_sub" ] && [ "$target_sub" != "$active_sub" ]; then
    if printf '%s\n' "$AZ_ACCOUNTS" | awk -F'\t' -v s="$target_sub" '$2==s{found=1} END{exit !found}'; then
      if "$AZ" account set --subscription "$target_sub" >/dev/null 2>&1; then
        me="$("$AZ" account show --query user.name -o tsv 2>/dev/null)"
        log_success "Switched active Azure account to ${BOLD}${me}${NC} (subscription ${target_sub})"
      else
        log_warning "Could not switch to ${target_sub}; continuing with the current account."
      fi
    else
      log_warning "${target_sub} isn't in your logged-in accounts — you may need: az login"
    fi
  fi

  # --- tier ---------------------------------------------------------------
  if [ -z "$tier" ]; then
    tier=$(prompt_choice "Select access tier:" "${VALID_TIERS[@]}")
  fi
  validate_in "$tier" "${VALID_TIERS[@]}" || { log_error "Invalid tier: $tier"; exit 1; }

  local group port
  group="$(group_for "$environment" "$tier")"
  port="$(env_port "$environment")"; [ -z "$port" ] && port=5432
  [ -z "$group" ] && { log_error "No group configured for ${environment}:${tier}"; exit 1; }

  # No prod confirmation prompt: prod access is gated by PIM activation (MFA +
  # justification + time-bound), and the membership check below fails clearly if
  # you haven't activated. A "type prod to continue" prompt would be theater.

  # --- best-effort membership pre-check (terminal CAN show this) ----------
  local my_oid; my_oid="$("$AZ" ad signed-in-user show --query id -o tsv 2>/dev/null || true)"
  if [ -n "$my_oid" ]; then
    local is_member
    is_member="$("$AZ" ad group member check --group "$group" --member-id "$my_oid" --query value -o tsv 2>/dev/null || true)"
    if [ "$is_member" = "false" ]; then
      log_error "Identity '${me}' is NOT currently in group '${group}'."
      # A bare 'false' has several causes the check can't distinguish; list the
      # realistic ones per tier. read = standing membership (no PIM);
      # write/migrator = PIM-eligible, activated on demand.
      if [ "$tier" = "read" ]; then
        log_info  "Read access is standing membership. Likely causes:"
        log_info  "  • You haven't been granted read access yet → ask to be added to the group."
        log_info  "  • The account auto-switch couldn't reach the right account for this env:"
        echo      "      az account list -o table   /   az account set --subscription \"<name>\"   /   az login"
      else
        log_info  "${BOLD}${tier}${NC} access is granted on demand via PIM. Likely causes:"
        log_info  "  • You're eligible but haven't activated yet (or it expired) → ${PIM_ACTIVATION_HINT}"
        log_info  "  • You may not be eligible for this role at all → request ${tier} access if you need it."
        log_info  "  • Already activated and on the right account? Then re-check the account:"
        echo      "      az account list -o table   /   az account set --subscription \"<name>\"   /   az login"
      fi
      exit 1
    elif [ "$is_member" = "true" ]; then
      log_success "Membership confirmed: ${me} ∈ ${group}"
    else
      log_warning "Could not verify group membership (continuing; PostgreSQL will enforce it)."
    fi
    echo
  fi

  echo
  case "$client" in
    token)        handoff_token "$port" "$group" ;;
    psql)         handoff_psql "$port" "$group" ;;
    # accept the old client aliases (and -c flag values) for compatibility
    pgadmin|rider|raw) handoff_token "$port" "$group" ;;
    *) log_error "Unknown client: $client"; exit 1 ;;
  esac
}

# --- Interactive generator: write servers.json + ready-to-run import command -
interactive_export_pgadmin() {
  local include_migrator=$1
  local out="./dp-pgadmin-servers.json"
  export_pgadmin "$include_migrator" > "$out"
  local abs_out; abs_out="$(cd "$(dirname "$out")" && pwd)/$(basename "$out")"
  local count; count=$(grep -c '"Name"' "$out")
  log_success "Wrote ${count} pgAdmin servers to ${BOLD}${abs_out}${NC}"
  echo

  # Build the import command (multi-line; pasting runs all three lines).
  local import_cmd
  import_cmd="PYBIN=\"/Applications/pgAdmin 4.app/Contents/Frameworks/Python.framework/Versions/3.13/bin/python3.13\"
SETUP=\"/Applications/pgAdmin 4.app/Contents/Resources/web/setup.py\"
\"\$PYBIN\" \"\$SETUP\" load-servers \"${abs_out}\" --sqlite-path ~/.pgadmin/pgadmin4.db"

  log_warning "Requires pgAdmin 4 to be installed (the import uses its bundled tooling)."
  if command -v pbcopy >/dev/null 2>&1; then
    printf '%s' "$import_cmd" | pbcopy
    print_box "Import command — ${GREEN}copied to your clipboard${NC}" "\
  Paste it into a terminal and run it.
  ${YELLOW}Additive${NC} by default (keeps your existing servers); add ${BOLD}--replace${NC}
  to update servers you imported before."
  else
    print_box "Import command" "\
  Run the command below in a terminal (pbcopy unavailable, not auto-copied).
  ${YELLOW}Additive${NC} by default; add ${BOLD}--replace${NC} to update prior imports."
  fi
  echo
  echo "$import_cmd"
  echo
  log_info "After running it, ${BOLD}restart pgAdmin${NC} — the server tree only shows imported servers after a restart."
}

# --- psql: launch directly ------------------------------------------------
handoff_psql() {
  local port=$1 group=$2
  local token; token="$("$PG_TOKEN_SCRIPT" 2>/dev/null || true)"
  [ -z "$token" ] && { log_error "Failed to get a token. Is your Azure session active? Run: az login"; exit 1; }
  log_info "Launching psql..."
  PGPASSWORD="$token" PGUSER="$group" \
    psql "host=localhost port=${port} dbname=${DB_NAME} sslmode=require" || true
}

# --- Manual connect: connection details + token on the clipboard -----------
# Works for any client (pgAdmin, Rider, DBeaver, a psql GUI, ...): give the
# connection details and the token to paste as the password. For pgAdmin
# auto-refresh (no pasting), use the "set up all pgAdmin servers" option.
handoff_token() {
  local port=$1 group=$2
  local token; token="$("$PG_TOKEN_SCRIPT" 2>/dev/null || true)"
  [ -z "$token" ] && { log_error "Failed to get a token. Is your Azure session active? Run: az login"; exit 1; }
  print_box "Connection details — paste the token as the password" "\
  Host      ${BOLD}localhost${NC}
  Port      ${BOLD}${port}${NC}
  Database  ${BOLD}${DB_NAME}${NC}
  Username  ${BOLD}${group}${NC}
  Password  ${YELLOW}the token (on your clipboard)${NC}"
  echo
  if command -v pbcopy >/dev/null 2>&1; then
    printf '%s' "$token" | pbcopy
    log_success "Token copied to clipboard. Valid ~75 min; re-run this for a fresh one."
  else
    log_warning "pbcopy not found — copy the token manually (not shown here for safety)."
  fi
  log_info "pgAdmin tip: for auto-refresh (no token pasting), use the 'set up all pgAdmin servers' option."
}

main "$@"
