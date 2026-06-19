#!/usr/bin/env bash
# =========================================================================
# Database Connection Forwarder for Dialogporten
#
# Sets up secure SSH tunnels to Azure database resources using a jumper VM.
# Supports PostgreSQL and Redis connections across environments.
# =========================================================================

set -euo pipefail

# =========================================================================
# Azure CLI Configuration
# =========================================================================
# Disable Azure CLI auto-update checks and telemetry to prevent interactive prompts
export AZURE_CORE_COLLECT_TELEMETRY=false
export AZURE_CORE_NO_COLOR=true
export AZURE_CORE_DISABLE_PROGRESS_BAR=true
export AZURE_CORE_ONLY_SHOW_ERRORS=false
export AZURE_CORE_DISABLE_UPGRADE_WARNINGS=yes

# =========================================================================
# Constants
# =========================================================================
readonly PRODUCT_TAG="Dialogporten"
readonly DEFAULT_POSTGRES_PORT=5432   # remote port on the server (do not change)
readonly DEFAULT_REDIS_PORT=6380
readonly VALID_ENVIRONMENTS=("test" "yt01" "staging" "prod")

# Per-env LOCAL bind port for postgres, so tunnels to multiple envs don't
# collide on localhost. Scheme: increasing toward prod (prod most distinct).
# Keep in sync with db-login.sh env_port() and the pgAdmin server configs.
postgres_local_port() {
    # Per-env LOCAL bind ports, deliberately in the 25432-55432 range to avoid the
    # common local-Docker/Podman Postgres port (15432) and the default 5432.
    case "$1" in
        test)    echo 25432 ;;
        yt01)    echo 35432 ;;
        staging) echo 45432 ;;
        prod)    echo 55432 ;;
        *)       echo "$DEFAULT_POSTGRES_PORT" ;;
    esac
}
readonly VALID_DB_TYPES=("postgres" "redis")
readonly SUBSCRIPTION_PREFIX="Dialogporten"
readonly JIT_DURATION="PT1H"  # 1 hour duration for JIT access

# Colors and formatting
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# =========================================================================
# Utility Functions
# =========================================================================

# Logging functions
log_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✖${NC} $1" >&2
}

log_title() {
    echo -e "\n${BOLD}${CYAN}$1${NC}"
}

# Print a formatted box with title and content (ANSI-aware). Borders are built
# with a printf loop instead of `tr`, which mangles the multibyte box chars when
# LANG is unset / C locale; rows are floored at zero padding so long lines (e.g.
# the connection string) don't produce a ragged box. Mirrors db-login.sh.
print_box() {
    local title="$1" content="$2" width=70 padding=2
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

# Convert a string to uppercase
to_upper() {
    echo "$1" | tr '[:lower:]' '[:upper:]'
}

# env -> human-friendly label (Azure codename in parens). Display only; the
# canonical env value (test/yt01/staging/prod) is what the rest of the script
# uses. Note: Dialogporten test = AT23 (some other teams use AT22).
env_label() {
    case "$1" in
        test)    echo "test (at23)"    ;;
        yt01)    echo "perf (yt01)"    ;;
        staging) echo "staging (tt02)" ;;
        prod)    echo "prod"           ;;
        *)       echo "$1"             ;;
    esac
}

# Show an interactive selection prompt
prompt_selection() {
    local prompt=$1
    shift
    local options=("$@")
    local selected

    trap 'echo -e "\nOperation cancelled by user"; exit 130' INT

    PS3="$prompt "
    select selected in "${options[@]}"; do
        if [ -n "$selected" ]; then
            echo "$selected"
            return
        fi
    done
}

# Show an interactive selection prompt with default support
# Usage: select_from_list "Resource Type" "name1\nname2\nname3"
# Echoes the selected name. Defaults to the shortest name (base name without suffix).
select_from_list() {
    local resource_label=$1
    local names=$2

    # Sort and build array, tracking shortest name as default
    local sorted_names
    sorted_names=$(echo "$names" | sed '/^$/d' | sort)

    local servers=()
    local default_name=""
    while IFS= read -r server_name; do
        if [ -z "$default_name" ] || [ ${#server_name} -lt ${#default_name} ]; then
            default_name="$server_name"
        fi
        servers+=("$server_name")
    done <<< "$sorted_names"

    # Single match — return directly
    if [ "${#servers[@]}" -le 1 ]; then
        echo "${servers[0]}"
        return
    fi

    # Multiple matches — prompt for selection
    log_warning "Multiple ${resource_label} servers found:" >&2
    log_info "Default: ${BOLD}${default_name}${NC}" >&2
    echo "" >&2
    local i=1
    for server_name in "${servers[@]}"; do
        if [ "$server_name" = "$default_name" ]; then
            echo -e "  ${GREEN}${i})${NC} ${BOLD}${server_name}${NC} (default)" >&2
        else
            echo -e "  ${i}) ${server_name}" >&2
        fi
        ((i++))
    done
    echo "" >&2

    local selection
    while true; do
        read -rp "Select server (1-${#servers[@]}) [default: ${default_name}]: " selection

        if [ -z "$selection" ]; then
            log_success "Selected server: ${default_name}" >&2
            echo "$default_name"
            return
        elif [[ "$selection" =~ ^[0-9]+$ ]] && [ "$selection" -ge 1 ] && [ "$selection" -le "${#servers[@]}" ]; then
            local selected="${servers[$((selection - 1))]}"
            log_success "Selected server: ${selected}" >&2
            echo "$selected"
            return
        else
            log_error "Invalid selection: $selection. Please try again." >&2
        fi
    done
}

# Show help message
print_help() {
    cat << EOF
Database Connection Forwarder
============================
A tool to set up secure SSH tunnels to Azure database resources.

Usage:
    $0 [OPTIONS]

Options:
    -e, --environment ENV  Environment to connect to (${VALID_ENVIRONMENTS[*]})
                          Each environment maps to a specific Azure subscription:
                          - test/yt01  -> ${SUBSCRIPTION_PREFIX}-Test
                          - staging    -> ${SUBSCRIPTION_PREFIX}-Staging
                          - prod      -> ${SUBSCRIPTION_PREFIX}-Prod

    -t, --type TYPE       Database type to connect to (${VALID_DB_TYPES[*]})
                          - postgres: PostgreSQL Flexible Server (local port per env:
                            test=25432, yt01=35432, staging=45432, prod=55432)
                          - redis:    Redis Cache (default port: $DEFAULT_REDIS_PORT)

    -n, --name NAME       Override resource base name for selected type/environment
                          - postgres hostname: NAME.postgres.database.azure.com
                          - redis hostname:    NAME.redis.cache.windows.net
                          The specified resource must exist in the selected environment

    -p, --port PORT       Local port to bind on localhost (127.0.0.1)
                          If not specified, will use the default port for the selected database

    -s, --shell           Open an interactive shell on the jumper (prints the Ubuntu MOTD;
                          exit to close). Default is port-forward only (-N): no shell, no
                          MOTD, tunnel holds the terminal — press Ctrl-C to stop. Use this
                          only if you need to run commands ON the jumper itself.

    --no-prompt           Never prompt; run unattended. REQUIRES -e (no safe env default).
                          Fills the rest with defaults: -t postgres, the per-env local port,
                          and tunnel-only mode. If -n is omitted and multiple servers match,
                          it errors (won't guess between e.g. postgres / postgres2) — pass -n.
                          Good for backgrounding: forward.sh --no-prompt -e staging &

    --kill-stale          If a stale Dialogporten tunnel is already holding the local port,
                          kill it and start fresh without prompting. (A healthy tunnel to the
                          SAME server is reused either way; a non-tunnel listener, e.g. Docker,
                          is never killed.) Without this flag, an interactive run asks first
                          and a --no-prompt run refuses.

    -h, --help           Show this help message

Prerequisites:
    - Azure CLI must be installed and you must be logged in
    - If Azure CLI prompts for upgrades, the script will detect this and provide guidance
    - You must have appropriate permissions for the target Azure subscription

Examples:
    # Interactive mode (will prompt for all options)
    $0

    # Connect to PostgreSQL in test environment
    $0 -e test -t postgres
    $0 --environment test --type postgres

    # Connect to Redis in prod with custom local port
    $0 -e prod -t redis -p 6380
    $0 --environment prod --type redis --port 6380

    # Connect to a specific named postgres server
    $0 -e test -t postgres -n foobar

Troubleshooting:
    If the script hangs or times out, it might be due to Azure CLI upgrade prompts.
    Run 'az version' manually to resolve any pending prompts, then re-run this script.

EOF
}

# =========================================================================
# Validation Functions
# =========================================================================

# Validate environment name
validate_environment() {
    local env=$1
    for valid_env in "${VALID_ENVIRONMENTS[@]}"; do
        if [[ "$env" == "$valid_env" ]]; then
            return 0
        fi
    done
    log_error "Invalid environment: $env"
    log_info "Valid environments: ${VALID_ENVIRONMENTS[*]}"
    exit 1
}

# Validate database type
validate_db_type() {
    local db_type=$1
    for valid_type in "${VALID_DB_TYPES[@]}"; do
        if [[ "$db_type" == "$valid_type" ]]; then
            return 0
        fi
    done
    log_error "Invalid database type: $db_type"
    log_info "Valid database types: ${VALID_DB_TYPES[*]}"
    exit 1
}

# Validate port number
validate_port() {
    local port=$1

    # Check if the port is a number
    if ! [[ "$port" =~ ^[0-9]+$ ]]; then
        log_error "Port must be a number"
        return 1
    fi

    # Check if the port is within valid range (1-65535)
    if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
        log_error "Port must be between 1 and 65535"
        return 1
    fi

    return 0
}

# Validate that an option has a value
require_option_value() {
    local option_name=$1
    local value=${2:-}

    if [ -z "$value" ] || [[ "$value" == -* ]]; then
        log_error "Option ${option_name} requires a value"
        exit 1
    fi
}

# =========================================================================
# Azure Functions
# =========================================================================

# Check prerequisites
check_dependencies() {
    if ! command -v az >/dev/null 2>&1; then
        log_error "Azure CLI is not installed. Please visit: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
        exit 1
    fi
    log_success "Azure CLI is installed"
}

# Cache of enabled accounts, "<user>\t<subscription>" per line. Populated by
# show_account_landscape, reused to mark envs whose owning account isn't logged in.
AZ_ACCOUNTS=""

# True if the subscription that owns $1 (an env) is among the logged-in accounts.
subscription_logged_in() {
    local sub_name
    sub_name=$(get_subscription_name "$1")
    [ -z "$sub_name" ] && return 1
    printf '%s\n' "$AZ_ACCOUNTS" | awk -F'\t' -v s="$sub_name" '$2==s{f=1} END{exit !f}'
}

# Surface which Dialogporten accounts are logged in (mirrors db-login.sh). What
# matters is being logged into the account that OWNS the env you target; main()
# then switches the active account to that owner before tunneling (the tunnel cert
# is minted for the active identity). Showing the landscape up front makes a
# partial login obvious before the tunnel fails. Returns 0 if any Dialogporten
# account is logged in.
show_account_landscape() {
    AZ_ACCOUNTS=$(az account list --query "[?state=='Enabled'].[user.name, name]" -o tsv 2>/dev/null || true)
    if [ -z "$AZ_ACCOUNTS" ]; then
        log_warning "No Azure accounts logged in. Run:  az login"
        return 1
    fi
    local dp_accounts
    dp_accounts=$(printf '%s\n' "$AZ_ACCOUNTS" | grep -i 'Dialogporten' || true)
    if [ -z "$dp_accounts" ]; then
        log_warning "Logged in, but no Dialogporten subscriptions found — you may need:  az login"
        return 1
    fi
    log_info "Logged-in Dialogporten accounts (the env you pick must be owned by one of these):"
    printf '%s\n' "$dp_accounts" | awk -F'\t' '
        { subs[$1] = subs[$1] (subs[$1]?", ":"") $2 }
        END { for (u in subs) printf "%s\t%s\n", u, subs[u] }
    ' | while IFS=$'\t' read -r user subs; do
        echo -e "      ${BOLD}${user}${NC} → ${subs}"
    done
    return 0
}

# Get subscription name from environment
get_subscription_name() {
    local env=$1
    case "$env" in
        "test"|"yt01")  echo "${SUBSCRIPTION_PREFIX}-Test"     ;;
        "staging")      echo "${SUBSCRIPTION_PREFIX}-Staging"   ;;
        "prod")         echo "${SUBSCRIPTION_PREFIX}-Prod"      ;;
        *)              echo ""                                 ;;
    esac
}

# Resource naming helper functions
get_resource_group() {
    local env=$1
    echo "dp-be-${env}-rg"
}

get_jumper_vm_name() {
    local env=$1
    echo "dp-be-${env}-ssh-jumper"
}

# Configure Just-In-Time access for the jumper VM
configure_jit_access() {
    local env=$1
    local subscription_id=$2
    local resource_group
    resource_group=$(get_resource_group "$env")
    local vm_name
    vm_name=$(get_jumper_vm_name "$env")

    # Create temporary file
    local temp_file
    temp_file=$(mktemp)

    # Define cleanup function
    cleanup() {
        local tf="$1"
        [ -f "$tf" ] && rm -f "$tf"
    }

    # Set up cleanup trap using the cleanup function
    trap "cleanup '$temp_file'" EXIT

    log_info "Configuring JIT access..."

    # Get public IP
    log_info "Detecting your public IP address..."
    local my_ip
    my_ip=$(curl -s https://ipinfo.io/json | grep -o '"ip": *"[^"]*"' | sed 's/"ip": *"\([^"]*\)"/\1/')
    if [ -z "$my_ip" ]; then
        log_error "Failed to get public IP address from ipinfo.io"
        exit 1
    fi

    # Validate IP format from ipinfo.io
    if ! [[ "$my_ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || ! [[ "$(echo "$my_ip" | tr '.' '\n' | sort -n | tail -n1)" -le 255 ]]; then
        log_error "Invalid IP address format from ipinfo.io: $my_ip"
        exit 1
    fi

    # Double check IP with a second service
    local my_ip_2
    my_ip_2=$(curl -s https://api.ipify.org)
    if [ -z "$my_ip_2" ]; then
        log_error "Failed to get public IP address from ipify.org"
        exit 1
    fi

    # Validate IP format from ipify.org
    if ! [[ "$my_ip_2" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || ! [[ "$(echo "$my_ip_2" | tr '.' '\n' | sort -n | tail -n1)" -le 255 ]]; then
        log_error "Invalid IP address format from ipify.org: $my_ip_2"
        exit 1
    fi

    # Compare the two IPs
    if [ "$my_ip" != "$my_ip_2" ]; then
        log_error "Inconsistent IP addresses detected:"
        log_error "ipinfo.io: $my_ip"
        log_error "ipify.org:   $my_ip_2"
        exit 1
    fi

    log_success "Public IP detected: $my_ip"

    # Get VM details. Scope each az call with --subscription so it targets this
    # env's subscription explicitly (main() has already set the active account to
    # the owning identity by this point).
    log_info "Fetching VM details..."
    local vm_id
    vm_id=$(az vm show --subscription "$subscription_id" --resource-group "$resource_group" --name "$vm_name" --query "id" -o tsv)
    if [ -z "$vm_id" ]; then
        log_error "Failed to get VM ID for $vm_name in resource group $resource_group"
        exit 1
    fi
    log_success "Found VM with ID: $vm_id"

    local location
    location=$(az vm show --subscription "$subscription_id" --resource-group "$resource_group" --name "$vm_name" --query "location" -o tsv)
    if [ -z "$location" ]; then
        log_error "Failed to get location for VM $vm_name"
        exit 1
    fi
    log_success "VM is located in: $location"

    # Construct JIT API endpoint
    local endpoint="https://management.azure.com/subscriptions/$subscription_id/resourceGroups/$resource_group/providers/Microsoft.Security/locations/$location/jitNetworkAccessPolicies/${vm_name}/initiate?api-version=2020-01-01"

    # Construct JSON payload
    log_info "Preparing JIT access request..."

    # Write JSON to temporary file
    cat > "$temp_file" << EOF
{
  "virtualMachines": [
    {
      "id": "$vm_id",
      "ports": [
        {
          "number": 22,
          "duration": "$JIT_DURATION",
          "allowedSourceAddressPrefix": "$my_ip/32"
        }
      ]
    }
  ]
}
EOF

    # Request JIT access
    log_info "Requesting JIT access..."
    log_info "Using endpoint: $endpoint"
    echo

    local jit_response
    if ! jit_response=$(az rest --subscription "$subscription_id" --method post --uri "$endpoint" --headers "Content-Type=application/json" --body "@$temp_file" 2>&1); then
        log_error "Failed to configure JIT access. Error: $jit_response"
        log_info "Please ensure you have the necessary permissions and that JIT access is enabled for this VM"
        exit 1
    fi

    log_success "JIT access configured successfully (valid for 1 hour)"
}

# =========================================================================
# Database Functions
# =========================================================================

# Discover available servers and let the user pick if multiple exist.
# Echoes the selected server name.
discover_server() {
    local db_type=$1
    local env=$2
    local subscription_id=$3
    local no_prompt=${4:-false}

    local all_names
    if [ "$db_type" = "postgres" ]; then
        log_info "Discovering PostgreSQL servers..." >&2
        all_names=$(az postgres flexible-server list --subscription "$subscription_id" \
            --query "[?tags.Environment=='$env' && tags.Product=='$PRODUCT_TAG'].name" -o tsv)
    else
        log_info "Discovering Redis servers..." >&2
        all_names=$(az redis list --subscription "$subscription_id" \
            --query "[?tags.Environment=='$env' && tags.Product=='$PRODUCT_TAG'].name" -o tsv)
    fi

    if [ -z "$all_names" ]; then
        log_error "No ${db_type} server found in environment '${env}'"
        exit 1
    fi

    # Under --no-prompt, refuse to silently pick between multiple matches (e.g.
    # postgres vs postgres2): that ambiguity is exactly where a wrong guess hurts.
    # Require an explicit -n in that case.
    if [ "$no_prompt" = "true" ]; then
        local count
        count=$(printf '%s\n' "$all_names" | sed '/^$/d' | wc -l | tr -d ' ')
        if [ "$count" -gt 1 ]; then
            log_error "Multiple ${db_type} servers found in '${env}' — pass -n NAME to choose (--no-prompt won't guess):" >&2
            printf '%s\n' "$all_names" | sed '/^$/d; s/^/    /' >&2
            exit 1
        fi
        printf '%s\n' "$all_names" | sed '/^$/d'
        return
    fi

    local label
    label=$([[ "$db_type" == "postgres" ]] && echo "PostgreSQL" || echo "Redis")
    select_from_list "$label" "$all_names"
}

# Get PostgreSQL server information
get_postgres_info() {
    local env=$1
    local subscription_id=$2
    local name=$3

    log_info "Fetching PostgreSQL server information..."
    local username

    if ! username=$(az postgres flexible-server show \
        --subscription "$subscription_id" \
        --resource-group "$(get_resource_group "$env")" \
        --name "$name" \
        --query "administratorLogin" -o tsv 2>/dev/null); then
        log_error "Postgres server '$name' was not found in environment '$env'"
        exit 1
    fi

    local hostname="${name}.postgres.database.azure.com"
    local port=$DEFAULT_POSTGRES_PORT

    echo "name=$name"
    echo "hostname=$hostname"
    echo "port=$port"
    echo "connection_string=postgresql://${username}:<retrieve-password-from-keyvault>@localhost:${local_port:-$port}/dialogporten"
}

# Get Redis server information
get_redis_info() {
    local env=$1
    local subscription_id=$2
    local name=$3

    log_info "Fetching Redis server information..."

    if ! az redis show \
        --subscription "$subscription_id" \
        --resource-group "$(get_resource_group "$env")" \
        --name "$name" \
        --query "name" -o tsv >/dev/null 2>&1; then
        log_error "Redis server '$name' was not found in environment '$env'"
        exit 1
    fi

    local hostname="${name}.redis.cache.windows.net"
    local port=$DEFAULT_REDIS_PORT

    echo "name=$name"
    echo "hostname=$hostname"
    echo "port=$port"
    echo "connection_string=redis://:<retrieve-password-from-keyvault>@${hostname}:${local_port:-$port}"
}

# Pre-flight a local port before opening a tunnel on it. Tunnel-only (-N) sessions
# can be left running (terminal closed, machine slept) and silently squat on the
# port, causing a cryptic "Address already in use" on the next run AND, worse, your
# DB client connecting through a stale/wrong tunnel. This classifies what (if
# anything) holds the port and decides what to do.
#
# Args: $1=local_port  $2=target_hostname  $3=no_prompt(true/false)  $4=kill_stale(true/false)
# Returns: 0 = OK to launch a new tunnel; 1 = reuse the existing healthy tunnel
#          (caller should NOT launch); exits non-zero on an unresolved conflict.
preflight_port() {
    local port=$1 target=$2 no_prompt=${3:-false} kill_stale=${4:-false}

    local pids
    pids=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | sort -u)
    [ -z "$pids" ] && return 0   # port free -> launch

    # Inspect each holder. An "our tunnel" is an ssh process whose command contains
    # `-L <port>:<host>:`. Capture whether it targets the SAME host we want.
    local pid cmd our_same="" our_other="" foreign=""
    for pid in $pids; do
        cmd=$(ps -o command= -p "$pid" 2>/dev/null)
        if printf '%s' "$cmd" | grep -q -- "-L ${port}:.*:"; then
            if printf '%s' "$cmd" | grep -q -- "-L ${port}:${target}:"; then
                our_same="$our_same $pid"
            else
                our_other="$our_other $pid"
            fi
        else
            foreign="$foreign $pid ($(printf '%s' "$cmd" | awk '{print $1}'))"
        fi
    done

    # Case 1: a healthy tunnel to the SAME server already exists -> reuse it.
    if [ -n "$our_same" ] && [ -z "$our_other" ] && [ -z "$foreign" ]; then
        log_success "A tunnel to ${BOLD}${target}${NC} is already up on ${BOLD}localhost:${port}${NC} — reusing it."
        log_info "Stop it with ${BOLD}Ctrl-C${NC} in its terminal, or:  kill${our_same}"
        return 1   # signal caller to NOT launch a duplicate
    fi

    # Case 2: a FOREIGN listener (not our ssh tunnel) holds the port (e.g. gvproxy/Docker).
    # Never kill it — it's not ours.
    if [ -n "$foreign" ]; then
        log_error "Port ${port} is held by a non-tunnel process:${foreign}"
        log_info  "That isn't a Dialogporten tunnel, so it won't be touched. Free it, or pick another port with ${BOLD}-p <port>${NC}."
        exit 1
    fi

    # Case 3: stale/other Dialogporten tunnel(s) on this port (wrong host, or duplicates).
    local stale="${our_same}${our_other}"
    log_warning "Port ${port} is already held by an existing Dialogporten tunnel (likely stale):${stale}"
    if [ "$kill_stale" = "true" ]; then
        log_info "Killing stale tunnel(s) (--kill-stale):${stale}"
        # shellcheck disable=SC2086
        kill $stale 2>/dev/null; sleep 1
        return 0
    fi
    if [ "$no_prompt" = "true" ]; then
        log_error "Refusing to launch over a stale tunnel. Re-run with ${BOLD}--kill-stale${NC}, or stop it:  kill${stale}"
        exit 1
    fi
    # Interactive: offer to kill and continue.
    local ans
    read -rp "Kill the stale tunnel(s) and start fresh? [Y/n]: " ans
    case "${ans:-Y}" in
        [Yy]*|"")
            # shellcheck disable=SC2086
            kill $stale 2>/dev/null; sleep 1
            log_success "Cleared. Starting a fresh tunnel."
            return 0 ;;
        *)
            log_warning "Left the existing tunnel in place. If it's healthy you can just use localhost:${port}; otherwise stop it and re-run."
            exit 0 ;;
    esac
}

# Set up SSH tunnel to the database.
# Default (shell_mode=false): -N port-forward only — no remote shell, no Ubuntu
#   MOTD; the tunnel holds the terminal, press Ctrl-C to stop.
# shell_mode=true (--shell): -tt interactive jumper shell (prints the MOTD);
#   exit the shell to close the tunnel. Only needed to run commands ON the jumper.
setup_ssh_tunnel() {
    local env=$1
    local hostname=$2
    local remote_port=$3
    local local_port=${4:-$remote_port}
    local shell_mode=${5:-false}
    local subscription_id=${6:-}
    local no_prompt=${7:-false}
    local kill_stale=${8:-false}

    # Pre-flight the local port: free -> continue; healthy same-host tunnel -> reuse
    # (don't launch a duplicate); foreign listener -> refuse; stale tunnel -> kill (prompt
    # or --kill-stale) then continue.
    if ! preflight_port "$local_port" "$hostname" "$no_prompt" "$kill_stale"; then
        return 0   # reused an existing tunnel; nothing more to do
    fi

    # By this point main() has set the active account to the env's owning identity
    # (the SSH cert is minted for the ACTIVE identity, and only the owning account
    # can authenticate to the env's jumper). The --subscription flag keeps the call
    # scoped to that env's subscription.
    #
    # `-o IdentitiesOnly=yes` is critical: it stops ssh offering every key in your
    # ssh-agent to the jumper. With a busy agent (many keys), ssh burns through the
    # jumper's MaxAuthTries (~6) before reaching the Entra cert az just minted, and
    # the jumper drops you with "Too many authentication failures". This forces ssh
    # to use ONLY the az-provided cert.
    local ssh_opts=(-o IdentitiesOnly=yes)
    log_info "Connecting to ${hostname}:${remote_port} via local port ${local_port}"
    if [ "$shell_mode" = "true" ]; then
        log_info "Interactive jumper shell; type ${BOLD}exit${NC} to close the tunnel."
        az ssh vm \
            --subscription "$subscription_id" \
            -g "$(get_resource_group "$env")" \
            -n "$(get_jumper_vm_name "$env")" \
            -- "${ssh_opts[@]}" -tt -L "${local_port}:${hostname}:${remote_port}"
    else
        # `az ssh vm -N` holds the terminal once the tunnel is up, so we can't print
        # "tunnel up" after it. But printing before it is a lie: az still has to log in,
        # request the cert and authenticate to the jumper before the local port binds
        # (often 10-30s), and a client that connects in that window gets "Connection
        # refused". So announce that we're establishing, then background a waiter that
        # prints the real "up" line only once the local port is actually LISTENING
        # (same lsof check preflight_port uses). The waiter exits on its own.
        log_info "Establishing tunnel to ${hostname}:${remote_port} on ${BOLD}localhost:${local_port}${NC} (Azure login + JIT cert can take ~10-30s)..."
        (
            for _ in $(seq 1 120); do
                if [ -n "$(lsof -nP -iTCP:"$local_port" -sTCP:LISTEN -t 2>/dev/null)" ]; then
                    log_success "Tunnel up on ${BOLD}localhost:${local_port}${NC} — press ${BOLD}Ctrl-C${NC} to stop."
                    exit 0
                fi
                sleep 0.5
            done
            log_warning "Local port ${local_port} still not listening after 60s — check for errors above; the tunnel may have failed."
        ) &
        local waiter_pid=$!
        # Don't leave the waiter running if the tunnel command returns/cancels first.
        trap 'kill "$waiter_pid" 2>/dev/null' RETURN
        az ssh vm \
            --subscription "$subscription_id" \
            -g "$(get_resource_group "$env")" \
            -n "$(get_jumper_vm_name "$env")" \
            -- "${ssh_opts[@]}" -N -L "${local_port}:${hostname}:${remote_port}"
    fi
}

# =========================================================================
# Easter Eggs
# =========================================================================

show_coffee_break() {
    cat << "EOF"
   ( (
    ) )
  ........
  |      |]
  \      /
   `----'

Time for a coffee break! ☕
Remember: Database connections are like good coffee - they should be secure and well-filtered.
EOF
    sleep 2  # Pause to enjoy the coffee
}

# =========================================================================
# Main Function
# =========================================================================

# Main execution function
main() {
    local environment=$1
    local db_type=$2
    local local_port=$3
    local name_override=${4:-}
    local shell_mode=${5:-false}
    local no_prompt=${6:-false}
    local kill_stale=${7:-false}

    # Add trap to handle script termination
    trap 'echo -e "\n${YELLOW}⚠${NC} Operation interrupted"; exit 130' INT TERM

    log_title "Database Connection Forwarder"

    check_dependencies
    show_account_landscape || true   # informational; the per-env resolve enforces it
    echo

    # --no-prompt: never prompt. Require an explicit env (no safe default — test/
    # staging/prod are not interchangeable). Other values get non-interactive
    # defaults below (type=postgres, per-env port, connect-mode=tunnel-only); an
    # ambiguous server with no -n fails in discover_server rather than guessing.
    if [ "$no_prompt" = "true" ] && [ -z "$environment" ]; then
        log_error "--no-prompt requires an explicit environment: -e ${VALID_ENVIRONMENTS[*]}"
        exit 1
    fi

    # If environment is not provided, prompt for it (show friendly labels,
    # return the canonical env value). Envs whose owning account isn't logged in
    # are shown in red with a "(not logged in)" marker — still selectable (the
    # login check is a best-effort snapshot; picking one yields the clear az-login
    # error rather than a hard block that a false negative could trap you behind).
    if [ -z "$environment" ]; then
        log_info "Please select target environment:"
        local env_labels=() ev chosen_label
        for ev in "${VALID_ENVIRONMENTS[@]}"; do
            if subscription_logged_in "$ev"; then
                env_labels+=("$(env_label "$ev")")
            else
                env_labels+=("$(printf '%b' "${RED}$(env_label "$ev")  (not logged in)${NC}")")
            fi
        done
        chosen_label=$(prompt_selection "Environment (1-${#VALID_ENVIRONMENTS[@]}): " "${env_labels[@]}")
        # map the chosen label back to its canonical env value by index
        local i
        for i in "${!env_labels[@]}"; do
            [ "${env_labels[$i]}" = "$chosen_label" ] && { environment="${VALID_ENVIRONMENTS[$i]}"; break; }
        done
    fi
    validate_environment "$environment"

    # If db_type is not provided, prompt for it — or default to postgres under
    # --no-prompt (the common case; pass -t redis to override).
    if [ -z "$db_type" ]; then
        if [ "$no_prompt" = "true" ]; then
            db_type="postgres"
            log_info "No -t given; defaulting to ${BOLD}postgres${NC} (--no-prompt)."
        else
            log_info "Please select database type:"
            db_type=$(prompt_selection "Database (1-${#VALID_DB_TYPES[@]}): " "${VALID_DB_TYPES[@]}")
        fi
    fi
    validate_db_type "$db_type"

    # If local_port is not provided: use the per-env default silently under
    # --no-prompt, otherwise prompt (with the default pre-filled).
    if [ -z "$local_port" ]; then
        # postgres uses a per-env local port (avoids multi-env collisions); redis keeps its default
        default_port=$([[ "$db_type" == "postgres" ]] && postgres_local_port "$environment" || echo "$DEFAULT_REDIS_PORT")
        if [ "$no_prompt" = "true" ]; then
            local_port="$default_port"
            log_info "No -p given; binding default port ${BOLD}${local_port}${NC} (--no-prompt)."
            validate_port "$local_port" || exit 1
        else
            while true; do
                log_info "Local port to bind on localhost (127.0.0.1) for ${BOLD}$(env_label "$environment")${NC} ${db_type}"
                read -rp "Port [press Enter for default ${default_port}]: " local_port
                local_port=${local_port:-$default_port}

                if validate_port "$local_port"; then
                    break
                fi
            done
        fi
    else
        validate_port "$local_port" || exit 1
    fi

    # Ensure the ACTIVE az account owns this env's subscription, switching if needed.
    # Unlike token fetching (db-login.sh, which is --subscription-scoped and never
    # switches), the SSH tunnel below mints a cert for the ACTIVE identity — and only
    # the identity that owns the env's subscription can authenticate to its jumper.
    # So here a switch is genuinely required when the env is owned by a different
    # logged-in account (e.g. test/yt01 -> ext-khaug, staging/prod -> ext-khaug-prod).
    local sub_name; sub_name=$(get_subscription_name "$environment")
    # Find which logged-in account (if any) owns this subscription, and the active one.
    local owner active_user active_sub
    owner=$(az account list --query "[?name=='${sub_name}' && state=='Enabled'].user.name | [0]" -o tsv 2>/dev/null)
    active_user=$(az account show --query user.name -o tsv 2>/dev/null || true)
    active_sub=$(az account show --query name -o tsv 2>/dev/null || true)

    if [ -z "$owner" ]; then
        log_error "Not logged into any account that has '${sub_name}' (needed for ${BOLD}$(env_label "$environment")${NC})."
        log_info  "Run:  az login  as the identity that owns it, then retry."
        exit 1
    fi

    if [ "$active_sub" = "$sub_name" ]; then
        log_success "Active account ${BOLD}${active_user}${NC} already targets ${sub_name} for ${BOLD}$(env_label "$environment")${NC}."
    else
        # The switch is required (the tunnel cert is bound to the active identity, and
        # only the env's owning account can authenticate to its jumper) and benign
        # (az account set is trivially reversible). So we don't prompt — but we DO
        # announce it loudly, since changing the active account affects other terminals.
        log_warning "Switching active az account to ${BOLD}${owner}${NC} (subscription ${sub_name}) for ${BOLD}$(env_label "$environment")${NC}."
        log_info  "  (the tunnel needs the env's owning identity active; this also changes it for other terminals)"
        if az account set --subscription "$sub_name" >/dev/null 2>&1; then
            active_user=$(az account show --query user.name -o tsv 2>/dev/null)
            log_success "Active account is now ${BOLD}${active_user}${NC}."
        else
            log_error "Failed to switch to ${sub_name}. Try:  az account set --subscription \"${sub_name}\""
            exit 1
        fi
    fi

    local subscription_id
    subscription_id=$(az account show --query id -o tsv 2>/dev/null)

    # Discover/select server if no explicit name was given
    if [ -z "$name_override" ]; then
        name_override=$(discover_server "$db_type" "$environment" "$subscription_id" "$no_prompt")
    fi

    # Print confirmation
    print_box "Configuration" "\
Environment: ${BOLD}${CYAN}$(env_label "$environment")${NC}
Database:    ${BOLD}${YELLOW}${db_type}${NC}
Name:        ${BOLD}${name_override}${NC}
Local Port:  ${BOLD}${local_port:-"<default>"}${NC}"
    echo

    # Confirm + pick tunnel mode in one step. Skip the prompt when intent is
    # already stated: --shell (explicit shell) or --no-prompt (tunnel-only).
    if [ "$shell_mode" != "true" ] && [ "$no_prompt" != "true" ]; then
        echo -e "${BOLD}How do you want to connect?${NC}"
        echo -e "  ${CYAN}1)${NC} Tunnel only        (recommended — port-forward, Ctrl-C to stop)"
        echo -e "  ${CYAN}2)${NC} Tunnel + shell     (also opens an interactive shell on the jumper)"
        echo -e "  ${CYAN}3)${NC} Cancel"
        local mode
        read -rp "Select (1-3) [default: 1]: " mode
        case "${mode:-1}" in
            1) shell_mode="false" ;;
            2) shell_mode="true" ;;
            3) log_warning "Cancelled."; exit 0 ;;
            *) log_warning "Cancelled (unrecognized choice)."; exit 0 ;;
        esac
    fi
    echo

    log_info "Setting up connection for ${BOLD}$(env_label "$environment")${NC}"

    # Get database information based on database type
    local resource_info
    if [ "$db_type" = "postgres" ]; then
        resource_info=$(get_postgres_info "$environment" "$subscription_id" "$name_override")
    else
        resource_info=$(get_redis_info "$environment" "$subscription_id" "$name_override")
    fi

    # Parse the resource information
    local hostname="" port="" connection_string=""
    while IFS='=' read -r key value; do
        case "$key" in
            "hostname") hostname="$value" ;;
            "port") port="$value" ;;
            "connection_string") connection_string="$value" ;;
        esac
    done <<< "$resource_info"

    # Validate that we have all required information
    if [ -z "$hostname" ] || [ -z "$port" ] || [ -z "$connection_string" ]; then
        log_error "Failed to get resource information"
        exit 1
    fi

    # Configure JIT access before proceeding with database operations
    configure_jit_access "$environment" "$subscription_id"

    # Print connection details LAST (right before the tunnel), so the box isn't
    # buried under the JIT / IP-detection / VM-lookup output above it.
    print_box "$(to_upper "$db_type") Connection Info" "\
Server:     ${hostname}
Local Port: ${local_port:-$port}
Remote Port: ${port}

Connection String:
${BOLD}${connection_string/localhost/$'\n'localhost}${NC}"

    # For postgres, point the dev at the token helper (a sibling script, run
    # locally in a separate terminal). Print the resolved absolute path.
    if [ "$db_type" = "postgres" ]; then
        local login_script
        login_script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/db-login.sh"
        echo
        echo -e "${YELLOW}➜ Next: in a new terminal, run db-login.sh for a DB token (pgAdmin/psql/Rider):${NC}"
        echo -e "    ${BOLD}${login_script}${NC}"
    fi

    # Set up the SSH tunnel
    setup_ssh_tunnel "$environment" "${hostname}" "${port}" "${local_port:-$port}" "${shell_mode}" "${subscription_id}" "${no_prompt}" "${kill_stale}"
}

# =========================================================================
# Script Entry Point
# =========================================================================

# Parse command line arguments
environment=""
db_type=""
local_port=""
name_override=""
shell_mode="false"   # default: port-forward only (-N), no jumper shell / no MOTD
no_prompt="false"    # --no-prompt: never prompt; use flags + defaults, else fail
kill_stale="false"   # --kill-stale: if a stale tunnel holds the port, kill it (no prompt)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -e|--environment)
            require_option_value "$1" "${2:-}"
            environment="$2"
            shift 2
            ;;
        -t|--type)
            require_option_value "$1" "${2:-}"
            db_type="$2"
            shift 2
            ;;
        -n|--name)
            require_option_value "$1" "${2:-}"
            name_override="$2"
            shift 2
            ;;
        -p|--port)
            require_option_value "$1" "${2:-}"
            local_port="$2"
            shift 2
            ;;
        -s|--shell)
            shell_mode="true"
            shift
            ;;
        --no-prompt)
            no_prompt="true"
            shift
            ;;
        --kill-stale)
            kill_stale="true"
            shift
            ;;
        -h|--help)
            print_help
            exit 0
            ;;
        -c|--coffee)
            show_coffee_break
            shift
            ;;
        *)
            log_error "Invalid option: $1"
            log_info "Use -h or --help for help"
            exit 1
            ;;
    esac
done

# Call main with all arguments
main "${environment:-}" "${db_type:-}" "${local_port:-}" "${name_override:-}" "${shell_mode}" "${no_prompt}" "${kill_stale}"
