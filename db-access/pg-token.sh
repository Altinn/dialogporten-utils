#!/usr/bin/env bash
# =========================================================================
# pg-token.sh — emit a Microsoft Entra access token for Azure PostgreSQL.
#
# Intended for use as pgAdmin's "Password exec command" so pgAdmin can
# auto-renew the token on (re)connect. On success, outputs ONLY the raw
# token to stdout (no trailing newline) so pgAdmin uses it verbatim.
#
# Usage:
#   pg-token.sh [ENV]
#     ENV (optional): test | yt01 | staging | prod
#       When given, the token is requested SCOPED TO that environment's
#       subscription (`--subscription`), so it is issued for whichever
#       logged-in account OWNS that subscription — regardless of which
#       account is currently "active". This means you can be logged into
#       both the test and prod identities at once and pgAdmin will always
#       get the RIGHT identity's token for the server it is connecting to,
#       without anyone running `az account set`.
#     No ENV: falls back to the active account (manual / single-account use).
#
# The db-login.sh --export-pgadmin generator wires this up per-server, e.g.
#   "PasswordExecCommand": ".../pg-token.sh test"   (for AT23/yt01 servers)
#   "PasswordExecCommand": ".../pg-token.sh prod"   (for staging/prod servers)
#
# pgAdmin captures this script's EXIT CODE but discards its stderr, so we
# keep it minimal: a valid token, or a clean non-zero exit. Friendly,
# context-aware error messages (wrong identity, not a group member, etc.)
# belong in the interactive db-login.sh wrapper, which can actually show
# them — not here, where pgAdmin would swallow them.
#
# The wrong-identity case (valid token, but that identity is not a member of
# the target group) is intentionally left to PostgreSQL to reject: PG's
# "principal with oid ... isn't a member" error, while cryptic, at least
# states the actual problem, whereas a pre-flight refusal here would surface
# only "command returned non-zero exit status 1".
#
# pgAdmin runs this in a minimal environment, so resolve `az` by absolute
# path rather than relying on PATH.
# =========================================================================
set -uo pipefail

# env -> the Azure subscription that env's DB lives in. Keep in sync with
# db-login.sh env_subscription() and forward.sh get_subscription_name().
env_subscription() {
  case "$1" in
    test|yt01) echo "Dialogporten-Test" ;;
    staging)   echo "Dialogporten-Staging" ;;
    prod)      echo "Dialogporten-Prod" ;;
    *)         echo "" ;;
  esac
}

ENV_ARG="${1:-}"

# --- locate az (pgAdmin's PATH will not find a bare `az`) ------------------
AZ=""
for cand in "${AZ_BIN:-}" /opt/homebrew/bin/az /usr/local/bin/az "$(command -v az 2>/dev/null || true)"; do
  if [ -n "$cand" ] && [ -x "$cand" ]; then AZ="$cand"; break; fi
done
if [ -z "$AZ" ]; then
  echo "pg-token: could not find the 'az' CLI. Install Azure CLI or set AZ_BIN." >&2
  exit 1
fi

# --- build the token request -----------------------------------------------
# If an ENV was given, scope the request to that env's subscription so the
# token is issued for the OWNING account (no active-account switch needed).
# An unrecognized ENV is treated as "no scoping" (use the active account).
sub=""
if [ -n "$ENV_ARG" ]; then
  sub="$(env_subscription "$ENV_ARG")"
fi

set -- account get-access-token --resource-type oss-rdbms --query accessToken -o tsv
[ -n "$sub" ] && set -- "$@" --subscription "$sub"

# --- emit the token (empty/failure -> non-zero exit) -----------------------
token="$("$AZ" "$@" 2>/dev/null || true)"
if [ -z "$token" ]; then
  if [ -n "$sub" ]; then
    echo "pg-token: failed to acquire a token for subscription '${sub}'. Are you logged into the account that owns it? (az login)" >&2
  else
    echo "pg-token: failed to acquire an Azure PostgreSQL access token (run: az login)." >&2
  fi
  exit 1
fi
printf '%s' "$token"
