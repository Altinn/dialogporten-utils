#!/usr/bin/env bash
# =========================================================================
# pg-token.sh — emit a Microsoft Entra access token for Azure PostgreSQL.
#
# Intended for use as pgAdmin's "Password exec command" so pgAdmin can
# auto-renew the token on (re)connect. On success, outputs ONLY the raw
# token to stdout (no trailing newline) so pgAdmin uses it verbatim.
#
# pgAdmin captures this script's EXIT CODE but discards its stderr, so we
# keep it minimal: a valid token, or a clean non-zero exit. Friendly,
# context-aware error messages (wrong identity, not a group member, etc.)
# belong in the interactive db-login.sh wrapper, which can actually show
# them — not here, where pgAdmin would swallow them.
#
# The wrong-identity case (valid token, but the active az identity is not a
# member of the target group) is intentionally left to PostgreSQL to reject:
# PG's "principal with oid ... isn't a member" error, while cryptic, at least
# states the actual problem, whereas a pre-flight refusal here would surface
# only "command returned non-zero exit status 1".
#
# pgAdmin runs this in a minimal environment, so resolve `az` by absolute
# path rather than relying on PATH.
# =========================================================================
set -uo pipefail

# --- locate az (pgAdmin's PATH will not find a bare `az`) ------------------
AZ=""
for cand in "${AZ_BIN:-}" /opt/homebrew/bin/az /usr/local/bin/az "$(command -v az 2>/dev/null || true)"; do
  if [ -n "$cand" ] && [ -x "$cand" ]; then AZ="$cand"; break; fi
done
if [ -z "$AZ" ]; then
  echo "pg-token: could not find the 'az' CLI. Install Azure CLI or set AZ_BIN." >&2
  exit 1
fi

# --- emit the token (empty/failure -> non-zero exit) -----------------------
token="$("$AZ" account get-access-token --resource-type oss-rdbms --query accessToken -o tsv 2>/dev/null || true)"
if [ -z "$token" ]; then
  echo "pg-token: failed to acquire an Azure PostgreSQL access token (run: az login)." >&2
  exit 1
fi
printf '%s' "$token"
