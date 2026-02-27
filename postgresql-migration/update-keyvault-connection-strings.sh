#!/usr/bin/env bash
set -euo pipefail

SECRET_ADO="dialogportenAdoConnectionString"
SECRET_PSQL="dialogportenPsqlConnectionString"
DB_NAME="dialogporten"
DB_PORT="5432"

usage() {
  cat <<'USAGE'
Usage:
  update-keyvault-connection-strings.sh get --resource-group <rg>
  update-keyvault-connection-strings.sh update --resource-group <rg> --server <server-name> --username <db-user> --password <db-password>
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: missing required command: $cmd" >&2
    exit 1
  fi
}

resolve_keyvault() {
  local rg="$1"

  local count
  count="$(az keyvault list -g "$rg" --query "length(@)" -o tsv)"

  if [[ "$count" != "1" ]]; then
    echo "ERROR: expected exactly one key vault in resource group '$rg', found ${count:-0}" >&2
    az keyvault list -g "$rg" --query "[].name" -o tsv || true
    exit 1
  fi

  az keyvault list -g "$rg" --query "[0].name" -o tsv
}

get_secret_value() {
  local vault="$1"
  local name="$2"
  az keyvault secret show --vault-name "$vault" --name "$name" --query value -o tsv
}

build_ado_string() {
  local server="$1"
  local user="$2"
  local pass="$3"

  local host="${server}.postgres.database.azure.com"
  printf 'Server=%s;Database=%s;Port=%s;User Id=%s;Password=%s;Ssl Mode=Require;Trust Server Certificate=true;Include Error Detail=True;' \
    "$host" "$DB_NAME" "$DB_PORT" "$user" "$pass"
}

build_psql_string() {
  local server="$1"
  local user="$2"
  local pass="$3"

  local host="${server}.postgres.database.azure.com"
  printf "psql 'host=%s port=%s dbname=%s user=%s password=%s sslmode=require'" \
    "$host" "$DB_PORT" "$DB_NAME" "$user" "$pass"
}

confirm_overwrite() {
  local answer
  read -r -p "Continue? [yN] " answer
  case "$answer" in
    y|Y|yes|YES)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

main() {
  require_cmd az

  local mode="${1:-}"
  if [[ -z "$mode" ]]; then
    usage
    exit 2
  fi
  shift

  local rg=""
  local server=""
  local username=""
  local password=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --resource-group|-g)
        rg="${2:-}"
        shift 2
        ;;
      --server)
        server="${2:-}"
        shift 2
        ;;
      --username)
        username="${2:-}"
        shift 2
        ;;
      --password)
        password="${2:-}"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "ERROR: unknown argument: $1" >&2
        usage
        exit 2
        ;;
    esac
  done

  if [[ -z "$rg" ]]; then
    echo "ERROR: --resource-group is required" >&2
    usage
    exit 2
  fi

  if [[ "$mode" != "get" && "$mode" != "update" ]]; then
    echo "ERROR: mode must be 'get' or 'update'" >&2
    usage
    exit 2
  fi

  if [[ "$mode" == "update" ]]; then
    if [[ -z "$server" || -z "$username" || -z "$password" ]]; then
      echo "ERROR: update requires --server, --username and --password" >&2
      usage
      exit 2
    fi
  fi

  echo "Determining key vault for '$rg' ..."
  local vault
  vault="$(resolve_keyvault "$rg")"

  if [[ "$mode" == "get" ]]; then
    echo "Getting secrets from '$vault' ..."
    echo

    local current_psql current_ado
    current_psql="$(get_secret_value "$vault" "$SECRET_PSQL")"
    current_ado="$(get_secret_value "$vault" "$SECRET_ADO")"

    echo "PSQL: $current_psql"
    echo "ADO: $current_ado"
    exit 0
  fi

  local new_psql new_ado
  new_psql="$(build_psql_string "$server" "$username" "$password")"
  new_ado="$(build_ado_string "$server" "$username" "$password")"

  echo
  echo "About to set these values in '$vault'"
  echo
  echo "PSQL: $new_psql"
  echo "ADO: $new_ado"
  echo

  if ! confirm_overwrite; then
    echo "Cancelled."
    exit 0
  fi

  echo
  echo "Setting values ..."
  az keyvault secret set --vault-name "$vault" --name "$SECRET_PSQL" --value "$new_psql" --only-show-errors >/dev/null
  az keyvault secret set --vault-name "$vault" --name "$SECRET_ADO" --value "$new_ado" --only-show-errors >/dev/null
  echo "Done!"
}

main "$@"
