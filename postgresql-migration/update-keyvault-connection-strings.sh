#!/usr/bin/env bash
set -euo pipefail

SECRET_ADO="dialogportenAdoConnectionString"
SECRET_PSQL="dialogportenPsqlConnectionString"

usage() {
  cat <<'USAGE'
Usage:
  update-keyvault-connection-strings.sh show <server-name>
  update-keyvault-connection-strings.sh update <server-name> <db-user> <db-password>

Arguments:
  server-name    PostgreSQL server name without DNS suffix,
                 for example: dp-be-test-postgres-i7se3jtjey3lo-v2

Behavior:
  - Derives environment from server-name prefix dp-be-<env>-...
  - Uses resource group: dp-be-<env>-rg (or RESOURCE_GROUP override)
  - Finds the single key vault in that RG matching dp-be-<env>-keyvault-
  - Reads/updates:
      dialogportenAdoConnectionString
      dialogportenPsqlConnectionString
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[\\/&]/\\&/g'
}

extract_env() {
  local server_name="$1"
  if [[ "$server_name" =~ ^dp-be-([a-z0-9]+)- ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  echo "ERROR: could not derive environment from server name: $server_name" >&2
  echo "Expected pattern: dp-be-<env>-..." >&2
  return 1
}

find_keyvault() {
  local rg="$1"
  local env_name="$2"
  local prefix="dp-be-${env_name}-keyvault-"

  local count
  count="$(az keyvault list -g "$rg" --query "[?starts_with(name, '${prefix}')].name | length(@)" -o tsv)"

  if [[ "$count" != "1" ]]; then
    echo "ERROR: expected exactly 1 key vault in '$rg' with prefix '$prefix', found: ${count:-0}" >&2
    az keyvault list -g "$rg" --query "[?starts_with(name, '${prefix}')].name" -o tsv || true
    return 1
  fi

  az keyvault list -g "$rg" --query "[?starts_with(name, '${prefix}')].name | [0]" -o tsv
}

extract_host() {
  local value="$1"

  if [[ "$value" == *"://"* ]]; then
    printf '%s' "$value" | sed -E 's#^[a-zA-Z][a-zA-Z0-9+.-]*://([^@/]+@)?([^/:?]+).*$#\2#'
    return 0
  fi

  if [[ "$value" == *";"* && "$value" == *"="* ]]; then
    printf '%s' "$value" \
      | tr ';' '\n' \
      | sed -n -E 's/^[[:space:]]*([Hh]ost|[Ss]erver)[[:space:]]*=[[:space:]]*//p' \
      | head -n1
    return 0
  fi

  if [[ "$value" == *"host="* ]]; then
    printf '%s' "$value" | sed -n -E 's/.*(^|[[:space:]])host=([^[:space:]]+).*/\2/p'
    return 0
  fi

  printf 'unknown'
}

rewrite_connection_string() {
  local value="$1"
  local target_fqdn="$2"
  local db_user="$3"
  local db_password="$4"

  local escaped_host escaped_user escaped_password
  escaped_host="$(escape_sed_replacement "$target_fqdn")"
  escaped_user="$(escape_sed_replacement "$db_user")"
  escaped_password="$(escape_sed_replacement "$db_password")"

  local updated="$value"

  # URI format, e.g. postgres://user:pass@host:5432/db?...
  if [[ "$value" == *"://"* ]]; then
    updated="$(printf '%s' "$value" | sed -E "s#^([a-zA-Z][a-zA-Z0-9+.-]*://)([^@/]*@)?([^/:?]+)#\\1${escaped_user}:${escaped_password}@${escaped_host}#")"
    printf '%s' "$updated"
    return 0
  fi

  # Semicolon key-value format, e.g. Host=...;Username=...;Password=...
  if [[ "$value" == *";"* && "$value" == *"="* ]]; then
    updated="$(printf '%s' "$value" | sed -E \
      -e "s/(^|;)[[:space:]]*[Hh]ost[[:space:]]*=[^;]*/\\1Host=${escaped_host}/" \
      -e "s/(^|;)[[:space:]]*[Ss]erver[[:space:]]*=[^;]*/\\1Server=${escaped_host}/" \
      -e "s/(^|;)[[:space:]]*[Uu]ser[Nn]ame[[:space:]]*=[^;]*/\\1Username=${escaped_user}/" \
      -e "s/(^|;)[[:space:]]*[Uu]ser[[:space:]]*[Ii][Dd][[:space:]]*=[^;]*/\\1User ID=${escaped_user}/" \
      -e "s/(^|;)[[:space:]]*[Pp]assword[[:space:]]*=[^;]*/\\1Password=${escaped_password}/")"

    [[ "$updated" =~ [Hh]ost[[:space:]]*=|[Ss]erver[[:space:]]*= ]] || updated="${updated%;};Host=${target_fqdn}"
    [[ "$updated" =~ [Uu]ser[Nn]ame[[:space:]]*=|[Uu]ser[[:space:]]*[Ii][Dd][[:space:]]*= ]] || updated="${updated%;};Username=${db_user}"
    [[ "$updated" =~ [Pp]assword[[:space:]]*= ]] || updated="${updated%;};Password=${db_password}"

    printf '%s' "$updated"
    return 0
  fi

  # Space separated key-value format, e.g. host=... port=... user=... password=...
  if [[ "$value" == *"="* ]]; then
    updated="$(printf '%s' "$value" | sed -E \
      -e "s/(^|[[:space:]])host=[^[:space:]]+/\\1host=${escaped_host}/" \
      -e "s/(^|[[:space:]])user=[^[:space:]]+/\\1user=${escaped_user}/" \
      -e "s/(^|[[:space:]])password=[^[:space:]]+/\\1password=${escaped_password}/")"

    [[ "$updated" =~ (^|[[:space:]])host= ]] || updated="${updated} host=${target_fqdn}"
    [[ "$updated" =~ (^|[[:space:]])user= ]] || updated="${updated} user=${db_user}"
    [[ "$updated" =~ (^|[[:space:]])password= ]] || updated="${updated} password=${db_password}"

    printf '%s' "$updated"
    return 0
  fi

  echo "ERROR: unsupported connection string format" >&2
  return 1
}

show_secret() {
  local vault_name="$1"
  local secret_name="$2"

  local id updated enabled value host
  id="$(az keyvault secret show --vault-name "$vault_name" --name "$secret_name" --query id -o tsv)"
  updated="$(az keyvault secret show --vault-name "$vault_name" --name "$secret_name" --query attributes.updated -o tsv)"
  enabled="$(az keyvault secret show --vault-name "$vault_name" --name "$secret_name" --query attributes.enabled -o tsv)"
  value="$(az keyvault secret show --vault-name "$vault_name" --name "$secret_name" --query value -o tsv)"
  host="$(extract_host "$value")"

  echo "secret=${secret_name}"
  echo "  id=${id}"
  echo "  updated=${updated}"
  echo "  enabled=${enabled}"
  echo "  host=${host}"
}

update_secret() {
  local vault_name="$1"
  local secret_name="$2"
  local target_fqdn="$3"
  local db_user="$4"
  local db_password="$5"

  local current_value current_id new_value new_id
  current_id="$(az keyvault secret show --vault-name "$vault_name" --name "$secret_name" --query id -o tsv)"
  current_value="$(az keyvault secret show --vault-name "$vault_name" --name "$secret_name" --query value -o tsv)"
  new_value="$(rewrite_connection_string "$current_value" "$target_fqdn" "$db_user" "$db_password")"

  new_id="$(az keyvault secret set --vault-name "$vault_name" --name "$secret_name" --value "$new_value" --query id -o tsv)"

  echo "secret=${secret_name}"
  echo "  previousVersion=${current_id}"
  echo "  newVersion=${new_id}"
}

main() {
  require_cmd az
  require_cmd sed
  require_cmd tr

  local mode="${1:-}"
  if [[ -z "$mode" ]]; then
    usage
    exit 2
  fi
  shift

  local server_name="${1:-}"
  if [[ -z "$server_name" ]]; then
    usage
    exit 2
  fi
  shift

  local db_user=""
  local db_password=""

  if [[ "$mode" == "update" ]]; then
    db_user="${1:-}"
    db_password="${2:-}"

    if [[ -z "$db_user" || -z "$db_password" ]]; then
      echo "ERROR: update mode requires <db-user> and <db-password>" >&2
      usage
      exit 2
    fi
  elif [[ "$mode" != "show" ]]; then
    echo "ERROR: mode must be 'show' or 'update'" >&2
    usage
    exit 2
  fi

  local env_name rg kv_name target_fqdn
  env_name="$(extract_env "$server_name")"
  rg="${RESOURCE_GROUP:-dp-be-${env_name}-rg}"
  kv_name="$(find_keyvault "$rg" "$env_name")"
  target_fqdn="${server_name}.postgres.database.azure.com"

  echo "Resolved context:"
  echo "  env=${env_name}"
  echo "  rg=${rg}"
  echo "  keyvault=${kv_name}"
  echo "  targetFqdn=${target_fqdn}"

  case "$mode" in
    show)
      echo "Current secret versions:" 
      show_secret "$kv_name" "$SECRET_ADO"
      show_secret "$kv_name" "$SECRET_PSQL"
      ;;
    update)
      echo "Updating secrets..."
      update_secret "$kv_name" "$SECRET_ADO" "$target_fqdn" "$db_user" "$db_password"
      update_secret "$kv_name" "$SECRET_PSQL" "$target_fqdn" "$db_user" "$db_password"

      echo "Post-update versions:"
      show_secret "$kv_name" "$SECRET_ADO"
      show_secret "$kv_name" "$SECRET_PSQL"
      ;;
  esac
}

main "$@"
