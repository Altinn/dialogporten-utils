#!/usr/bin/env bash
set -euo pipefail

# pgcopydb migration helper for Azure PostgreSQL Flexible Server.
#
# Defaults are set for:
#   source: dp-be-test-postgres-i7se3jtjey3lo
#   target: dp-be-test-postgres-i7se3jtjey3lo-v2
#
# Usage:
#   export SOURCE_PASSWORD='...'
#   export TARGET_PASSWORD='...'
#   ./run-pgcopydb-migration.sh start
#
#   # later, at cutover:
#   ./run-pgcopydb-migration.sh cutover
#
#   # inspect replication sentinel status:
#   ./run-pgcopydb-migration.sh status
#
#   # cleanup slot/origin/sentinel artifacts after completion:
#   ./run-pgcopydb-migration.sh cleanup

ACTION="${1:-start}"

SOURCE_SERVER="${SOURCE_SERVER:-dp-be-test-postgres-i7se3jtjey3lo}"
TARGET_SERVER="${TARGET_SERVER:-dp-be-test-postgres-i7se3jtjey3lo-v2}"

SOURCE_HOST="${SOURCE_HOST:-${SOURCE_SERVER}.postgres.database.azure.com}"
TARGET_HOST="${TARGET_HOST:-${TARGET_SERVER}.postgres.database.azure.com}"

SOURCE_DB="${SOURCE_DB:-dialogporten}"
TARGET_DB="${TARGET_DB:-dialogporten}"

SOURCE_USER="${SOURCE_USER:-dialogportenPgAdmin}"
TARGET_USER="${TARGET_USER:-dialogportenPgAdmin}"

PGPORT="${PGPORT:-5432}"
PGSSLMODE="${PGSSLMODE:-require}"

WORK_DIR="${WORK_DIR:-/tmp/pgcopydb-${SOURCE_SERVER}-to-${TARGET_SERVER}}"
PGPASSFILE_PATH="${PGPASSFILE_PATH:-${WORK_DIR}/.pgpass}"

TABLE_JOBS="${TABLE_JOBS:-4}"
INDEX_JOBS="${INDEX_JOBS:-4}"
RESTORE_JOBS="${RESTORE_JOBS:-4}"
SPLIT_TABLES_LARGER_THAN="${SPLIT_TABLES_LARGER_THAN:-10GB}"
DROP_IF_EXISTS="${DROP_IF_EXISTS:-false}"
OUTPUT_PLUGIN="${OUTPUT_PLUGIN:-}"
START_FRESH="${START_FRESH:-true}"
COPY_ROLES="${COPY_ROLES:-false}"
COPY_ROLE_PASSWORDS="${COPY_ROLE_PASSWORDS:-false}"
EXTRA_CLONE_ARGS="${EXTRA_CLONE_ARGS:-}"

SOURCE_PGURI="${SOURCE_PGURI:-postgres://${SOURCE_USER}@${SOURCE_HOST}:${PGPORT}/${SOURCE_DB}?sslmode=${PGSSLMODE}}"
TARGET_PGURI="${TARGET_PGURI:-postgres://${TARGET_USER}@${TARGET_HOST}:${PGPORT}/${TARGET_DB}?sslmode=${PGSSLMODE}}"
TARGET_ADMIN_PGURI="${TARGET_ADMIN_PGURI:-postgres://${TARGET_USER}@${TARGET_HOST}:${PGPORT}/postgres?sslmode=${PGSSLMODE}}"
SOURCE_ADMIN_PGURI="${SOURCE_ADMIN_PGURI:-postgres://${SOURCE_USER}@${SOURCE_HOST}:${PGPORT}/postgres?sslmode=${PGSSLMODE}}"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

require_non_empty() {
  local key="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    echo "ERROR: required variable '$key' is empty" >&2
    exit 1
  fi
}

write_pgpass() {
  mkdir -p "$WORK_DIR"
  umask 077
  cat >"$PGPASSFILE_PATH" <<EOF
${SOURCE_HOST}:${PGPORT}:${SOURCE_DB}:${SOURCE_USER}:${SOURCE_PASSWORD}
${SOURCE_HOST}:${PGPORT}:postgres:${SOURCE_USER}:${SOURCE_PASSWORD}
${TARGET_HOST}:${PGPORT}:${TARGET_DB}:${TARGET_USER}:${TARGET_PASSWORD}
${TARGET_HOST}:${PGPORT}:postgres:${TARGET_USER}:${TARGET_PASSWORD}
EOF
  chmod 600 "$PGPASSFILE_PATH"
  export PGPASSFILE="$PGPASSFILE_PATH"
}

ensure_target_db_exists() {
  local exists
  exists="$(psql "$TARGET_ADMIN_PGURI" -Atqc "SELECT 1 FROM pg_database WHERE datname = '${TARGET_DB}'")"
  if [[ "$exists" != "1" ]]; then
    echo "Target database '${TARGET_DB}' does not exist; creating it..."
    psql "$TARGET_ADMIN_PGURI" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${TARGET_DB}\""
  fi
}

check_source_logical_settings() {
  local wal_level slots senders
  wal_level="$(psql "$SOURCE_ADMIN_PGURI" -Atqc "SHOW wal_level;")"
  slots="$(psql "$SOURCE_ADMIN_PGURI" -Atqc "SHOW max_replication_slots;")"
  senders="$(psql "$SOURCE_ADMIN_PGURI" -Atqc "SHOW max_wal_senders;")"

  echo "Source logical settings: wal_level=${wal_level}, max_replication_slots=${slots}, max_wal_senders=${senders}"

  if [[ "$wal_level" != "logical" ]]; then
    echo "ERROR: source wal_level must be 'logical' for pgcopydb --follow." >&2
    exit 1
  fi
  if [[ "${slots:-0}" -lt 1 ]]; then
    echo "ERROR: source max_replication_slots must be >= 1." >&2
    exit 1
  fi
  if [[ "${senders:-0}" -lt 1 ]]; then
    echo "ERROR: source max_wal_senders must be >= 1." >&2
    exit 1
  fi
}

print_config() {
  echo "Configuration:"
  echo "  ACTION=${ACTION}"
  echo "  SOURCE_SERVER=${SOURCE_SERVER}"
  echo "  TARGET_SERVER=${TARGET_SERVER}"
  echo "  SOURCE_HOST=${SOURCE_HOST}"
  echo "  TARGET_HOST=${TARGET_HOST}"
  echo "  SOURCE_DB=${SOURCE_DB}"
  echo "  TARGET_DB=${TARGET_DB}"
  echo "  SOURCE_USER=${SOURCE_USER}"
  echo "  TARGET_USER=${TARGET_USER}"
  echo "  WORK_DIR=${WORK_DIR}"
  echo "  TABLE_JOBS=${TABLE_JOBS}"
  echo "  INDEX_JOBS=${INDEX_JOBS}"
  echo "  RESTORE_JOBS=${RESTORE_JOBS}"
  echo "  SPLIT_TABLES_LARGER_THAN=${SPLIT_TABLES_LARGER_THAN}"
  echo "  DROP_IF_EXISTS=${DROP_IF_EXISTS}"
  echo "  OUTPUT_PLUGIN=${OUTPUT_PLUGIN:-<default>}"
  echo "  START_FRESH=${START_FRESH}"
  echo "  COPY_ROLES=${COPY_ROLES}"
  echo "  COPY_ROLE_PASSWORDS=${COPY_ROLE_PASSWORDS}"
  echo "  EXTRA_CLONE_ARGS=${EXTRA_CLONE_ARGS:-<none>}"
}

setup_common() {
  require_cmd pgcopydb
  require_cmd psql
  require_non_empty "SOURCE_PASSWORD" "${SOURCE_PASSWORD:-}"
  require_non_empty "TARGET_PASSWORD" "${TARGET_PASSWORD:-}"
  write_pgpass
  print_config
}

do_start() {
  setup_common

  if [[ "$START_FRESH" == "true" ]]; then
    echo "START_FRESH=true: cleaning previous pgcopydb stream state and work directory..."
    pgcopydb stream cleanup --source "$SOURCE_PGURI" --target "$TARGET_PGURI" --dir "$WORK_DIR" || true
    rm -rf "$WORK_DIR"
    write_pgpass
  fi

  echo "Pinging source/target..."
  pgcopydb ping --source "$SOURCE_PGURI" --target "$TARGET_PGURI"

  check_source_logical_settings
  ensure_target_db_exists

  local cmd=(
    pgcopydb clone --follow
    --source "$SOURCE_PGURI"
    --target "$TARGET_PGURI"
    --dir "$WORK_DIR"
    --table-jobs "$TABLE_JOBS"
    --index-jobs "$INDEX_JOBS"
    --restore-jobs "$RESTORE_JOBS"
    --split-tables-larger-than "$SPLIT_TABLES_LARGER_THAN"
  )

  if [[ "$DROP_IF_EXISTS" == "true" ]]; then
    cmd+=(--drop-if-exists)
  fi

  if [[ -n "$OUTPUT_PLUGIN" ]]; then
    cmd+=(--plugin "$OUTPUT_PLUGIN")
  fi

  if [[ "$COPY_ROLES" == "true" ]]; then
    cmd+=(--roles)
    if [[ "$COPY_ROLE_PASSWORDS" != "true" ]]; then
      cmd+=(--no-role-passwords)
    fi
  fi

  if [[ -n "$EXTRA_CLONE_ARGS" ]]; then
    # Allow advanced operator overrides without editing this script.
    # Intentionally uses shell word-splitting semantics.
    read -r -a extra_args <<<"$EXTRA_CLONE_ARGS"
    cmd+=("${extra_args[@]}")
  fi

  echo "Starting pgcopydb clone --follow..."
  echo "Run this script with 'cutover' later to stop at a controlled end LSN."
  "${cmd[@]}"
}

do_status() {
  setup_common
  pgcopydb stream sentinel get --source "$SOURCE_PGURI" --dir "$WORK_DIR" --json || true
}

do_cutover() {
  setup_common
  echo "Setting cutover end position to current source WAL flush LSN..."
  pgcopydb stream sentinel set endpos --source "$SOURCE_PGURI" --dir "$WORK_DIR" --current
  echo "Cutover signal set. Wait for the running 'clone --follow' process to finish."
}

do_cleanup() {
  setup_common
  echo "Cleaning up pgcopydb logical decoding artifacts..."
  pgcopydb stream cleanup --source "$SOURCE_PGURI" --target "$TARGET_PGURI" --dir "$WORK_DIR"
}

case "$ACTION" in
  start)
    do_start
    ;;
  status)
    do_status
    ;;
  cutover)
    do_cutover
    ;;
  cleanup)
    do_cleanup
    ;;
  *)
    echo "Usage: $0 {start|status|cutover|cleanup}" >&2
    exit 2
    ;;
esac
