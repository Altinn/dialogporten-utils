#!/usr/bin/env bash
set -euo pipefail

# Parallel stop/start for all Container Apps in one Container Apps Environment.
#
# Usage:
#   ./cae-apps-parallel.sh stop  <resource-group> <environment> [--parallelism N]
#   ./cae-apps-parallel.sh start <resource-group> <environment> [--revision REV] [--parallelism N]
#
# Examples:
#   ./cae-apps-parallel.sh stop  dp-be-test-rg dp-be-test-cae
#   ./cae-apps-parallel.sh start dp-be-test-rg dp-be-test-cae
#   ./cae-apps-parallel.sh start dp-be-test-rg dp-be-test-cae --revision abc123
#   ./cae-apps-parallel.sh start dp-be-test-rg dp-be-test-cae --revision '{app}--abc123'
#   ./cae-apps-parallel.sh start dp-be-test-rg dp-be-test-cae --parallelism 10

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

usage() {
  cat <<'EOF'
Usage:
  cae-apps-parallel.sh stop  <resource-group> <environment> [--parallelism N]
  cae-apps-parallel.sh start <resource-group> <environment> [--revision REV] [--parallelism N]

Revision formats for --revision:
  abc123            => resolves per app to <app>--abc123
  {app}--abc123     => explicit template
  myapp--abc123     => exact revision string
EOF
}

if [[ $# -lt 3 ]]; then
  usage
  exit 2
fi

MODE="$1"
RG="$2"
ENV_NAME="$3"
shift 3

REVISION=""
PARALLELISM="${PARALLELISM:-6}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --revision)
      REVISION="${2:-}"
      shift 2
      ;;
    --parallelism)
      PARALLELISM="${2:-}"
      shift 2
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ "$MODE" != "stop" && "$MODE" != "start" ]]; then
  echo "ERROR: mode must be 'stop' or 'start'" >&2
  exit 2
fi

if [[ "$MODE" == "stop" && -n "$REVISION" ]]; then
  echo "ERROR: --revision is only valid with 'start'" >&2
  exit 2
fi

if ! [[ "$PARALLELISM" =~ ^[0-9]+$ ]] || [[ "$PARALLELISM" -lt 1 ]]; then
  echo "ERROR: --parallelism must be an integer >= 1" >&2
  exit 2
fi

require_cmd az
require_cmd xargs

mapfile -t APPS < <(az containerapp list -g "$RG" --environment "$ENV_NAME" --query "[].name" -o tsv)

if [[ "${#APPS[@]}" -eq 0 ]]; then
  echo "No container apps found in env '$ENV_NAME' (rg '$RG')."
  exit 0
fi

echo "Mode=$MODE Apps=${#APPS[@]} Parallelism=$PARALLELISM Revision=${REVISION:-<auto>}"

stop_one() {
  local app="$1"
  local rg="$2"
  mapfile -t revs < <(az containerapp revision list -g "$rg" -n "$app" --query "[?properties.active].name" -o tsv)

  if [[ "${#revs[@]}" -eq 0 ]]; then
    echo "[$app] no active revisions"
    return 0
  fi

  local rev
  for rev in "${revs[@]}"; do
    az containerapp revision deactivate -g "$rg" -n "$app" --revision "$rev" --only-show-errors 1>/dev/null
    echo "[$app] deactivated $rev"
  done
}

start_one() {
  local app="$1"
  local rg="$2"
  local revision="$3"
  local rev=""

  if [[ -n "$revision" ]]; then
    if [[ "$revision" == *"{app}"* ]]; then
      rev="${revision//\{app\}/$app}"
    elif [[ "$revision" == *"--"* ]]; then
      rev="$revision"
    else
      rev="${app}--${revision}"
    fi
  else
    rev="$(az containerapp show -g "$rg" -n "$app" --query "properties.latestRevisionName" -o tsv)"
    if [[ -z "$rev" || "$rev" == "None" ]]; then
      rev="$(az containerapp revision list -g "$rg" -n "$app" --query "sort_by(@,&properties.createdTime)[-1].name" -o tsv)"
    fi
  fi

  if [[ -z "$rev" || "$rev" == "None" ]]; then
    echo "[$app] ERROR: could not determine revision to activate" >&2
    return 1
  fi

  az containerapp revision activate -g "$rg" -n "$app" --revision "$rev" --only-show-errors 1>/dev/null
  echo "[$app] activated $rev"
}

export -f stop_one
export -f start_one
export RG
export REVISION

if [[ "$MODE" == "stop" ]]; then
  printf '%s\n' "${APPS[@]}" | xargs -P "$PARALLELISM" -n 1 -I {} bash -c 'stop_one "$1" "$2"' _ {} "$RG"
else
  printf '%s\n' "${APPS[@]}" | xargs -P "$PARALLELISM" -n 1 -I {} bash -c 'start_one "$1" "$2" "$3"' _ {} "$RG" "$REVISION"
fi
