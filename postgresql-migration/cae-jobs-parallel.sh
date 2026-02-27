#!/usr/bin/env bash
set -euo pipefail

# Parallel pause/resume/status for scheduled Container App Jobs in one resource group.
#
# Pause strategy:
# - Save each job's current cron expression in a tag (TAG_KEY) if not already present.
# - Update cron to PAUSE_CRON to prevent normal scheduling during cutover.
# - Stop currently running executions.
#
# Resume strategy:
# - Read saved cron expression from tag (TAG_KEY).
# - Restore cron expression.

usage() {
  cat <<'USAGE'
Usage:
  cae-jobs-parallel.sh pause  <resource-group> [--parallelism N] [--pause-cron EXPR] [--tag-key KEY]
  cae-jobs-parallel.sh resume <resource-group> [--parallelism N] [--tag-key KEY]
  cae-jobs-parallel.sh status <resource-group> [--parallelism N] [--tag-key KEY]

Defaults:
  --parallelism  8
  --pause-cron   "0 0 1 1 *"
  --tag-key      pgMigrationOriginalCron
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

get_scheduled_jobs() {
  local rg="$1"
  az containerapp job list -g "$rg" --query "[?properties.configuration.triggerType=='Schedule'].name" -o tsv
}

get_job_id() {
  local rg="$1"
  local job="$2"
  az containerapp job show -g "$rg" -n "$job" --query id -o tsv
}

get_job_cron() {
  local rg="$1"
  local job="$2"
  az containerapp job show -g "$rg" -n "$job" --query "properties.configuration.scheduleTriggerConfig.cronExpression" -o tsv
}

get_saved_cron() {
  local rg="$1"
  local job="$2"
  local tag_key="$3"
  az containerapp job show -g "$rg" -n "$job" --query "tags.\"${tag_key}\"" -o tsv 2>/dev/null || true
}

pause_one() {
  local job="$1"
  local rg="$2"
  local pause_cron="$3"
  local tag_key="$4"

  local current_cron job_id
  current_cron="$(get_job_cron "$rg" "$job")"

  if [[ -z "$current_cron" || "$current_cron" == "None" ]]; then
    echo "[$job] skipped: empty cron expression"
    return 0
  fi

  job_id="$(get_job_id "$rg" "$job")"
  az tag update --resource-id "$job_id" --operation Merge --tags "${tag_key}=${current_cron}" --only-show-errors >/dev/null

  az containerapp job update -g "$rg" -n "$job" --cron-expression "$pause_cron" --only-show-errors >/dev/null
  az containerapp job stop -g "$rg" -n "$job" --only-show-errors >/dev/null || true
  echo "[$job] paused (cron=${current_cron} -> ${pause_cron})"
}

resume_one() {
  local job="$1"
  local rg="$2"
  local tag_key="$3"

  local saved_cron
  saved_cron="$(get_saved_cron "$rg" "$job" "$tag_key")"

  if [[ -z "$saved_cron" || "$saved_cron" == "None" ]]; then
    echo "[$job] skipped: no saved cron tag '${tag_key}'"
    return 0
  fi

  az containerapp job update -g "$rg" -n "$job" --cron-expression "$saved_cron" --only-show-errors >/dev/null
  echo "[$job] resumed (cron=${saved_cron})"
}

status_one() {
  local job="$1"
  local rg="$2"
  local tag_key="$3"

  local current_cron saved_cron
  current_cron="$(get_job_cron "$rg" "$job")"
  saved_cron="$(get_saved_cron "$rg" "$job" "$tag_key")"

  echo "job=${job} cron=${current_cron} saved=${saved_cron:-<none>}"
}

main() {
  require_cmd az
  require_cmd xargs

  if [[ $# -lt 2 ]]; then
    usage
    exit 2
  fi

  local mode="$1"
  local rg="$2"
  shift 2

  local parallelism="${PARALLELISM:-8}"
  local pause_cron="${PAUSE_CRON:-0 0 1 1 *}"
  local tag_key="${TAG_KEY:-pgMigrationOriginalCron}"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --parallelism)
        parallelism="${2:-}"
        shift 2
        ;;
      --pause-cron)
        pause_cron="${2:-}"
        shift 2
        ;;
      --tag-key)
        tag_key="${2:-}"
        shift 2
        ;;
      *)
        echo "ERROR: unknown argument: $1" >&2
        usage
        exit 2
        ;;
    esac
  done

  if [[ "$mode" != "pause" && "$mode" != "resume" && "$mode" != "status" ]]; then
    echo "ERROR: mode must be pause|resume|status" >&2
    exit 2
  fi

  if ! [[ "$parallelism" =~ ^[0-9]+$ ]] || [[ "$parallelism" -lt 1 ]]; then
    echo "ERROR: --parallelism must be integer >= 1" >&2
    exit 2
  fi

  mapfile -t jobs < <(get_scheduled_jobs "$rg")

  if [[ "${#jobs[@]}" -eq 0 ]]; then
    echo "No scheduled Container App Jobs found in resource group '$rg'."
    exit 0
  fi

  echo "Mode=$mode Jobs=${#jobs[@]} Parallelism=$parallelism TagKey=$tag_key"
  if [[ "$mode" == "pause" ]]; then
    echo "PauseCron=$pause_cron"
  fi

  export -f get_job_id get_job_cron get_saved_cron pause_one resume_one status_one

  if [[ "$mode" == "pause" ]]; then
    # shellcheck disable=SC2016
    printf '%s\n' "${jobs[@]}" | xargs -P "$parallelism" -n 1 -I {} bash -c 'pause_one "$1" "$2" "$3" "$4"' _ {} "$rg" "$pause_cron" "$tag_key"
  elif [[ "$mode" == "resume" ]]; then
    # shellcheck disable=SC2016
    printf '%s\n' "${jobs[@]}" | xargs -P "$parallelism" -n 1 -I {} bash -c 'resume_one "$1" "$2" "$3"' _ {} "$rg" "$tag_key"
  else
    # shellcheck disable=SC2016
    printf '%s\n' "${jobs[@]}" | xargs -P "$parallelism" -n 1 -I {} bash -c 'status_one "$1" "$2" "$3"' _ {} "$rg" "$tag_key"
  fi
}

main "$@"
