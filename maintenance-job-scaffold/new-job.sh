#!/usr/bin/env bash
#
# Scaffold a new one-off Postgres maintenance job from _template/.
#
# Usage:
#   ./new-job.sh <job-name-in-kebab-case> [target-parent-dir]
#
#   ./new-job.sh a1-status-fix
#       -> creates ../a1-status-fix/ (sibling of this scaffold dir)
#   ./new-job.sh a1-status-fix /some/where
#       -> creates /some/where/a1-status-fix/
#
# The single kebab-case argument is the only name you supply; all the casings
# used inside the generated files are derived from it:
#   a1-status-fix  ->  A1StatusFix  (PascalCase: table/index/view prefix)
#                  ->  a1statusfix  (lower, no hyphens: procedure name)
#                  ->  a1-status-fix (kebab: dir name, application_name, docs)
#
# After generating, fill in the marked edit points:
#   grep -rn 'TODO(job)' <job-dir>/
# The job is fully filled in when this returns nothing:
#   grep -rn 'TODO(job)\|TODO_\|{{' <job-dir>/
#
# See AGENTS.md for the full editing contract.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
template_dir="${script_dir}/_template"

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: $0 <job-name-in-kebab-case> [target-parent-dir]" >&2
    exit 2
fi

job_kebab="$1"
parent_dir="${2:-$(dirname "$script_dir")}"   # default: sibling of scaffold dir

# Validate: lowercase, digits, single hyphens; must start with a letter.
if [[ ! "${job_kebab}" =~ ^[a-z][a-z0-9]*(-[a-z0-9]+)*$ ]]; then
    echo "ERROR: job name must be kebab-case ^[a-z][a-z0-9]*(-[a-z0-9]+)*$" >&2
    echo "       got: '${job_kebab}'" >&2
    exit 2
fi
# Keep generated identifiers under Postgres' 63-byte NAMEDATALEN. The longest
# generated identifier is IX_<Pascal>_BatchLog_WorkerId_BatchId (29 fixed chars).
if [[ "${#job_kebab}" -gt 30 ]]; then
    echo "ERROR: job name must be <= 30 chars (got ${#job_kebab})" >&2
    exit 2
fi

if [[ ! -d "${template_dir}" ]]; then
    echo "ERROR: template dir not found: ${template_dir}" >&2
    exit 1
fi

# Derive casings (bash 3.2 / macOS compatible -- no ${var^}).
job_lower="$(printf '%s' "${job_kebab}" | tr -d -- '-')"
job_pascal="$(printf '%s' "${job_kebab}" | awk -F'-' \
    '{ for (i=1;i<=NF;i++) printf "%s%s", toupper(substr($i,1,1)), substr($i,2) }')"

target_dir="${parent_dir%/}/${job_kebab}"
if [[ -e "${target_dir}" ]]; then
    echo "ERROR: target already exists: ${target_dir}" >&2
    echo "       delete it yourself if you really mean to regenerate." >&2
    exit 1
fi

mkdir -p "${target_dir}"

for src in "${template_dir}"/*; do
    base="$(basename "${src}")"
    dst="${target_dir}/${base}"
    sed -e "s/{{JOB_PASCAL}}/${job_pascal}/g" \
        -e "s/{{JOB_LOWER}}/${job_lower}/g" \
        -e "s/{{JOB_KEBAB}}/${job_kebab}/g" \
        "${src}" > "${dst}"
done

chmod +x "${target_dir}/repair.sh"

# Self-check: no token should survive substitution.
if grep -rl '{{' "${target_dir}" >/dev/null 2>&1; then
    echo "ERROR: unsubstituted tokens remain in ${target_dir}:" >&2
    grep -rn '{{' "${target_dir}" >&2
    exit 1
fi

echo "Created ${target_dir}"
echo "  PascalCase prefix : ${job_pascal}_*"
echo "  procedure         : maintenance.${job_lower}_run_batch"
echo "  application_name  : maint-${job_kebab}-<worker>"
echo
echo "Next: fill in the edit points, then confirm none remain:"
echo "  grep -rn 'TODO(job)' '${target_dir}'"
echo "  grep -rn 'TODO(job)\\|TODO_\\|{{' '${target_dir}'   # should be empty when done"
echo
echo "See ${script_dir}/AGENTS.md for the editing contract and ${script_dir}/README.md for the runbook."
