#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: summarize-explain-io.sh [--run-dir DIR] [--pass N ...] [--case CASE_ID ...] [--case-glob PATTERN ...]

Summarize top-level EXPLAIN JSON shared hit/read blocks and shared I/O read
time by query variant.

Options:
  -r, --run-dir DIR       Run directory, or a directory containing raw/. Default: .
  -p, --pass N            Include one pass number. Can be repeated.
  -c, --case CASE_ID      Include one exact case id. Can be repeated.
  -g, --case-glob GLOB    Include case ids matching a shell glob. Can be repeated.
  -h, --help              Show this help.

Examples:
  ./summarize-explain-io.sh --run-dir runs/20260428T163154Z
  ./summarize-explain-io.sh --run-dir runs/20260428T163154Z --pass 2
  ./summarize-explain-io.sh --run-dir runs/20260428T163154Z --case case_0020_p_500_s_100
  ./summarize-explain-io.sh --run-dir runs/20260428T163154Z --case-glob 'case_*_p_500_*'
EOF
}

run_dir="."
passes=()
case_patterns=()

while (($#)); do
  case "$1" in
    -r|--run-dir)
      [[ $# -ge 2 ]] || { echo "Missing value for $1" >&2; exit 2; }
      run_dir="$2"
      shift 2
      ;;
    -p|--pass)
      [[ $# -ge 2 ]] || { echo "Missing value for $1" >&2; exit 2; }
      [[ "$2" =~ ^[0-9]+$ ]] || { echo "Pass must be a positive integer: $2" >&2; exit 2; }
      passes+=("$((10#$2))")
      shift 2
      ;;
    -c|--case)
      [[ $# -ge 2 ]] || { echo "Missing value for $1" >&2; exit 2; }
      case_patterns+=("$2")
      shift 2
      ;;
    -g|--case-glob)
      [[ $# -ge 2 ]] || { echo "Missing value for $1" >&2; exit 2; }
      case_patterns+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -d "$run_dir/raw" ]]; then
  raw_dir="$run_dir/raw"
elif [[ "$(basename "$run_dir")" == "raw" && -d "$run_dir" ]]; then
  raw_dir="$run_dir"
else
  echo "Could not find raw/ under: $run_dir" >&2
  exit 1
fi

tmp="$(mktemp "${TMPDIR:-/tmp}/explain-io-files.XXXXXX")"
trap 'rm -f "$tmp"' EXIT

if ((${#case_patterns[@]} == 0)); then
  find "$raw_dir" -mindepth 5 -maxdepth 5 -name explain.json -type f -print0 > "$tmp"
else
  for pattern in "${case_patterns[@]}"; do
    find "$raw_dir" \
      -mindepth 5 \
      -maxdepth 5 \
      -path "$raw_dir/$pattern/pass_*/run_*/*/explain.json" \
      -type f \
      -print0 >> "$tmp"
  done
fi

if [[ ! -s "$tmp" ]]; then
  echo "No explain.json files matched." >&2
  exit 1
fi

pass_filter="$(printf '%s\n' "${passes[@]}")"
export PASS_FILTER="$pass_filter"

xargs -0 jq -r '
  env.PASS_FILTER as $pass_filter |
  input_filename as $f |
  ($f | split("/")) as $parts |
  ($parts | index("raw")) as $raw_idx |
  ($parts[$raw_idx + 2] | sub("^pass_0*"; "")) as $pass |
  select(($pass_filter == "") or (($pass_filter | split("\n")) | index($pass))) |
  (.[0].Plan."Shared Hit Blocks" // 0) as $hit_blocks |
  (.[0].Plan."Shared Read Blocks" // 0) as $read_blocks |
  [
    $parts[$raw_idx + 4],
    $parts[$raw_idx + 1],
    $pass,
    $hit_blocks,
    $read_blocks,
    ($hit_blocks + $read_blocks),
    (.[0].Plan."Shared I/O Read Time" // 0),
    (.[0]."Planning Time" // 0),
    (.[0]."Execution Time" // 0)
  ] | @tsv
' < "$tmp" |
  awk '
    {
      variant = $1
      hit_blocks[variant] += $4
      read_blocks[variant] += $5
      total_blocks[variant] += $6
      read_ms[variant] += $7
      planning_ms[variant] += $8
      execution_ms[variant] += $9
      samples[variant] += 1
      if ($5 > 0) {
        read_samples[variant] += 1
      }
    }
    END {
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", "variant", "samples", "read_samples", "hit_blocks", "read_blocks", "total_blocks", "total_GiB", "read_GiB", "read_time", "planning_time", "execution_time"
      for (variant in samples) {
        total_gib = total_blocks[variant] * 8192 / 1024 / 1024 / 1024
        read_gib = read_blocks[variant] * 8192 / 1024 / 1024 / 1024
        printf "%s\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f ms / %.2f s\t%.2f ms / %.2f s\t%.2f ms / %.2f s\n",
          variant,
          samples[variant],
          read_samples[variant],
          hit_blocks[variant],
          read_blocks[variant],
          total_blocks[variant],
          total_gib,
          read_gib,
          read_ms[variant],
          read_ms[variant] / 1000,
          planning_ms[variant],
          planning_ms[variant] / 1000,
          execution_ms[variant],
          execution_ms[variant] / 1000
      }
    }
  '
