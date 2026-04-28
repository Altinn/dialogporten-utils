#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: summarize-explain-io.sh [--run-dir DIR] [--case CASE_ID ...] [--case-glob PATTERN ...]

Summarize top-level EXPLAIN JSON shared read blocks and shared I/O read time
by query variant.

Options:
  -r, --run-dir DIR       Run directory, or a directory containing raw/. Default: .
  -c, --case CASE_ID      Include one exact case id. Can be repeated.
  -g, --case-glob GLOB    Include case ids matching a shell glob. Can be repeated.
  -h, --help              Show this help.

Examples:
  ./summarize-explain-io.sh --run-dir runs/20260428T163154Z
  ./summarize-explain-io.sh --run-dir runs/20260428T163154Z --case case_0020_p_500_s_100
  ./summarize-explain-io.sh --run-dir runs/20260428T163154Z --case-glob 'case_*_p_500_*'
EOF
}

run_dir="."
case_patterns=()

while (($#)); do
  case "$1" in
    -r|--run-dir)
      [[ $# -ge 2 ]] || { echo "Missing value for $1" >&2; exit 2; }
      run_dir="$2"
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

xargs -0 jq -r '
  input_filename as $f |
  ($f | split("/")) as $parts |
  ($parts | index("raw")) as $raw_idx |
  [
    $parts[$raw_idx + 4],
    $parts[$raw_idx + 1],
    (.[0].Plan."Shared Read Blocks" // 0),
    (.[0].Plan."Shared I/O Read Time" // 0)
  ] | @tsv
' < "$tmp" |
  awk '
    {
      variant = $1
      blocks[variant] += $3
      read_ms[variant] += $4
      samples[variant] += 1
      if ($3 > 0) {
        read_samples[variant] += 1
      }
    }
    END {
      printf "%s\t%s\t%s\t%s\t%s\t%s\n", "variant", "samples", "read_samples", "blocks", "GiB", "read_time"
      for (variant in blocks) {
        gib = blocks[variant] * 8192 / 1024 / 1024 / 1024
        printf "%s\t%d\t%d\t%d\t%.2f\t%.2f ms / %.2f s\n",
          variant,
          samples[variant],
          read_samples[variant],
          blocks[variant],
          gib,
          read_ms[variant],
          read_ms[variant] / 1000
      }
    }
  '
