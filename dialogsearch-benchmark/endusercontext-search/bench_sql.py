#!/usr/bin/env python3
"""Benchmark SQL files with randomized party values via psql EXPLAIN ANALYZE.

Behavior:
- Reads SQL files from a directory (default: sql/)
- Replaces "$party" placeholder with randomized values from parties.txt
- Executes each SQL N times with psql using:
    EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)
- Stores per-run output under output/<timestamp>/
- Computes avg, p50, p95, p99 execution times per SQL

Authentication:
- Password is never accepted as an argument.
- psql is run with -w, so credentials must come from .pgpass/libpq config.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
import re
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any


PLACEHOLDER = "$party"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark SQL files via psql EXPLAIN ANALYZE")
    parser.add_argument("--host", required=True, help="PostgreSQL host")
    parser.add_argument("--port", required=True, type=int, help="PostgreSQL port")
    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--runs", required=True, type=int, help="Runs per SQL file")
    parser.add_argument("--sql-dir", default="sql", help="Directory containing .sql files")
    parser.add_argument("--parties-file", default="parties.txt", help="Newline-separated party values")
    parser.add_argument("--output-dir", default="output", help="Output root directory")
    parser.add_argument("--seed", type=int, help="Optional random seed")
    return parser.parse_args()


def utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sanitize_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def percentile(values: list[float], p: float) -> float:
    if not values:
        raise ValueError("Cannot compute percentile of empty values")
    if p < 0 or p > 100:
        raise ValueError("Percentile must be in [0, 100]")

    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return sorted_vals[0]

    idx = (p / 100.0) * (len(sorted_vals) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(sorted_vals) - 1)
    weight = idx - lower
    return sorted_vals[lower] * (1.0 - weight) + sorted_vals[upper] * weight


def extract_execution_time_ms(explain_json: Any) -> float:
    payload = explain_json
    if isinstance(payload, list):
        if not payload:
            raise ValueError("EXPLAIN JSON list was empty")
        payload = payload[0]

    if not isinstance(payload, dict):
        raise ValueError("Unexpected EXPLAIN JSON structure")

    value = payload.get("Execution Time")
    if value is None:
        raise ValueError("Missing 'Execution Time' in EXPLAIN JSON")

    return float(value)


def load_parties(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Parties file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = [line.strip() for line in f if line.strip()]

    # Deduplicate values so the no-consecutive rule is guaranteed by value.
    unique = list(dict.fromkeys(raw))
    if len(unique) < 2:
        raise ValueError("parties.txt must contain at least 2 distinct non-empty values")

    return unique


def load_sql_jobs(sql_dir: Path) -> list[tuple[str, Path, str]]:
    if not sql_dir.is_dir():
        raise FileNotFoundError(f"SQL directory not found: {sql_dir}")

    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        raise ValueError(f"No .sql files found in {sql_dir}")

    jobs: list[tuple[str, Path, str]] = []
    for sql_file in files:
        text = sql_file.read_text(encoding="utf-8")
        if PLACEHOLDER not in text:
            raise ValueError(f"Placeholder {PLACEHOLDER!r} not found in {sql_file}")

        sql_id = sanitize_name(sql_file.stem)
        jobs.append((sql_id, sql_file, text))

    return jobs


def choose_party(rng: random.Random, parties: list[str], previous: str | None) -> str:
    if previous is None:
        return rng.choice(parties)

    # Rejection sample until we get a different value.
    party = rng.choice(parties)
    while party == previous:
        party = rng.choice(parties)
    return party


def build_query(sql_text: str, party: str) -> str:
    escaped_party = party.replace("'", "''")
    rendered = sql_text.replace(PLACEHOLDER, escaped_party)
    return f"EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)\n{rendered}"


def run_psql(
    query: str,
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        "psql",
        "-X",
        "-q",
        "-t",
        "-A",
        "-w",
        "-v",
        "ON_ERROR_STOP=1",
        "--host",
        host,
        "--port",
        str(port),
        "--dbname",
        dbname,
        "--username",
        user,
        "-c",
        query,
    ]

    return subprocess.run(cmd, text=True, capture_output=True)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def round2(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def fmt_number(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def build_markdown_table(rows: list[dict[str, Any]]) -> str:
    header = (
        "| sql_id | count_success | count_failures | avg_ms | p50_ms | p95_ms | p99_ms | min_ms | max_ms |\n"
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
    )

    body_lines: list[str] = []
    for row in rows:
        body_lines.append(
            "| "
            + " | ".join(
                [
                    str(row["sql_id"]),
                    str(row["count_success"]),
                    str(row["count_failures"]),
                    fmt_number(row["avg_ms"]),
                    fmt_number(row["p50_ms"]),
                    fmt_number(row["p95_ms"]),
                    fmt_number(row["p99_ms"]),
                    fmt_number(row["min_ms"]),
                    fmt_number(row["max_ms"]),
                ]
            )
            + " |"
        )

    return header + "\n".join(body_lines) + "\n"


def build_ascii_table(rows: list[dict[str, Any]]) -> str:
    columns = [
        "sql_id",
        "count_success",
        "count_failures",
        "avg_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "min_ms",
        "max_ms",
    ]

    table_rows: list[list[str]] = []
    for row in rows:
        table_rows.append(
            [
                str(row["sql_id"]),
                str(row["count_success"]),
                str(row["count_failures"]),
                fmt_number(row["avg_ms"]),
                fmt_number(row["p50_ms"]),
                fmt_number(row["p95_ms"]),
                fmt_number(row["p99_ms"]),
                fmt_number(row["min_ms"]),
                fmt_number(row["max_ms"]),
            ]
        )

    widths = [len(col) for col in columns]
    for values in table_rows:
        for idx, value in enumerate(values):
            widths[idx] = max(widths[idx], len(value))

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "| " + " | ".join(col.ljust(widths[idx]) for idx, col in enumerate(columns)) + " |"
    lines = [sep, header, sep]
    for values in table_rows:
        lines.append("| " + " | ".join(values[idx].ljust(widths[idx]) for idx in range(len(columns))) + " |")
    lines.append(sep)
    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    if args.runs <= 0:
        print("--runs must be > 0", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)

    parties = load_parties(Path(args.parties_file))
    sql_jobs = load_sql_jobs(Path(args.sql_dir))

    run_root = Path(args.output_dir) / utc_timestamp()
    run_root.mkdir(parents=True, exist_ok=False)

    write_json(
        run_root / "manifest.json",
        {
            "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "host": args.host,
            "port": args.port,
            "dbname": args.dbname,
            "user": args.user,
            "runs_per_sql": args.runs,
            "sql_dir": str(Path(args.sql_dir)),
            "parties_file": str(Path(args.parties_file)),
            "seed": args.seed,
            "sql_files": [str(job[1]) for job in sql_jobs],
            "party_count_distinct": len(parties),
        },
    )

    for sql_id, _, _ in sql_jobs:
        (run_root / "runs" / sql_id).mkdir(parents=True, exist_ok=True)

    rows_path = run_root / "runs.jsonl"

    per_sql_times: dict[str, list[float]] = {sql_id: [] for sql_id, _, _ in sql_jobs}
    per_sql_failures: dict[str, int] = {sql_id: 0 for sql_id, _, _ in sql_jobs}

    global_run_index = 0
    previous_party: str | None = None

    with rows_path.open("w", encoding="utf-8") as rows_file:
        for iteration in range(1, args.runs + 1):
            jobs_this_round = list(sql_jobs)
            rng.shuffle(jobs_this_round)

            for sql_id, sql_file, sql_text in jobs_this_round:
                global_run_index += 1
                party = choose_party(rng, parties, previous_party)
                previous_party = party

                query = build_query(sql_text, party)
                completed = run_psql(
                    query,
                    host=args.host,
                    port=args.port,
                    dbname=args.dbname,
                    user=args.user,
                )

                sql_run_dir = run_root / "runs" / sql_id
                run_tag = f"run_{iteration:04d}"

                (sql_run_dir / f"{run_tag}.stdout.txt").write_text(completed.stdout, encoding="utf-8")
                (sql_run_dir / f"{run_tag}.stderr.txt").write_text(completed.stderr, encoding="utf-8")

                record: dict[str, Any] = {
                    "global_run_index": global_run_index,
                    "iteration": iteration,
                    "sql_id": sql_id,
                    "sql_file": str(sql_file),
                    "party": party,
                    "success": False,
                    "returncode": completed.returncode,
                    "execution_time_ms": None,
                    "error": None,
                }

                if completed.returncode != 0:
                    per_sql_failures[sql_id] += 1
                    record["error"] = "psql_non_zero_exit"
                else:
                    try:
                        explain_json = json.loads(completed.stdout.strip())
                        execution_ms = extract_execution_time_ms(explain_json)
                        record["success"] = True
                        record["execution_time_ms"] = execution_ms
                        per_sql_times[sql_id].append(execution_ms)
                        write_json(sql_run_dir / f"{run_tag}.explain.json", explain_json)
                    except Exception as exc:  # noqa: BLE001
                        per_sql_failures[sql_id] += 1
                        record["error"] = f"parse_error: {exc}"

                rows_file.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary: dict[str, Any] = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_root": str(run_root),
        "runs_per_sql": args.runs,
        "sql": {},
        "total_runs": args.runs * len(sql_jobs),
    }

    csv_rows: list[dict[str, Any]] = []
    any_failures = False

    for sql_id, _, _ in sql_jobs:
        values = per_sql_times[sql_id]
        failures = per_sql_failures[sql_id]
        if failures > 0:
            any_failures = True

        metrics: dict[str, Any] = {
            "count_success": len(values),
            "count_failures": failures,
            "avg_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "min_ms": None,
            "max_ms": None,
        }

        if values:
            metrics.update(
                {
                    "avg_ms": round2(statistics.fmean(values)),
                    "p50_ms": round2(percentile(values, 50)),
                    "p95_ms": round2(percentile(values, 95)),
                    "p99_ms": round2(percentile(values, 99)),
                    "min_ms": round2(min(values)),
                    "max_ms": round2(max(values)),
                }
            )
        else:
            any_failures = True

        summary["sql"][sql_id] = metrics
        csv_rows.append({"sql_id": sql_id, **metrics})

    write_json(run_root / "summary.json", summary)

    with (run_root / "summary.csv").open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "sql_id",
                "count_success",
                "count_failures",
                "avg_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "min_ms",
                "max_ms",
            ],
        )
        writer.writeheader()
        writer.writerows(csv_rows)

    markdown_path = run_root / "summary.md"
    markdown_path.write_text(build_markdown_table(csv_rows), encoding="utf-8")

    print(f"Benchmark complete. Output: {run_root}")
    print(f"Markdown summary filename: {markdown_path.name}")
    print(f"Markdown summary path: {markdown_path}")
    print(build_ascii_table(csv_rows))

    return 1 if any_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
