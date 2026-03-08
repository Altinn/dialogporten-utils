#!/usr/bin/env python3
"""Check functional equivalence of SQL variants using EXCEPT ALL / UNION ALL.

For each selected party and SQL comparison pair, this script runs:

WITH old_q AS (...), new_q AS (...), diff AS (
  (SELECT * FROM old_q EXCEPT ALL SELECT * FROM new_q)
  UNION ALL
  (SELECT * FROM new_q EXCEPT ALL SELECT * FROM old_q)
)
SELECT COUNT(*) FROM diff;

A diff count of 0 means equivalent result multisets for that party.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import itertools
import json
import os
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


PLACEHOLDER = "$party"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check SQL functional equivalence across sampled parties")
    parser.add_argument("--host", required=True, help="PostgreSQL host")
    parser.add_argument("--port", required=True, type=int, help="PostgreSQL port")
    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")

    parser.add_argument("--sql-dir", default="sql", help="Directory containing .sql files")
    parser.add_argument(
        "--mode",
        choices=["baseline", "all-pairs"],
        default="baseline",
        help="Compare all SQLs to a baseline, or compare every pair",
    )
    parser.add_argument(
        "--baseline",
        default="current",
        help="Baseline SQL stem (without .sql) when --mode baseline",
    )

    parser.add_argument("--parties-file", default="benchmark_parties.txt", help="Input parties file")
    parser.add_argument(
        "--num-parties",
        type=int,
        default=200,
        help="How many parties to sample for equivalence checks",
    )
    parser.add_argument("--seed", type=int, default=1337, help="Random seed")

    parser.add_argument(
        "--show-diff-rows",
        type=int,
        default=0,
        help="If >0, fetch up to N sample diff rows for mismatches",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=0,
        help="Optional PostgreSQL statement_timeout in milliseconds",
    )
    parser.add_argument(
        "--stop-on-first-mismatch",
        action="store_true",
        help="Stop immediately on first mismatch",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output root directory",
    )
    return parser.parse_args()


def utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(seconds), 60)
    return f"{minutes}m{sec:02d}s"


def strip_trailing_semicolons(sql: str) -> str:
    s = sql.strip()
    s = re.sub(r";+\s*$", "", s)
    return s


def load_parties(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Parties file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = [line.strip() for line in f if line.strip()]
    unique = list(dict.fromkeys(raw))
    if not unique:
        raise ValueError("No parties found in parties file")
    return unique


def load_sql_jobs(sql_dir: Path) -> list[tuple[str, Path, str]]:
    if not sql_dir.is_dir():
        raise FileNotFoundError(f"SQL directory not found: {sql_dir}")

    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        raise ValueError(f"No .sql files found in {sql_dir}")

    jobs: list[tuple[str, Path, str]] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        if PLACEHOLDER not in text:
            raise ValueError(f"Placeholder {PLACEHOLDER!r} not found in {path}")
        jobs.append((path.stem, path, strip_trailing_semicolons(text)))
    return jobs


def render_sql(sql_template: str, party: str) -> str:
    return sql_template.replace(PLACEHOLDER, party.replace("'", "''"))


def build_diff_query(old_sql: str, new_sql: str, sample_rows: int = 0) -> str:
    cte = (
        "WITH old_q AS (\n"
        f"{old_sql}\n"
        "), new_q AS (\n"
        f"{new_sql}\n"
        "), diff AS (\n"
        "  (SELECT * FROM old_q EXCEPT ALL SELECT * FROM new_q)\n"
        "  UNION ALL\n"
        "  (SELECT * FROM new_q EXCEPT ALL SELECT * FROM old_q)\n"
        ")\n"
    )
    if sample_rows > 0:
        return cte + f"SELECT * FROM diff LIMIT {sample_rows};"
    return cte + "SELECT COUNT(*)::bigint AS diff_count FROM diff;"


def run_psql(query: str, *, args: argparse.Namespace, csv_output: bool = False) -> subprocess.CompletedProcess[str]:
    cmd = [
        "psql",
        "-X",
        "-q",
        "-w",
        "-v",
        "ON_ERROR_STOP=1",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--dbname",
        args.dbname,
        "--username",
        args.user,
    ]

    if csv_output:
        cmd += ["--csv", "-t"]
    else:
        cmd += ["-t", "-A"]

    cmd += ["-c", query]

    env = dict(os.environ)
    if args.statement_timeout_ms > 0:
        env["PGOPTIONS"] = f"-c statement_timeout={args.statement_timeout_ms}"

    return subprocess.run(cmd, text=True, capture_output=True, env=env)


def parse_scalar_int(stdout: str) -> int:
    value = stdout.strip()
    if not value:
        raise ValueError("Empty scalar output from psql")
    return int(value)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.num_parties <= 0:
        print("--num-parties must be > 0", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)

    parties = load_parties(Path(args.parties_file))
    if args.num_parties > len(parties):
        print(
            f"--num-parties {args.num_parties} exceeds available distinct parties {len(parties)}",
            file=sys.stderr,
        )
        return 2

    sampled_parties = rng.sample(parties, args.num_parties)
    sql_jobs = load_sql_jobs(Path(args.sql_dir))
    by_id = {sql_id: (path, sql) for sql_id, path, sql in sql_jobs}

    if args.mode == "baseline":
        if args.baseline not in by_id:
            print(f"Baseline {args.baseline!r} not found in {args.sql_dir}", file=sys.stderr)
            return 2
        pairs: list[tuple[str, str]] = [
            (args.baseline, sql_id) for sql_id in sorted(by_id) if sql_id != args.baseline
        ]
    else:
        ids = sorted(by_id)
        pairs = list(itertools.combinations(ids, 2))

    if not pairs:
        print("No SQL comparison pairs to evaluate", file=sys.stderr)
        return 2

    run_root = Path(args.output_dir) / f"equivalence_{utc_timestamp()}"
    run_root.mkdir(parents=True, exist_ok=False)

    write_json(
        run_root / "manifest.json",
        {
            "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "host": args.host,
            "port": args.port,
            "dbname": args.dbname,
            "user": args.user,
            "sql_dir": args.sql_dir,
            "mode": args.mode,
            "baseline": args.baseline if args.mode == "baseline" else None,
            "pairs": pairs,
            "parties_file": args.parties_file,
            "num_parties": args.num_parties,
            "seed": args.seed,
            "statement_timeout_ms": args.statement_timeout_ms,
            "show_diff_rows": args.show_diff_rows,
            "stop_on_first_mismatch": args.stop_on_first_mismatch,
        },
    )

    total_checks = len(pairs) * len(sampled_parties)
    completed = 0
    mismatches = 0
    failures = 0

    started = time.perf_counter()

    result_rows: list[dict[str, Any]] = []
    mismatch_samples: list[dict[str, Any]] = []

    print(
        f"Starting equivalence check: pairs={len(pairs)} parties={len(sampled_parties)} total_checks={total_checks}"
    )

    for old_id, new_id in pairs:
        old_path, old_template = by_id[old_id]
        new_path, new_template = by_id[new_id]

        for party in sampled_parties:
            completed += 1

            old_sql = render_sql(old_template, party)
            new_sql = render_sql(new_template, party)
            query = build_diff_query(old_sql, new_sql)

            proc = run_psql(query, args=args, csv_output=False)
            row: dict[str, Any] = {
                "old_sql_id": old_id,
                "new_sql_id": new_id,
                "old_sql_file": str(old_path),
                "new_sql_file": str(new_path),
                "party": party,
                "success": False,
                "diff_count": None,
                "error": None,
            }

            if proc.returncode != 0:
                failures += 1
                row["error"] = proc.stderr.strip() or "psql_failed"
            else:
                try:
                    diff_count = parse_scalar_int(proc.stdout)
                    row["success"] = True
                    row["diff_count"] = diff_count
                    if diff_count != 0:
                        mismatches += 1
                        if args.show_diff_rows > 0:
                            sample_query = build_diff_query(old_sql, new_sql, sample_rows=args.show_diff_rows)
                            sample_proc = run_psql(sample_query, args=args, csv_output=True)
                            mismatch_samples.append(
                                {
                                    "old_sql_id": old_id,
                                    "new_sql_id": new_id,
                                    "party": party,
                                    "diff_count": diff_count,
                                    "sample_rows_csv": sample_proc.stdout if sample_proc.returncode == 0 else None,
                                    "sample_error": sample_proc.stderr.strip() if sample_proc.returncode != 0 else None,
                                }
                            )
                except Exception as exc:  # noqa: BLE001
                    failures += 1
                    row["error"] = f"parse_error: {exc}"

            result_rows.append(row)

            elapsed = time.perf_counter() - started
            remaining = total_checks - completed
            rate = completed / elapsed if elapsed > 0 else 0.0
            eta = remaining / rate if rate > 0 else 0.0
            print(
                f"[progress {completed}/{total_checks} {(completed/total_checks)*100:5.1f}%] "
                f"{old_id} vs {new_id} mismatch={mismatches} fail={failures} "
                f"elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
            )
            sys.stdout.flush()

            if args.stop_on_first_mismatch and mismatches > 0:
                break
        if args.stop_on_first_mismatch and mismatches > 0:
            break

    with (run_root / "results.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "old_sql_id",
                "new_sql_id",
                "old_sql_file",
                "new_sql_file",
                "party",
                "success",
                "diff_count",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(result_rows)

    if mismatch_samples:
        write_json(run_root / "mismatch_samples.json", mismatch_samples)

    pair_summary: dict[str, dict[str, int]] = {}
    for row in result_rows:
        key = f"{row['old_sql_id']}__vs__{row['new_sql_id']}"
        pair_summary.setdefault(key, {"checks": 0, "mismatches": 0, "failures": 0})
        pair_summary[key]["checks"] += 1
        if not row["success"]:
            pair_summary[key]["failures"] += 1
        elif int(row["diff_count"] or 0) != 0:
            pair_summary[key]["mismatches"] += 1

    summary = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_root": str(run_root),
        "total_checks": total_checks,
        "completed_checks": len(result_rows),
        "mismatches": mismatches,
        "failures": failures,
        "all_equivalent": mismatches == 0 and failures == 0,
        "pair_summary": pair_summary,
    }
    write_json(run_root / "summary.json", summary)

    print(f"Equivalence check complete. Output: {run_root}")
    print(
        f"Results: completed={len(result_rows)} mismatches={mismatches} failures={failures} "
        f"all_equivalent={summary['all_equivalent']}"
    )

    return 1 if mismatches > 0 or failures > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
