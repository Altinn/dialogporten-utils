#!/usr/bin/env python3
"""Generate a benchmark party set with stratified sampling by dialog count.

Default counting mode is select-only friendly:
- Uses batched SELECT statements and requires only SELECT privilege on "Dialog".
- Avoids temp tables and COPY.

Optional temp-table mode is kept for speed when privileges and impact budget allow.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build benchmark party set from parties.txt")
    parser.add_argument("--host", required=True, help="PostgreSQL host")
    parser.add_argument("--port", required=True, type=int, help="PostgreSQL port")
    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--parties-file", default="parties.txt", help="Input party pool (one per line)")
    parser.add_argument("--sample-size", type=int, default=2000, help="How many parties to sample")
    parser.add_argument(
        "--strategy",
        choices=["stratified", "uniform"],
        default="stratified",
        help="Sampling strategy",
    )
    parser.add_argument(
        "--allocation",
        choices=["balanced", "proportional"],
        default="balanced",
        help="Allocation mode for stratified strategy",
    )
    parser.add_argument("--seed", type=int, default=1337, help="Random seed")
    parser.add_argument(
        "--count-mode",
        choices=["batched-selects", "temp-table"],
        default="batched-selects",
        help="How to compute dialog counts (batched-selects works with SELECT-only users)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Parties per batched SELECT when using --count-mode batched-selects",
    )
    parser.add_argument(
        "--pause-ms",
        type=int,
        default=0,
        help="Sleep between count batches to reduce DB pressure",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=0,
        help="Optional PostgreSQL statement_timeout in milliseconds (0 means no override)",
    )
    parser.add_argument(
        "--output-parties-file",
        default="benchmark_parties.txt",
        help="Output benchmark party file (used by bench_sql.py by default)",
    )
    parser.add_argument(
        "--output-prefix",
        default="benchmark_parties",
        help="Prefix for sidecar outputs (.csv/.json)",
    )
    return parser.parse_args()


def load_party_pool(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Parties file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = [line.strip() for line in f if line.strip()]

    unique = list(dict.fromkeys(raw))
    if not unique:
        raise ValueError("Input party file contains no non-empty values")

    return unique


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(seconds), 60)
    return f"{minutes}m{sec:02d}s"


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_psql_select(
    query: str,
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    statement_timeout_ms: int,
) -> str:
    cmd = [
        "psql",
        "-X",
        "-q",
        "-w",
        "--csv",
        "-t",
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

    env = None
    if statement_timeout_ms > 0:
        env = dict(**dict(**__import__("os").environ), PGOPTIONS=f"-c statement_timeout={statement_timeout_ms}")

    completed = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"psql failed ({completed.returncode}):\n{completed.stderr}")

    return completed.stdout


def parse_csv_pairs(csv_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    reader = csv.reader(csv_text.splitlines())
    for rec in reader:
        if len(rec) < 2:
            continue
        rows.append({"party": rec[0], "dialog_count": int(rec[1])})
    return rows


def run_count_query_batched(
    parties: list[str],
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    batch_size: int,
    pause_ms: int,
    statement_timeout_ms: int,
) -> list[dict[str, Any]]:
    if batch_size <= 0:
        raise ValueError("--batch-size must be > 0")

    batches = chunked(parties, batch_size)
    total_batches = len(batches)
    out: list[dict[str, Any]] = []
    started_at = time.perf_counter()

    for idx, batch in enumerate(batches, start=1):
        values_sql = ", ".join(f"({sql_quote(p)})" for p in batch)
        query = (
            "SELECT v.party, "
            "       COALESCE((SELECT COUNT(*)::bigint FROM \"Dialog\" d WHERE d.\"Party\" = v.party), 0) AS dialog_count "
            "FROM (VALUES "
            f"{values_sql}"
            ") AS v(party) "
            "ORDER BY v.party"
        )

        csv_text = run_psql_select(
            query,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            statement_timeout_ms=statement_timeout_ms,
        )

        parsed = parse_csv_pairs(csv_text)
        out.extend(parsed)

        elapsed = time.perf_counter() - started_at
        done = idx
        remaining = total_batches - done
        rate = done / elapsed if elapsed > 0 else 0.0
        eta = remaining / rate if rate > 0 else 0.0
        print(
            f"[count {done}/{total_batches} {(done/total_batches)*100:5.1f}%] "
            f"batch_size={len(batch)} rows={len(parsed)} elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
        )
        sys.stdout.flush()

        if pause_ms > 0 and idx < total_batches:
            time.sleep(pause_ms / 1000.0)

    return out


def run_count_query_temp_table(
    parties: list[str],
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    statement_timeout_ms: int,
) -> str:
    psql_script = """
\\set ON_ERROR_STOP on
CREATE TEMP TABLE benchmark_pool (
    party text PRIMARY KEY
);
COPY benchmark_pool(party) FROM STDIN;
"""
    psql_script += "\n".join(parties)
    psql_script += "\n\\.\n"
    psql_script += """
\\copy (
    SELECT p.party,
           COALESCE(d.dialog_count, 0) AS dialog_count
    FROM benchmark_pool p
    LEFT JOIN (
        SELECT d."Party" AS party,
               COUNT(*)::bigint AS dialog_count
        FROM "Dialog" d
        INNER JOIN benchmark_pool bp ON bp.party = d."Party"
        GROUP BY d."Party"
    ) d ON d.party = p.party
    ORDER BY p.party
) TO STDOUT WITH CSV HEADER
"""

    cmd = [
        "psql",
        "-X",
        "-q",
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
    ]

    env = None
    if statement_timeout_ms > 0:
        env = dict(**dict(**__import__("os").environ), PGOPTIONS=f"-c statement_timeout={statement_timeout_ms}")

    completed = subprocess.run(cmd, input=psql_script, text=True, capture_output=True, env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"psql failed ({completed.returncode}):\n{completed.stderr}")

    return completed.stdout


def parse_counts_csv(csv_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(csv_text.splitlines())
    for row in reader:
        party = row.get("party")
        cnt_raw = row.get("dialog_count")
        if party is None or cnt_raw is None:
            continue
        rows.append({"party": party, "dialog_count": int(cnt_raw)})

    if not rows:
        raise ValueError("No rows returned from count query")
    return rows


def stratum_label(count: int) -> str:
    if count == 0:
        return "0"
    if count <= 10:
        return "1-10"
    if count <= 100:
        return "11-100"
    if count <= 1000:
        return "101-1000"
    if count <= 10000:
        return "1001-10000"
    return "10001+"


def allocate_targets(
    capacities: dict[str, int],
    sample_size: int,
    mode: str,
) -> dict[str, int]:
    labels = [label for label, cap in capacities.items() if cap > 0]
    if not labels:
        raise ValueError("No non-empty strata available")

    targets = {label: 0 for label in capacities}

    if mode == "balanced":
        base = sample_size // len(labels)
        rem = sample_size % len(labels)

        for idx, label in enumerate(labels):
            want = base + (1 if idx < rem else 0)
            targets[label] = min(want, capacities[label])

        leftover = sample_size - sum(targets.values())
        while leftover > 0:
            made_progress = False
            for label in labels:
                if leftover == 0:
                    break
                if targets[label] < capacities[label]:
                    targets[label] += 1
                    leftover -= 1
                    made_progress = True
            if not made_progress:
                break

        return targets

    # proportional
    total_cap = sum(capacities[label] for label in labels)
    remainders: list[tuple[float, str]] = []

    for label in labels:
        ideal = sample_size * capacities[label] / total_cap
        take = min(int(ideal), capacities[label])
        targets[label] = take
        remainders.append((ideal - int(ideal), label))

    leftover = sample_size - sum(targets.values())
    remainders.sort(reverse=True)

    while leftover > 0:
        made_progress = False
        for _, label in remainders:
            if leftover == 0:
                break
            if targets[label] < capacities[label]:
                targets[label] += 1
                leftover -= 1
                made_progress = True
        if not made_progress:
            break

    return targets


def sample_uniform(rows: list[dict[str, Any]], sample_size: int, rng: random.Random) -> list[dict[str, Any]]:
    if sample_size > len(rows):
        raise ValueError(f"sample_size {sample_size} exceeds available parties {len(rows)}")
    return rng.sample(rows, sample_size)


def sample_stratified(
    rows: list[dict[str, Any]],
    sample_size: int,
    allocation: str,
    rng: random.Random,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int]]:
    by_stratum: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_stratum[stratum_label(row["dialog_count"])].append(row)

    capacities = {label: len(items) for label, items in by_stratum.items()}
    targets = allocate_targets(capacities, sample_size, allocation)

    sampled: list[dict[str, Any]] = []
    for label, items in by_stratum.items():
        k = targets.get(label, 0)
        if k <= 0:
            continue
        sampled.extend(rng.sample(items, k))

    if len(sampled) != sample_size:
        raise RuntimeError(
            f"Sampling produced {len(sampled)} rows, expected {sample_size}. "
            "Check strata capacities and allocation logic."
        )

    return sampled, capacities, targets


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_ascii_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    matrix = [[str(row.get(col, "")) for col in columns] for row in rows]
    widths = [len(col) for col in columns]
    for values in matrix:
        for i, value in enumerate(values):
            widths[i] = max(widths[i], len(value))

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "| " + " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns)) + " |"
    out = [sep, header, sep]
    for values in matrix:
        out.append("| " + " | ".join(values[i].ljust(widths[i]) for i in range(len(columns))) + " |")
    out.append(sep)
    return "\n".join(out)


def main() -> int:
    args = parse_args()

    if args.sample_size <= 0:
        print("--sample-size must be > 0", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)

    print("[1/5] Loading input party pool...")
    pool = load_party_pool(Path(args.parties_file))
    print(f"Loaded {len(pool)} distinct parties from {args.parties_file}")

    if args.sample_size > len(pool):
        print(
            f"sample_size {args.sample_size} exceeds distinct pool size {len(pool)}",
            file=sys.stderr,
        )
        return 2

    print(f"[2/5] Querying dialog counts with psql (mode={args.count_mode})...")
    if args.count_mode == "batched-selects":
        rows = run_count_query_batched(
            pool,
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            batch_size=args.batch_size,
            pause_ms=args.pause_ms,
            statement_timeout_ms=args.statement_timeout_ms,
        )
    else:
        csv_text = run_count_query_temp_table(
            pool,
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            statement_timeout_ms=args.statement_timeout_ms,
        )
        rows = parse_counts_csv(csv_text)

    print("[3/5] Parsing counts and sampling parties...")
    if len(rows) != len(pool):
        print(
            f"Warning: count rows ({len(rows)}) differ from pool size ({len(pool)}). Proceeding with returned rows.",
            file=sys.stderr,
        )

    # Deduplicate by party just in case.
    by_party: dict[str, dict[str, Any]] = {}
    for row in rows:
        by_party[row["party"]] = row
    rows = list(by_party.values())

    for row in rows:
        row["stratum"] = stratum_label(row["dialog_count"])

    capacities: dict[str, int] = {}
    targets: dict[str, int] = {}

    if args.strategy == "uniform":
        sampled = sample_uniform(rows, args.sample_size, rng)
    else:
        sampled, capacities, targets = sample_stratified(rows, args.sample_size, args.allocation, rng)

    sampled.sort(key=lambda r: r["party"])

    print("[4/5] Writing output files...")
    out_parties = Path(args.output_parties_file)
    out_prefix = Path(args.output_prefix)

    out_parties.write_text("\n".join(row["party"] for row in sampled) + "\n", encoding="utf-8")

    write_csv(
        out_prefix.with_suffix(".csv"),
        ["party", "dialog_count", "stratum"],
        sampled,
    )

    write_csv(
        out_prefix.with_name(out_prefix.name + ".party_counts.csv"),
        ["party", "dialog_count", "stratum"],
        sorted(rows, key=lambda r: r["party"]),
    )

    strata_rows: list[dict[str, Any]] = []
    if args.strategy == "stratified":
        labels = sorted(set(capacities) | set(targets))
        for label in labels:
            strata_rows.append(
                {
                    "stratum": label,
                    "available": capacities.get(label, 0),
                    "selected": targets.get(label, 0),
                }
            )
    else:
        counts_by_stratum: dict[str, int] = defaultdict(int)
        available_by_stratum: dict[str, int] = defaultdict(int)
        for row in rows:
            available_by_stratum[row["stratum"]] += 1
        for row in sampled:
            counts_by_stratum[row["stratum"]] += 1
        for label in sorted(available_by_stratum):
            strata_rows.append(
                {
                    "stratum": label,
                    "available": available_by_stratum[label],
                    "selected": counts_by_stratum.get(label, 0),
                }
            )

    write_csv(
        out_prefix.with_name(out_prefix.name + ".strata_summary.csv"),
        ["stratum", "available", "selected"],
        strata_rows,
    )

    metadata = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "parties_file": args.parties_file,
        "pool_size": len(pool),
        "counted_rows": len(rows),
        "sample_size": args.sample_size,
        "strategy": args.strategy,
        "allocation": args.allocation,
        "seed": args.seed,
        "count_mode": args.count_mode,
        "batch_size": args.batch_size,
        "pause_ms": args.pause_ms,
        "statement_timeout_ms": args.statement_timeout_ms,
        "output_parties_file": str(out_parties),
        "output_prefix": str(out_prefix),
    }
    out_prefix.with_suffix(".metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print("[5/5] Done.")
    print(f"Benchmark parties file: {out_parties}")
    print(f"Sample rows csv: {out_prefix.with_suffix('.csv')}")
    print(f"Full counts csv: {out_prefix.with_name(out_prefix.name + '.party_counts.csv')}")
    print(f"Strata summary csv: {out_prefix.with_name(out_prefix.name + '.strata_summary.csv')}")
    print(build_ascii_table(strata_rows, ["stratum", "available", "selected"]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
