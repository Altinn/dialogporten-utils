#!/usr/bin/env python3
"""Generate a benchmark party set with stratified sampling by dialog count.

Default counting mode is select-only friendly:
- Uses batched SELECT statements and requires only SELECT privilege on "Dialog".
- Uses capped counts by default so heavy parties do not dominate runtime.
- Supports adaptive split on timeout and checkpoint/resume.

Optional temp-table mode is kept for speed when privileges and impact budget allow.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import random
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any


class PsqlQueryError(RuntimeError):
    def __init__(self, message: str, *, is_timeout: bool = False):
        super().__init__(message)
        self.is_timeout = is_timeout


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
        "--count-cap",
        type=int,
        default=10001,
        help="Cap per-party count work by counting up to this many rows (default aligns with 10001+ stratum)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Parties per batched SELECT when using --count-mode batched-selects",
    )
    parser.add_argument(
        "--min-batch-size",
        type=int,
        default=1,
        help="Smallest batch size when splitting timeouts",
    )
    parser.add_argument(
        "--pause-ms",
        type=int,
        default=100,
        help="Sleep between top-level count batches to reduce DB pressure",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=120000,
        help="PostgreSQL statement_timeout in milliseconds (0 means no override)",
    )
    parser.add_argument(
        "--retry-on-timeout",
        action="store_true",
        default=True,
        help="Retry timed-out batches by splitting into smaller batches (default: on)",
    )
    parser.add_argument(
        "--no-retry-on-timeout",
        action="store_false",
        dest="retry_on_timeout",
        help="Disable adaptive split retry on timeout",
    )
    parser.add_argument(
        "--timeout-single-party-as-top-stratum",
        action="store_true",
        default=True,
        help="If a single-party count still times out, assign count_cap (default: on)",
    )
    parser.add_argument(
        "--fail-on-timeout-single-party",
        action="store_false",
        dest="timeout_single_party_as_top_stratum",
        help="Fail instead of assigning count_cap when single-party timeout occurs",
    )
    parser.add_argument(
        "--max-parties-to-count",
        type=int,
        help="Optional deterministic pre-sample of parties to count before final sampling",
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume counting from state file if present",
    )
    parser.add_argument(
        "--state-file",
        default="benchmark_parties.state.json",
        help="Checkpoint state file for batched-selects mode",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=500,
        help="Persist state every N newly counted parties",
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


def is_timeout_stderr(stderr: str) -> bool:
    s = stderr.lower()
    return "statement timeout" in s or "canceling statement due to statement timeout" in s


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

    env = dict(os.environ)
    if statement_timeout_ms > 0:
        env["PGOPTIONS"] = f"-c statement_timeout={statement_timeout_ms}"

    completed = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if completed.returncode != 0:
        raise PsqlQueryError(
            f"psql failed ({completed.returncode}):\n{completed.stderr}",
            is_timeout=is_timeout_stderr(completed.stderr),
        )

    return completed.stdout


def parse_csv_pairs(csv_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    reader = csv.reader(csv_text.splitlines())
    for rec in reader:
        if len(rec) < 2:
            continue
        rows.append({"party": rec[0], "dialog_count": int(rec[1])})
    return rows


def build_batched_count_query(parties: list[str], count_cap: int) -> str:
    values_sql = ", ".join(f"({sql_quote(p)})" for p in parties)
    return (
        "SELECT v.party, "
        "       COALESCE(("
        "         SELECT COUNT(*)::bigint "
        "         FROM ("
        "           SELECT 1 FROM \"Dialog\" d "
        "           WHERE d.\"Party\" = v.party "
        f"          LIMIT {count_cap}"
        "         ) capped"
        "       ), 0) AS dialog_count "
        "FROM (VALUES "
        f"{values_sql}"
        ") AS v(party) "
        "ORDER BY v.party"
    )


def hash_candidates(candidates: list[str]) -> str:
    h = hashlib.sha256()
    for party in candidates:
        h.update(party.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def load_state(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def select_candidates(pool: list[str], max_parties_to_count: int | None, seed: int) -> list[str]:
    if max_parties_to_count is None or max_parties_to_count >= len(pool):
        return list(pool)
    if max_parties_to_count <= 0:
        raise ValueError("--max-parties-to-count must be > 0")

    rng = random.Random(seed + 7919)
    return rng.sample(pool, max_parties_to_count)


def count_parties_adaptive(
    parties: list[str],
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    statement_timeout_ms: int,
    count_cap: int,
    retry_on_timeout: bool,
    min_batch_size: int,
    timeout_single_party_as_top_stratum: bool,
    counts_by_party: dict[str, int],
    stats: dict[str, int],
) -> None:
    query = build_batched_count_query(parties, count_cap)

    try:
        stats["queries_attempted"] += 1
        csv_text = run_psql_select(
            query,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            statement_timeout_ms=statement_timeout_ms,
        )
        parsed = parse_csv_pairs(csv_text)
        parsed_map = {r["party"]: int(r["dialog_count"]) for r in parsed}
        if len(parsed_map) != len(parties):
            raise RuntimeError(
                f"Batch parse mismatch: expected {len(parties)} unique parties, got {len(parsed_map)}"
            )
        for party in parties:
            if party not in parsed_map:
                raise RuntimeError(f"Missing party in batch output: {party}")
            counts_by_party[party] = parsed_map[party]
    except PsqlQueryError as exc:
        if not exc.is_timeout:
            raise

        stats["timeouts"] += 1

        can_split = retry_on_timeout and len(parties) > max(1, min_batch_size)
        if can_split:
            stats["timeout_splits"] += 1
            mid = len(parties) // 2
            if mid <= 0:
                mid = 1
            left = parties[:mid]
            right = parties[mid:]
            print(
                f"[timeout] split batch size {len(parties)} -> {len(left)} + {len(right)}",
                file=sys.stderr,
            )
            count_parties_adaptive(
                left,
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                statement_timeout_ms=statement_timeout_ms,
                count_cap=count_cap,
                retry_on_timeout=retry_on_timeout,
                min_batch_size=min_batch_size,
                timeout_single_party_as_top_stratum=timeout_single_party_as_top_stratum,
                counts_by_party=counts_by_party,
                stats=stats,
            )
            if right:
                count_parties_adaptive(
                    right,
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    statement_timeout_ms=statement_timeout_ms,
                    count_cap=count_cap,
                    retry_on_timeout=retry_on_timeout,
                    min_batch_size=min_batch_size,
                    timeout_single_party_as_top_stratum=timeout_single_party_as_top_stratum,
                    counts_by_party=counts_by_party,
                    stats=stats,
                )
            return

        if len(parties) == 1 and timeout_single_party_as_top_stratum:
            party = parties[0]
            counts_by_party[party] = count_cap
            stats["single_party_timeout_fallbacks"] += 1
            print(
                f"[timeout] single party fallback to count_cap for party={party}",
                file=sys.stderr,
            )
            return

        raise RuntimeError(
            f"Timeout counting batch size {len(parties)} and unable to recover.\n{exc}"
        ) from exc


def run_count_query_batched(
    candidates: list[str],
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    batch_size: int,
    min_batch_size: int,
    pause_ms: int,
    statement_timeout_ms: int,
    count_cap: int,
    retry_on_timeout: bool,
    timeout_single_party_as_top_stratum: bool,
    state_file: Path,
    resume: bool,
    checkpoint_interval: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if batch_size <= 0:
        raise ValueError("--batch-size must be > 0")
    if min_batch_size <= 0:
        raise ValueError("--min-batch-size must be > 0")
    if checkpoint_interval <= 0:
        raise ValueError("--checkpoint-interval must be > 0")

    candidate_hash = hash_candidates(candidates)

    counts_by_party: dict[str, int] = {}
    stats: dict[str, int] = {
        "queries_attempted": 0,
        "timeouts": 0,
        "timeout_splits": 0,
        "single_party_timeout_fallbacks": 0,
    }

    if resume:
        state = load_state(state_file)
        if state is None:
            print(f"[resume] no state file found at {state_file}, starting fresh")
        else:
            if state.get("candidate_hash") != candidate_hash:
                raise RuntimeError("State candidate hash does not match current candidate set")
            if int(state.get("count_cap", -1)) != count_cap:
                raise RuntimeError("State count_cap does not match current --count-cap")
            counts_by_party = {k: int(v) for k, v in state.get("counts_by_party", {}).items()}
            for key in stats:
                if key in state.get("stats", {}):
                    stats[key] = int(state["stats"][key])
            print(f"[resume] loaded {len(counts_by_party)} already-counted parties from {state_file}")

    remaining = [p for p in candidates if p not in counts_by_party]
    total = len(candidates)

    if not remaining:
        rows = [{"party": p, "dialog_count": counts_by_party[p]} for p in candidates]
        return rows, stats

    top_batches = chunked(remaining, batch_size)
    top_total = len(top_batches)
    started_at = time.perf_counter()
    last_checkpoint_count = len(counts_by_party)

    def maybe_checkpoint(force: bool = False) -> None:
        nonlocal last_checkpoint_count
        done = len(counts_by_party)
        if not force and (done - last_checkpoint_count) < checkpoint_interval:
            return

        payload = {
            "version": 1,
            "saved_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "candidate_hash": candidate_hash,
            "count_cap": count_cap,
            "counts_by_party": counts_by_party,
            "stats": stats,
        }
        save_state(state_file, payload)
        last_checkpoint_count = done

    for idx, batch in enumerate(top_batches, start=1):
        before = len(counts_by_party)
        count_parties_adaptive(
            batch,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            statement_timeout_ms=statement_timeout_ms,
            count_cap=count_cap,
            retry_on_timeout=retry_on_timeout,
            min_batch_size=min_batch_size,
            timeout_single_party_as_top_stratum=timeout_single_party_as_top_stratum,
            counts_by_party=counts_by_party,
            stats=stats,
        )
        after = len(counts_by_party)

        elapsed = time.perf_counter() - started_at
        done = len(counts_by_party)
        rem = total - done
        rate = done / elapsed if elapsed > 0 else 0.0
        eta = rem / rate if rate > 0 else 0.0
        print(
            f"[count batch {idx}/{top_total} {(idx/top_total)*100:5.1f}%] "
            f"added={after-before} total_counted={done}/{total} elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
        )
        sys.stdout.flush()

        maybe_checkpoint(force=False)

        if pause_ms > 0 and idx < top_total:
            time.sleep(pause_ms / 1000.0)

    maybe_checkpoint(force=True)

    rows = [{"party": p, "dialog_count": counts_by_party[p]} for p in candidates if p in counts_by_party]
    if len(rows) != len(candidates):
        missing = len(candidates) - len(rows)
        raise RuntimeError(f"Missing counts for {missing} parties after counting")

    return rows, stats


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

    env = dict(os.environ)
    if statement_timeout_ms > 0:
        env["PGOPTIONS"] = f"-c statement_timeout={statement_timeout_ms}"

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
    if args.count_cap <= 0:
        print("--count-cap must be > 0", file=sys.stderr)
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

    candidates = select_candidates(pool, args.max_parties_to_count, args.seed)
    if args.sample_size > len(candidates):
        print(
            f"sample_size {args.sample_size} exceeds counted candidate pool size {len(candidates)}",
            file=sys.stderr,
        )
        return 2

    print(
        f"Candidate parties to count: {len(candidates)} "
        f"(from total {len(pool)})"
    )

    print(f"[2/5] Querying dialog counts with psql (mode={args.count_mode})...")
    count_stats: dict[str, int] = {}
    if args.count_mode == "batched-selects":
        rows, count_stats = run_count_query_batched(
            candidates,
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            batch_size=args.batch_size,
            min_batch_size=args.min_batch_size,
            pause_ms=args.pause_ms,
            statement_timeout_ms=args.statement_timeout_ms,
            count_cap=args.count_cap,
            retry_on_timeout=args.retry_on_timeout,
            timeout_single_party_as_top_stratum=args.timeout_single_party_as_top_stratum,
            state_file=Path(args.state_file),
            resume=args.resume,
            checkpoint_interval=args.checkpoint_interval,
        )
    else:
        csv_text = run_count_query_temp_table(
            candidates,
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            statement_timeout_ms=args.statement_timeout_ms,
        )
        rows = parse_counts_csv(csv_text)

    print("[3/5] Parsing counts and sampling parties...")
    if len(rows) != len(candidates):
        print(
            f"Warning: count rows ({len(rows)}) differ from candidate size ({len(candidates)}). Proceeding with returned rows.",
            file=sys.stderr,
        )

    by_party: dict[str, dict[str, Any]] = {}
    for row in rows:
        by_party[row["party"]] = row
    rows = list(by_party.values())

    for row in rows:
        row["stratum"] = stratum_label(int(row["dialog_count"]))

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
        "candidate_pool_size": len(candidates),
        "counted_rows": len(rows),
        "sample_size": args.sample_size,
        "strategy": args.strategy,
        "allocation": args.allocation,
        "seed": args.seed,
        "count_mode": args.count_mode,
        "count_cap": args.count_cap,
        "batch_size": args.batch_size,
        "min_batch_size": args.min_batch_size,
        "pause_ms": args.pause_ms,
        "statement_timeout_ms": args.statement_timeout_ms,
        "retry_on_timeout": args.retry_on_timeout,
        "timeout_single_party_as_top_stratum": args.timeout_single_party_as_top_stratum,
        "max_parties_to_count": args.max_parties_to_count,
        "resume": args.resume,
        "state_file": args.state_file,
        "checkpoint_interval": args.checkpoint_interval,
        "count_stats": count_stats,
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
    if count_stats:
        print(
            "Count stats: "
            + ", ".join(f"{k}={v}" for k, v in sorted(count_stats.items()))
        )
    print(build_ascii_table(strata_rows, ["stratum", "available", "selected"]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
