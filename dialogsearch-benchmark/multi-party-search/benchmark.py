#!/usr/bin/env python3
import argparse
import csv
import io
import json
import math
import os
import random
import re
import shlex
import statistics
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ROOT = Path(__file__).resolve().parent
PLACEHOLDER_RE = re.compile(r"\{\{[A-Z_]+\}\}")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Variant:
    name: str
    path: Path
    template: str


@dataclass(frozen=True)
class Candidate:
    party: str
    unprefixed_party_identifier: str
    partyresource_short_prefix: str
    estimated_dialog_count: int
    service_resources: tuple[str, ...]
    band: str
    selected: bool
    skip_reason: str


@dataclass(frozen=True)
class BenchmarkCase:
    case_id: str
    hot_party: str
    unprefixed_party_identifier: str
    estimated_dialog_count: int
    estimated_dialog_count_band: str
    permission_group_count: int
    party_count: int
    service_count: int
    parties: tuple[str, ...]
    services: tuple[str, ...]
    payload_json: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quote_identifier(value: str) -> str:
    if not IDENTIFIER_RE.match(value):
        raise ValueError(f"unsupported SQL identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'


def quote_qualified_identifier(value: str) -> str:
    parts = value.split(".")
    if len(parts) not in (1, 2):
        raise ValueError(f"expected unqualified or schema-qualified table name, got: {value!r}")
    return ".".join(quote_identifier(part) for part in parts)


def command_line(args: argparse.Namespace) -> str:
    argv = getattr(args, "original_argv", None)
    if argv is None:
        return ""
    return " ".join(shlex.quote(part) for part in [Path(__file__).name, *argv])


def parse_int_list(value: str) -> list[int]:
    values = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        parsed = int(part)
        if parsed <= 0:
            raise argparse.ArgumentTypeError("bucket sizes must be positive integers")
        values.append(parsed)
    if not values:
        raise argparse.ArgumentTypeError("at least one bucket size is required")
    return sorted(set(values))


def parse_bucket_pairs(value: str) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        left, sep, right = part.partition(";")
        if not sep:
            raise argparse.ArgumentTypeError("bucket pairs must use PARTY;SERVICE")
        try:
            party_count = int(left.strip())
            service_count = int(right.strip())
        except ValueError as exc:
            raise argparse.ArgumentTypeError("bucket pair values must be integers") from exc
        if party_count <= 0 or service_count <= 0:
            raise argparse.ArgumentTypeError("bucket pair values must be positive integers")
        pair = (party_count, service_count)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    if not pairs:
        raise argparse.ArgumentTypeError("at least one bucket pair is required")
    return pairs


def format_bucket_pairs(pairs: list[tuple[int, int]]) -> str:
    return ",".join(f"{party_count};{service_count}" for party_count, service_count in pairs)


def active_bucket_pairs(args: argparse.Namespace) -> list[tuple[int, int]]:
    if args.buckets:
        return args.buckets
    return [
        (party_bucket, service_bucket)
        for party_bucket in args.party_buckets
        for service_bucket in args.service_buckets
    ]


def relpath(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def psql_command(args: argparse.Namespace) -> list[str]:
    return [
        args.psql,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--dbname",
        args.dbname,
        "--username",
        args.user,
        "--no-psqlrc",
        "--set",
        "ON_ERROR_STOP=1",
        "--tuples-only",
        "--no-align",
        "--quiet",
    ]


def run_psql(args: argparse.Namespace, sql: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    pg_options = (
        f"-c statement_timeout={args.statement_timeout} "
        f"-c lock_timeout={args.lock_timeout}"
    )
    env["PGOPTIONS"] = (env.get("PGOPTIONS", "") + " " + pg_options).strip()
    return subprocess.run(
        psql_command(args),
        input=sql.strip() + "\n",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        env=env,
    )


def require_psql_success(proc: subprocess.CompletedProcess[str], context: str) -> str:
    if proc.returncode != 0:
        raise RuntimeError(
            f"{context} failed with exit code {proc.returncode}\n\nSTDERR:\n{proc.stderr.strip()}"
        )
    return proc.stdout


def copy_csv(args: argparse.Namespace, query: str, context: str) -> list[dict[str, str]]:
    sql = f"COPY (\n{query.strip()}\n) TO STDOUT WITH CSV HEADER;"
    stdout = require_psql_success(run_psql(args, sql), context)
    return list(csv.DictReader(io.StringIO(stdout)))


def fetch_single_column(args: argparse.Namespace, sql: str, context: str) -> str:
    stdout = require_psql_success(run_psql(args, sql), context)
    return stdout.strip()


def load_variants(queries_dir: Path, queries_glob: str) -> list[Variant]:
    if not queries_dir.exists():
        raise FileNotFoundError(f"queries directory does not exist: {queries_dir}")

    variants: list[Variant] = []
    for path in sorted(queries_dir.glob(queries_glob)):
        if not path.is_file():
            continue
        template = path.read_text(encoding="utf-8")
        stripped = template.lstrip()
        if stripped.upper().startswith("EXPLAIN"):
            raise ValueError(f"{path} must not include EXPLAIN; benchmark.py adds it")
        if template.count("{{PARTIES_AND_SERVICES_JSON}}") != 1:
            raise ValueError(
                f"{path} must contain exactly one {{PARTIES_AND_SERVICES_JSON}} placeholder"
            )
        if "{{AS_OF}}" not in template:
            raise ValueError(f"{path} must contain the {{AS_OF}} placeholder")
        variants.append(Variant(path.stem, path, template))

    if not variants:
        raise FileNotFoundError(f"no query files matching {queries_glob!r} found in {queries_dir}")
    return variants


def render_query(variant: Variant, case: BenchmarkCase, as_of: str) -> str:
    sql = variant.template.replace("{{PARTIES_AND_SERVICES_JSON}}", sql_literal(case.payload_json))
    sql = sql.replace("{{AS_OF}}", sql_literal(as_of))
    unresolved = PLACEHOLDER_RE.findall(sql)
    if unresolved:
        raise ValueError(f"{variant.path} has unresolved placeholders: {', '.join(unresolved)}")
    return "EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)\n" + sql.strip().rstrip(";") + ";\n"


def parse_party_urn(party: str) -> str:
    return party.rsplit(":", 1)[-1]


def party_short_prefix(party: str) -> str:
    if ":person:identifier-no:" in party:
        return "p"
    if ":organization:identifier-no:" in party:
        return "o"
    raise ValueError(f"cannot derive partyresource short prefix from party URN: {party}")


def partyresource_key(party: str) -> Optional[tuple[str, str]]:
    try:
        return (parse_party_urn(party), party_short_prefix(party))
    except ValueError:
        return None


def classify_bands(rows: list[dict[str, str]]) -> dict[str, str]:
    bands: dict[str, str] = {}
    total = max(len(rows), 1)
    for index, row in enumerate(rows):
        percentile = index / total
        party = row["Party"]
        if percentile < 0.01:
            bands[party] = "very_hot"
        elif percentile < 0.05:
            bands[party] = "hot"
        elif percentile < 0.25:
            bands[party] = "medium"
        else:
            bands[party] = "long_tail"
    return bands


def fetch_party_stats_status(args: argparse.Namespace) -> dict[str, str]:
    rows = copy_csv(
        args,
        """
SELECT
    schemaname,
    tablename,
    attname,
    n_distinct::text AS n_distinct,
    (most_common_vals IS NULL)::text AS most_common_vals_missing,
    (most_common_freqs IS NULL)::text AS most_common_freqs_missing,
    COALESCE(array_length(most_common_vals::text::text[], 1), 0)::text AS most_common_vals_count,
    COALESCE(array_length(most_common_freqs::float8[], 1), 0)::text AS most_common_freqs_count
FROM pg_stats
WHERE tablename = 'Dialog'
  AND attname = 'Party'
ORDER BY CASE WHEN schemaname = 'public' THEN 0 ELSE 1 END, schemaname
LIMIT 1
""",
        "Dialog.Party pg_stats status query",
    )
    return rows[0] if rows else {}


def has_usable_party_mcv_stats(status: dict[str, str]) -> bool:
    if not status:
        return False
    return (
        status.get("most_common_vals_missing") == "false"
        and status.get("most_common_freqs_missing") == "false"
        and int(status.get("most_common_vals_count") or "0") > 0
        and int(status.get("most_common_freqs_count") or "0") > 0
    )


def analyze_dialog_party_stats(args: argparse.Namespace) -> None:
    sql = 'ANALYZE "Dialog" ("Party");'
    require_psql_success(run_psql(args, sql), "ANALYZE Dialog(Party)")


def should_analyze_dialog_party_stats(args: argparse.Namespace, status: dict[str, str]) -> bool:
    if args.analyze_dialog_party_stats:
        return True
    if not sys.stdin.isatty():
        return False

    if status:
        details = (
            f"schemaname={status.get('schemaname')}, "
            f"n_distinct={status.get('n_distinct')}, "
            f"most_common_vals_count={status.get('most_common_vals_count')}, "
            f"most_common_freqs_count={status.get('most_common_freqs_count')}"
        )
    else:
        details = 'no pg_stats row exists for Dialog."Party"'

    print(
        'No usable pg_stats MCV data exists for Dialog."Party"; refusing to scan or group '
        f"the Dialog table to discover candidates. Stats status: {details}.",
        file=sys.stderr,
    )
    answer = input('Run ANALYZE "Dialog" ("Party") now? [y/N] ')
    return answer.strip().lower() in ("y", "yes")


def ensure_party_mcv_stats(args: argparse.Namespace) -> dict[str, str]:
    status = fetch_party_stats_status(args)
    if has_usable_party_mcv_stats(status):
        return status

    if should_analyze_dialog_party_stats(args, status):
        print('Dialog."Party" MCV stats are missing; running ANALYZE "Dialog" ("Party")...', file=sys.stderr)
        analyze_dialog_party_stats(args)
        status = fetch_party_stats_status(args)
        if has_usable_party_mcv_stats(status):
            return status

        raise RuntimeError(
            'ANALYZE "Dialog" ("Party") completed, but pg_stats still has no usable '
            'most_common_vals/most_common_freqs for Dialog."Party". Consider increasing the '
            'statistics target for the Party column and analyzing again.'
        )

    if status:
        details = (
            f"schemaname={status.get('schemaname')}, "
            f"n_distinct={status.get('n_distinct')}, "
            f"most_common_vals_count={status.get('most_common_vals_count')}, "
            f"most_common_freqs_count={status.get('most_common_freqs_count')}"
        )
    else:
        details = 'no pg_stats row exists for Dialog."Party"'

    raise RuntimeError(
        'No usable pg_stats MCV data exists for Dialog."Party"; refusing to scan or group '
        'the Dialog table to discover candidates. '
        f"Stats status: {details}. "
        'Run ANALYZE yourself, or rerun this script with --analyze-dialog-party-stats '
        'to explicitly allow ANALYZE "Dialog" ("Party").'
    )


def sampled_distribution_population_query(args: argparse.Namespace) -> str:
    table_name = quote_qualified_identifier(args.sampled_candidate_table)
    sample_percent = args.sampled_candidate_percent
    repeatable_seed = args.sampled_candidate_seed
    return f"""
DROP TABLE IF EXISTS {table_name};

CREATE UNLOGGED TABLE {table_name} AS
SELECT
    "Party"::text AS "Party",
    count(*)::bigint AS sample_count,
    greatest(1, round(count(*) / ({sample_percent}::numeric / 100.0))::bigint) AS estimated_count
FROM "Dialog" TABLESAMPLE SYSTEM ({sample_percent}) REPEATABLE ({repeatable_seed})
WHERE "Party" IS NOT NULL
GROUP BY "Party";

CREATE INDEX ON {table_name} (estimated_count DESC);
CREATE INDEX ON {table_name} ("Party");
ANALYZE {table_name};
""".strip()


def ensure_sampled_distribution_table(args: argparse.Namespace) -> None:
    table_name = args.sampled_candidate_table
    quoted_table_name = quote_qualified_identifier(table_name)
    exists = fetch_single_column(
        args,
        f"SELECT (to_regclass({sql_literal(table_name)}) IS NOT NULL)::text;",
        "sampled candidate distribution table existence query",
    )
    if exists != "true":
        raise RuntimeError(
            f"sampled candidate distribution table {table_name!r} does not exist.\n\n"
            "Populate it before using "
            f"--candidate-source {args.candidate_source}:\n\n"
            f"{sampled_distribution_population_query(args)}"
        )

    has_rows = fetch_single_column(
        args,
        f"SELECT EXISTS (SELECT 1 FROM {quoted_table_name} LIMIT 1)::text;",
        "sampled candidate distribution table emptiness query",
    )
    if has_rows == "true":
        return

    raise RuntimeError(
        f"sampled candidate distribution table {table_name!r} is empty.\n\n"
        "Populate it before using "
        f"--candidate-source {args.candidate_source}:\n\n"
        f"{sampled_distribution_population_query(args)}"
    )


def fetch_mcv_candidate_parties(args: argparse.Namespace) -> list[dict[str, str]]:
    ensure_party_mcv_stats(args)
    query = f"""
SELECT "Party", estimated_count
FROM (
    SELECT
        unnest(most_common_vals::text::text[]) AS "Party",
        (unnest(most_common_freqs::float8[]) *
            (SELECT reltuples FROM pg_class WHERE relname = 'Dialog'))::bigint AS estimated_count
    FROM pg_stats
    WHERE tablename = 'Dialog'
      AND attname = 'Party'
) s
ORDER BY estimated_count DESC
LIMIT {int(args.party_limit)}
"""
    return copy_csv(args, query, "candidate party query")


def fetch_sampled_candidate_parties(args: argparse.Namespace) -> list[dict[str, str]]:
    ensure_sampled_distribution_table(args)
    table_name = quote_qualified_identifier(args.sampled_candidate_table)
    per_band_limit = max(1, math.ceil(args.party_limit / 4))
    query = f"""
WITH ranked AS (
    SELECT
        "Party",
        estimated_count,
        cume_dist() OVER (ORDER BY estimated_count DESC, "Party") AS rank_fraction
    FROM {table_name}
    WHERE "Party" IS NOT NULL
      AND estimated_count > 0
), banded AS (
    SELECT
        "Party",
        estimated_count,
        CASE
            WHEN rank_fraction <= 0.01 THEN 'very_hot'
            WHEN rank_fraction <= 0.05 THEN 'hot'
            WHEN rank_fraction <= 0.25 THEN 'medium'
            ELSE 'long_tail'
        END AS distribution_band
    FROM ranked
), sampled AS (
    SELECT
        "Party",
        estimated_count,
        row_number() OVER (
            PARTITION BY distribution_band
            ORDER BY md5("Party" || {sql_literal(str(args.shuffle_seed))})
        ) AS band_sample_rank
    FROM banded
)
SELECT "Party", estimated_count
FROM sampled
WHERE band_sample_rank <= {per_band_limit}
ORDER BY estimated_count DESC, "Party"
LIMIT {int(args.party_limit)}
"""
    return copy_csv(args, query, "sampled candidate party query")


def fetch_candidate_parties(args: argparse.Namespace) -> list[dict[str, str]]:
    if args.candidate_source == "pg-stats-mcv":
        return fetch_mcv_candidate_parties(args)
    if args.candidate_source == "sampled-dialog-distribution":
        return fetch_sampled_candidate_parties(args)
    if args.candidate_source == "mcv-plus-sampled-dialog-distribution":
        mcv_args = argparse.Namespace(**vars(args))
        sampled_args = argparse.Namespace(**vars(args))
        mcv_args.party_limit = max(1, args.party_limit // 2)
        sampled_args.party_limit = max(1, args.party_limit - mcv_args.party_limit)
        merged: dict[str, dict[str, str]] = {}
        for row in fetch_sampled_candidate_parties(sampled_args):
            merged[row["Party"]] = row
        for row in fetch_mcv_candidate_parties(mcv_args):
            merged[row["Party"]] = row
        return sorted(
            merged.values(),
            key=lambda row: (-int(float(row["estimated_count"])), row["Party"]),
        )
    raise ValueError(f"unknown candidate source: {args.candidate_source}")


def fetch_service_resources(args: argparse.Namespace, parties: list[tuple[str, str]]) -> dict[tuple[str, str], list[str]]:
    if not parties:
        return {}
    values = ", ".join(
        f"({sql_literal(unprefixed)}, {sql_literal(short_prefix)})"
        for unprefixed, short_prefix in parties
    )
    query = f"""
WITH wanted(unprefixed_party_identifier, short_prefix) AS (
    VALUES {values}
)
SELECT
    p."UnprefixedPartyIdentifier" AS unprefixed_party_identifier,
    p."ShortPrefix" AS short_prefix,
    'urn:altinn:resource:' || r."UnprefixedResourceIdentifier" AS service_resource
FROM wanted w
JOIN partyresource."Party" p
    ON p."UnprefixedPartyIdentifier" = w.unprefixed_party_identifier
   AND p."ShortPrefix" = w.short_prefix
JOIN partyresource."PartyResource" pr
    ON pr."PartyId" = p."Id"
JOIN partyresource."Resource" r
    ON pr."ResourceId" = r."Id"
ORDER BY p."UnprefixedPartyIdentifier", r."UnprefixedResourceIdentifier"
"""
    rows = copy_csv(args, query, "party service-resource query")
    resources: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in rows:
        key = (row["unprefixed_party_identifier"], row["short_prefix"])
        resources[key].append(row["service_resource"])
    return resources


def build_candidates(args: argparse.Namespace) -> list[Candidate]:
    rows = fetch_candidate_parties(args)
    bands = classify_bands(rows)
    parties = [
        key
        for row in rows
        for key in [partyresource_key(row["Party"])]
        if key is not None
    ]
    resource_map = fetch_service_resources(args, parties)
    min_bucket = min(service_bucket for _, service_bucket in active_bucket_pairs(args))

    candidates: list[Candidate] = []
    for row in rows:
        party = row["Party"]
        unprefixed = parse_party_urn(party)
        key = partyresource_key(party)
        short_prefix = key[1] if key else ""
        resources = tuple(resource_map.get(key, [])) if key else ()
        if key is None:
            skip_reason = "unsupported_party_urn_for_partyresource_lookup"
        elif len(resources) < min_bucket:
            skip_reason = f"only_{len(resources)}_service_resources_below_min_bucket_{min_bucket}"
        else:
            skip_reason = ""
        selected = not skip_reason
        candidates.append(
            Candidate(
                party=party,
                unprefixed_party_identifier=unprefixed,
                partyresource_short_prefix=short_prefix,
                estimated_dialog_count=int(float(row["estimated_count"])),
                service_resources=resources,
                band=bands[party],
                selected=selected,
                skip_reason=skip_reason,
            )
        )
    return candidates


def select_candidates(args: argparse.Namespace, candidates: list[Candidate]) -> list[Candidate]:
    eligible = [candidate for candidate in candidates if candidate.selected]
    if args.candidate_strategy == "top":
        return choose_until_case_target(args, eligible)

    by_band: dict[str, list[Candidate]] = defaultdict(list)
    for candidate in eligible:
        by_band[candidate.band].append(candidate)

    ordered: list[Candidate] = []
    band_order = ["very_hot", "hot", "medium", "long_tail"]
    while True:
        added = False
        for band in band_order:
            if by_band[band]:
                ordered.append(by_band[band].pop(0))
                added = True
                if has_enough_selected_candidates(args, ordered, eligible):
                    return ordered
        if not added:
            return ordered


def has_enough_selected_candidates(
    args: argparse.Namespace,
    selected: list[Candidate],
    eligible: list[Candidate],
) -> bool:
    target_party_count = min(max(party_bucket for party_bucket, _ in active_bucket_pairs(args)), len(eligible))
    return len(selected) >= target_party_count and count_cases_for_candidates(args, selected) >= args.cases


def count_cases_for_candidates(args: argparse.Namespace, candidates: list[Candidate]) -> int:
    count = 0
    for candidate in candidates:
        for party_bucket, service_bucket in active_bucket_pairs(args):
            parties = choose_case_parties(args, candidates, candidate, party_bucket)
            if len(parties) < party_bucket:
                continue
            service_pool = service_union(parties)
            if len(service_pool) >= service_bucket:
                count += 1
            if count >= args.cases:
                return count
    return count


def choose_case_parties(
    args: argparse.Namespace,
    candidates: list[Candidate],
    seed: Candidate,
    party_bucket: int,
) -> list[Candidate]:
    if party_bucket <= 0:
        return []

    band_order = ["very_hot", "hot", "medium", "long_tail"]
    try:
        seed_band_index = band_order.index(seed.band)
    except ValueError:
        seed_band_index = 0

    def sort_key(candidate: Candidate) -> tuple[int, int, int, str]:
        try:
            band_distance = abs(band_order.index(candidate.band) - seed_band_index)
        except ValueError:
            band_distance = len(band_order)
        same_band = 0 if candidate.band == seed.band else 1
        return (same_band, band_distance, -candidate.estimated_dialog_count, candidate.party)

    selected = [seed]
    if len(selected) >= party_bucket:
        return selected
    for candidate in sorted(candidates, key=sort_key):
        if candidate.party == seed.party:
            continue
        selected.append(candidate)
        if len(selected) >= party_bucket:
            break
    return selected


def service_union(candidates: list[Candidate]) -> tuple[str, ...]:
    services: set[str] = set()
    for candidate in candidates:
        services.update(candidate.service_resources)
    return tuple(sorted(services))


def choose_case_services(
    args: argparse.Namespace,
    services: tuple[str, ...],
    case_index: int,
    service_bucket: int,
) -> tuple[str, ...]:
    if len(services) < service_bucket:
        return ()
    shuffled = list(services)
    random.Random(args.shuffle_seed + case_index * 1009 + service_bucket).shuffle(shuffled)
    return tuple(sorted(shuffled[:service_bucket]))


def build_payload(parties: tuple[str, ...], services: tuple[str, ...]) -> str:
    return json.dumps(
        [
            {
                "Parties": list(parties),
                "Services": list(services),
            }
        ],
        separators=(",", ":"),
    )


def count_bucket_cases(args: argparse.Namespace, candidates: list[Candidate], seed: Candidate) -> int:
    count = 0
    for party_bucket, service_bucket in active_bucket_pairs(args):
        parties = choose_case_parties(args, candidates, seed, party_bucket)
        if len(parties) < party_bucket:
            continue
        service_pool = service_union(parties)
        if len(service_pool) >= service_bucket:
            count += 1
    return count


def choose_until_case_target(args: argparse.Namespace, candidates: list[Candidate]) -> list[Candidate]:
    selected: list[Candidate] = []
    for candidate in candidates:
        selected.append(candidate)
        if has_enough_selected_candidates(args, selected, candidates):
            break
    return selected


def build_cases(args: argparse.Namespace, candidates: list[Candidate]) -> list[BenchmarkCase]:
    cases: list[BenchmarkCase] = []
    if not candidates:
        return cases

    bucket_pairs = active_bucket_pairs(args)
    max_attempts = max(args.cases * len(bucket_pairs) * 2, len(candidates) * len(bucket_pairs))
    seen: set[tuple[str, int, int]] = set()

    for attempt in range(max_attempts):
        candidate = candidates[attempt % len(candidates)]
        party_bucket, service_bucket = bucket_pairs[attempt % len(bucket_pairs)]
        key = (candidate.party, party_bucket, service_bucket)
        if key in seen:
            continue
        seen.add(key)

        party_candidates = choose_case_parties(args, candidates, candidate, party_bucket)
        if len(party_candidates) < party_bucket:
            continue
        parties = tuple(sorted(party.party for party in party_candidates))
        service_pool = service_union(party_candidates)
        services = choose_case_services(args, service_pool, len(cases) + 1, service_bucket)
        if len(services) < service_bucket:
            continue
        case_id = f"case_{len(cases) + 1:04d}_p_{party_bucket}_s_{service_bucket}"
        cases.append(
            BenchmarkCase(
                case_id=case_id,
                hot_party=candidate.party,
                unprefixed_party_identifier=candidate.unprefixed_party_identifier,
                estimated_dialog_count=candidate.estimated_dialog_count,
                estimated_dialog_count_band=candidate.band,
                permission_group_count=1,
                party_count=party_bucket,
                service_count=service_bucket,
                parties=parties,
                services=services,
                payload_json=build_payload(parties, services),
            )
        )
        if len(cases) >= args.cases:
            return cases
    return cases


def write_candidates_csv(path: Path, candidates: list[Candidate]) -> None:
    fieldnames = [
        "party",
        "unprefixed_party_identifier",
        "partyresource_short_prefix",
        "estimated_dialog_count",
        "estimated_dialog_count_band",
        "service_resource_count",
        "selected",
        "skip_reason",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(
                {
                    "party": candidate.party,
                    "unprefixed_party_identifier": candidate.unprefixed_party_identifier,
                    "partyresource_short_prefix": candidate.partyresource_short_prefix,
                    "estimated_dialog_count": candidate.estimated_dialog_count,
                    "estimated_dialog_count_band": candidate.band,
                    "service_resource_count": len(candidate.service_resources),
                    "selected": candidate.selected,
                    "skip_reason": candidate.skip_reason,
                }
            )


def write_cases_csv(path: Path, cases: list[BenchmarkCase]) -> None:
    fieldnames = [
        "case_id",
        "hot_party",
        "unprefixed_party_identifier",
        "estimated_dialog_count",
        "estimated_dialog_count_band",
        "permission_group_count",
        "party_count",
        "service_count",
        "payload_json",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for case in cases:
            writer.writerow(
                {
                    "case_id": case.case_id,
                    "hot_party": case.hot_party,
                    "unprefixed_party_identifier": case.unprefixed_party_identifier,
                    "estimated_dialog_count": case.estimated_dialog_count,
                    "estimated_dialog_count_band": case.estimated_dialog_count_band,
                    "permission_group_count": case.permission_group_count,
                    "party_count": case.party_count,
                    "service_count": case.service_count,
                    "payload_json": case.payload_json,
                }
            )


def capture_metadata(args: argparse.Namespace) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    metadata["captured_at"] = utc_now_iso()
    metadata["version"] = fetch_single_column(args, "SELECT version();", "PostgreSQL version query")
    metadata["current_database"] = fetch_single_column(
        args, "SELECT current_database();", "current database query"
    )

    settings = {}
    for setting in [
        "shared_buffers",
        "effective_cache_size",
        "work_mem",
        "random_page_cost",
        "seq_page_cost",
        "track_io_timing",
        "jit",
        "max_parallel_workers_per_gather",
    ]:
        settings[setting] = fetch_single_column(args, f"SHOW {setting};", f"SHOW {setting}")
    metadata["settings"] = settings

    table_rows = copy_csv(
        args,
        """
SELECT
    c.reltuples::bigint AS estimated_rows,
    pg_total_relation_size(c.oid) AS total_relation_size_bytes,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_relation_size_pretty
FROM pg_class c
WHERE c.relname = 'Dialog'
ORDER BY c.oid
LIMIT 1
""",
        "Dialog table metadata query",
    )
    metadata["dialog_table"] = table_rows[0] if table_rows else {}

    metadata["dialog_indexes"] = copy_csv(
        args,
        """
SELECT schemaname, indexname, indexdef
FROM pg_indexes
WHERE tablename = 'Dialog'
ORDER BY schemaname, indexname
""",
        "Dialog index metadata query",
    )
    return metadata


MANIFEST_FIELDS = [
    "run_id",
    "pass",
    "case_id",
    "run_index",
    "variant",
    "variant_order",
    "permission_group_count",
    "party_count",
    "service_count",
    "hot_party",
    "unprefixed_party_identifier",
    "estimated_dialog_count",
    "estimated_dialog_count_band",
    "as_of",
    "sql_file",
    "raw_output_file",
    "stderr_file",
    "payload_file",
    "started_at",
    "finished_at",
    "exit_code",
]


def open_manifest(path: Path) -> tuple[Any, csv.DictWriter]:
    handle = path.open("w", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
    writer.writeheader()
    handle.flush()
    return handle, writer


def append_manifest(handle: Any, writer: csv.DictWriter, row: dict[str, Any]) -> None:
    writer.writerow({field: row.get(field, "") for field in MANIFEST_FIELDS})
    handle.flush()


def explain_output_path(run_dir: Path, case: BenchmarkCase, pass_index: int, run_index: int, variant: Variant) -> Path:
    return run_dir / "raw" / case.case_id / f"pass_{pass_index:03d}" / f"run_{run_index}" / variant.name / "explain.json"


def rendered_sql_path(run_dir: Path, case: BenchmarkCase, pass_index: int, run_index: int, variant: Variant) -> Path:
    return run_dir / "raw" / case.case_id / f"pass_{pass_index:03d}" / f"run_{run_index}" / variant.name / "query.sql"


def stderr_path(run_dir: Path, case: BenchmarkCase, pass_index: int, run_index: int, variant: Variant) -> Path:
    return run_dir / "raw" / case.case_id / f"pass_{pass_index:03d}" / f"run_{run_index}" / variant.name / "stderr.txt"


def payload_path(run_dir: Path, case: BenchmarkCase) -> Path:
    return run_dir / "raw" / case.case_id / "payload.json"


def execute_benchmark(
    args: argparse.Namespace,
    run_dir: Path,
    variants: list[Variant],
    cases: list[BenchmarkCase],
    manifest_handle: Any,
    manifest_writer: csv.DictWriter,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for pass_index in range(1, args.passes + 1):
        shuffled_cases = list(cases)
        random.Random(args.shuffle_seed + pass_index).shuffle(shuffled_cases)

        for case_position, case in enumerate(shuffled_cases, start=1):
            for run_index in range(1, args.runs_per_variant + 1):
                ordered_variants = list(variants)
                random.Random(
                    args.shuffle_seed + pass_index * 100000 + case_position * 100 + run_index
                ).shuffle(ordered_variants)

                for variant_order, variant in enumerate(ordered_variants, start=1):
                    started_at = utc_now_iso()
                    query = render_query(variant, case, args.as_of)
                    sql_path = rendered_sql_path(run_dir, case, pass_index, run_index, variant)
                    raw_path = explain_output_path(run_dir, case, pass_index, run_index, variant)
                    err_path = stderr_path(run_dir, case, pass_index, run_index, variant)
                    sql_path.parent.mkdir(parents=True, exist_ok=True)
                    sql_path.write_text(query, encoding="utf-8")
                    case_payload_path = payload_path(run_dir, case)
                    case_payload_path.parent.mkdir(parents=True, exist_ok=True)
                    if not case_payload_path.exists():
                        case_payload_path.write_text(case.payload_json + "\n", encoding="utf-8")

                    if args.dry_run:
                        finished_at = utc_now_iso()
                        row = {
                            "run_id": f"p{pass_index:03d}_{case.case_id}_r{run_index}_{variant.name}",
                            "pass": pass_index,
                            "case_id": case.case_id,
                            "run_index": run_index,
                            "variant": variant.name,
                            "variant_order": variant_order,
                            "permission_group_count": case.permission_group_count,
                            "party_count": case.party_count,
                            "service_count": case.service_count,
                            "hot_party": case.hot_party,
                            "unprefixed_party_identifier": case.unprefixed_party_identifier,
                            "estimated_dialog_count": case.estimated_dialog_count,
                            "estimated_dialog_count_band": case.estimated_dialog_count_band,
                            "as_of": args.as_of,
                            "sql_file": relpath(sql_path, run_dir),
                            "raw_output_file": "",
                            "stderr_file": "",
                            "payload_file": relpath(case_payload_path, run_dir),
                            "started_at": started_at,
                            "finished_at": finished_at,
                            "exit_code": "DRY_RUN",
                        }
                        append_manifest(manifest_handle, manifest_writer, row)
                        rows.append(row)
                        continue

                    proc = run_psql(args, query)
                    finished_at = utc_now_iso()
                    raw_path.write_text(proc.stdout, encoding="utf-8")
                    err_path.write_text(proc.stderr, encoding="utf-8")
                    row = {
                        "run_id": f"p{pass_index:03d}_{case.case_id}_r{run_index}_{variant.name}",
                        "pass": pass_index,
                        "case_id": case.case_id,
                        "run_index": run_index,
                        "variant": variant.name,
                        "variant_order": variant_order,
                        "permission_group_count": case.permission_group_count,
                        "party_count": case.party_count,
                        "service_count": case.service_count,
                        "hot_party": case.hot_party,
                        "unprefixed_party_identifier": case.unprefixed_party_identifier,
                        "estimated_dialog_count": case.estimated_dialog_count,
                        "estimated_dialog_count_band": case.estimated_dialog_count_band,
                        "as_of": args.as_of,
                        "sql_file": relpath(sql_path, run_dir),
                        "raw_output_file": relpath(raw_path, run_dir),
                        "stderr_file": relpath(err_path, run_dir),
                        "payload_file": relpath(case_payload_path, run_dir),
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "exit_code": proc.returncode,
                    }
                    append_manifest(manifest_handle, manifest_writer, row)
                    rows.append(row)

                    if proc.returncode != 0 and not args.keep_going:
                        raise RuntimeError(
                            f"{variant.name} failed for {case.case_id}; see {err_path}"
                        )

    return rows


def parse_explain_json(raw_path: Path) -> dict[str, Any]:
    data = json.loads(raw_path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError(f"unexpected EXPLAIN JSON shape in {raw_path}")
    root = data[0]
    plan = root["Plan"]

    node_types: Counter[str] = Counter()
    index_names: Counter[str] = Counter()
    relation_names: Counter[str] = Counter()
    sort_methods: Counter[str] = Counter()
    rows_removed = 0

    def walk(node: dict[str, Any]) -> None:
        nonlocal rows_removed
        node_type = node.get("Node Type")
        if node_type:
            node_types[node_type] += 1
        if node.get("Index Name"):
            index_names[node["Index Name"]] += 1
        if node.get("Relation Name"):
            relation_names[node["Relation Name"]] += 1
        if node.get("Sort Method"):
            sort_methods[node["Sort Method"]] += 1
        rows_removed += int(node.get("Rows Removed by Filter", 0) or 0)
        rows_removed += int(node.get("Rows Removed by Join Filter", 0) or 0)
        for child in node.get("Plans", []) or []:
            walk(child)

    walk(plan)

    shared_hit = numeric(plan.get("Shared Hit Blocks"))
    shared_read = numeric(plan.get("Shared Read Blocks"))
    shared_total = shared_hit + shared_read

    metrics = {
        "planning_time_ms": numeric(root.get("Planning Time")),
        "execution_time_ms": numeric(root.get("Execution Time")),
        "actual_rows": numeric(plan.get("Actual Rows")),
        "actual_loops": numeric(plan.get("Actual Loops")),
        "shared_hit_blocks": shared_hit,
        "shared_read_blocks": shared_read,
        "shared_dirtied_blocks": numeric(plan.get("Shared Dirtied Blocks")),
        "shared_written_blocks": numeric(plan.get("Shared Written Blocks")),
        "local_hit_blocks": numeric(plan.get("Local Hit Blocks")),
        "local_read_blocks": numeric(plan.get("Local Read Blocks")),
        "local_dirtied_blocks": numeric(plan.get("Local Dirtied Blocks")),
        "local_written_blocks": numeric(plan.get("Local Written Blocks")),
        "temp_read_blocks": numeric(plan.get("Temp Read Blocks")),
        "temp_written_blocks": numeric(plan.get("Temp Written Blocks")),
        "io_read_time_ms": numeric(plan.get("I/O Read Time")),
        "io_write_time_ms": numeric(plan.get("I/O Write Time")),
        "shared_hit_ratio": (shared_hit / shared_total) if shared_total else None,
        "rows_removed": rows_removed,
        "node_types": dict(node_types),
        "index_names": dict(index_names),
        "relation_names": dict(relation_names),
        "sort_methods": dict(sort_methods),
        "jit": root.get("JIT", {}),
    }
    return metrics


def numeric(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def summarize_runs(run_dir: Path, manifest_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    report_rows: list[dict[str, Any]] = []
    for row in manifest_rows:
        if str(row["exit_code"]) != "0":
            continue
        raw_file = run_dir / str(row["raw_output_file"])
        try:
            metrics = parse_explain_json(raw_file)
        except Exception as exc:
            metrics = {"parse_error": str(exc)}

        flat = dict(row)
        for key, value in metrics.items():
            if isinstance(value, dict):
                flat[key] = "|".join(f"{name}:{count}" for name, count in sorted(value.items()))
            else:
                flat[key] = value
        report_rows.append(flat)
    return report_rows


def percentile(values: list[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil((pct / 100) * len(ordered)) - 1))
    return ordered[index]


def median(values: list[float]) -> Optional[float]:
    if not values:
        return None
    return float(statistics.median(values))


def format_number(value: Any, digits: int = 2) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.{digits}f}"


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    base_fields = MANIFEST_FIELDS + [
        "planning_time_ms",
        "execution_time_ms",
        "actual_rows",
        "actual_loops",
        "shared_hit_blocks",
        "shared_read_blocks",
        "shared_dirtied_blocks",
        "shared_written_blocks",
        "local_hit_blocks",
        "local_read_blocks",
        "local_dirtied_blocks",
        "local_written_blocks",
        "temp_read_blocks",
        "temp_written_blocks",
        "io_read_time_ms",
        "io_write_time_ms",
        "shared_hit_ratio",
        "rows_removed",
        "node_types",
        "index_names",
        "relation_names",
        "sort_methods",
        "jit",
        "parse_error",
    ]
    fields = [field for field in base_fields if any(field in row for row in rows)]
    if not fields:
        fields = base_fields
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def float_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values = []
    for row in rows:
        value = row.get(key)
        if value in ("", None):
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    return values


def group_summary(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row[key] for key in keys)].append(row)

    summaries: list[dict[str, Any]] = []
    for key_values, group in sorted(grouped.items()):
        execution_times = float_values(group, "execution_time_ms")
        shared_reads = float_values(group, "shared_read_blocks")
        shared_hits = float_values(group, "shared_hit_blocks")
        hit_ratios = float_values(group, "shared_hit_ratio")
        temp_blocks = []
        for row in group:
            temp_read = float_values([row], "temp_read_blocks")
            temp_written = float_values([row], "temp_written_blocks")
            if temp_read and temp_written:
                temp_blocks.append(temp_read[0] + temp_written[0])
        index_counter: Counter[str] = Counter()
        node_counter: Counter[str] = Counter()
        for row in group:
            for item in str(row.get("index_names", "")).split("|"):
                if ":" in item:
                    name, count = item.rsplit(":", 1)
                    index_counter[name] += int(count)
            for item in str(row.get("node_types", "")).split("|"):
                if ":" in item:
                    name, count = item.rsplit(":", 1)
                    node_counter[name] += int(count)

        summary = {keys[index]: key_values[index] for index in range(len(keys))}
        summary.update(
            {
                "samples": len(group),
                "median_execution_ms": median(execution_times),
                "p90_execution_ms": percentile(execution_times, 90),
                "min_execution_ms": min(execution_times) if execution_times else None,
                "max_execution_ms": max(execution_times) if execution_times else None,
                "median_shared_read_blocks": median(shared_reads),
                "median_shared_hit_blocks": median(shared_hits),
                "median_shared_hit_ratio": median(hit_ratios),
                "median_temp_blocks": median(temp_blocks),
                "common_indexes": ", ".join(name for name, _ in index_counter.most_common(5)),
                "common_node_types": ", ".join(name for name, _ in node_counter.most_common(8)),
            }
        )
        summaries.append(summary)
    return summaries


def winners_by_case(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["case_id"], str(row["run_index"]))].append(row)

    winners = []
    for (case_id, run_index), group in sorted(grouped.items()):
        best = min(group, key=lambda row: float(row["execution_time_ms"]))
        winners.append(
            {
                "case_id": case_id,
                "run_index": run_index,
                "winner": best["variant"],
                "execution_time_ms": float(best["execution_time_ms"]),
                "party_count": best["party_count"],
                "service_count": best["service_count"],
                "estimated_dialog_count_band": best["estimated_dialog_count_band"],
            }
        )
    return winners


def markdown_table(rows: list[dict[str, Any]], columns: list[str], limit: Optional[int] = None) -> str:
    if limit is not None:
        rows = rows[:limit]
    if not rows:
        return "_No rows._\n"

    lines = []
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for row in rows:
        values = []
        for column in columns:
            values.append(format_number(row.get(column)))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def write_reports(
    run_dir: Path,
    args: argparse.Namespace,
    variants: list[Variant],
    metadata: dict[str, Any],
    candidates: list[Candidate],
    cases: list[BenchmarkCase],
    manifest_rows: list[dict[str, Any]],
) -> None:
    report_rows = summarize_runs(run_dir, manifest_rows)
    write_report_csv(run_dir / "report.csv", report_rows)

    by_variant_run = group_summary(report_rows, ["variant", "run_index"])
    by_party_bucket = group_summary(report_rows, ["variant", "party_count", "run_index"])
    by_service_bucket = group_summary(report_rows, ["variant", "service_count", "run_index"])
    by_band = group_summary(report_rows, ["variant", "estimated_dialog_count_band", "run_index"])
    winners = winners_by_case(report_rows) if report_rows else []

    report_json = {
        "config": serializable_config(args),
        "command_line": command_line(args),
        "metadata": metadata,
        "sample_count": len(report_rows),
        "summary_by_variant_run": by_variant_run,
        "summary_by_party_bucket": by_party_bucket,
        "summary_by_service_bucket": by_service_bucket,
        "summary_by_band": by_band,
        "winners_by_case": winners,
    }
    (run_dir / "report.json").write_text(json.dumps(report_json, indent=2), encoding="utf-8")

    selected_candidates = [candidate for candidate in candidates if candidate.selected]
    lines = [
        "# Multi-Party SQL Benchmark Report",
        "",
        f"- Run directory: `{run_dir}`",
        f"- Generated at: `{utc_now_iso()}`",
        f"- Command line: `{command_line(args)}`",
        f"- Dry run: `{args.dry_run}`",
        f"- Variants: `{len(variants)}`",
        f"- Queries glob: `{args.queries_glob}`",
        f"- Cases: `{len(cases)}`",
        f"- Passes: `{args.passes}`",
        f"- Runs per variant per case: `{args.runs_per_variant}`",
        f"- Bucket pairs: `{format_bucket_pairs(active_bucket_pairs(args))}`",
        f"- As-of timestamp: `{args.as_of}`",
        f"- Shuffle seed: `{args.shuffle_seed}`",
        "",
        "## Environment",
        "",
        f"- PostgreSQL: `{metadata.get('version', '')}`",
        f"- Database: `{metadata.get('current_database', '')}`",
        f"- Dialog estimated rows: `{metadata.get('dialog_table', {}).get('estimated_rows', '')}`",
        f"- Dialog total size: `{metadata.get('dialog_table', {}).get('total_relation_size_pretty', '')}`",
        "",
        "## Settings",
        "",
    ]
    for key, value in metadata.get("settings", {}).items():
        lines.append(f"- `{key}`: `{value}`")

    lines.extend(
        [
            "",
            "## Candidate Distribution",
            "",
            f"- Candidate source: `{args.candidate_source}`",
            f"- Candidate parties fetched: `{len(candidates)}`",
            f"- Candidate parties with at least the minimum service bucket: `{len(selected_candidates)}`",
            f"- Benchmark cases generated from selected parties: `{len(cases)}`",
            "",
            "## Summary By Variant And Run",
            "",
            markdown_table(
                by_variant_run,
                [
                    "variant",
                    "run_index",
                    "samples",
                    "median_execution_ms",
                    "p90_execution_ms",
                    "median_shared_read_blocks",
                    "median_shared_hit_blocks",
                    "median_shared_hit_ratio",
                    "common_indexes",
                ],
            ),
            "## Summary By Party Bucket",
            "",
            markdown_table(
                by_party_bucket,
                [
                    "variant",
                    "party_count",
                    "run_index",
                    "samples",
                    "median_execution_ms",
                    "p90_execution_ms",
                    "median_shared_read_blocks",
                    "median_shared_hit_blocks",
                ],
            ),
            "## Summary By Service Bucket",
            "",
            markdown_table(
                by_service_bucket,
                [
                    "variant",
                    "service_count",
                    "run_index",
                    "samples",
                    "median_execution_ms",
                    "p90_execution_ms",
                    "median_shared_read_blocks",
                    "median_shared_hit_blocks",
                ],
            ),
            "## Summary By Estimated Row Count Band",
            "",
            markdown_table(
                by_band,
                [
                    "variant",
                    "estimated_dialog_count_band",
                    "run_index",
                    "samples",
                    "median_execution_ms",
                    "p90_execution_ms",
                    "median_shared_read_blocks",
                    "median_shared_hit_blocks",
                ],
            ),
            "## Per-Case Winners",
            "",
            markdown_table(
                winners,
                [
                    "case_id",
                    "run_index",
                    "winner",
                    "execution_time_ms",
                    "party_count",
                    "service_count",
                    "estimated_dialog_count_band",
                ],
                limit=100,
            ),
            "## Cache Notes",
            "",
            "The first run for a case is only cold-ish. This script does not restart PostgreSQL or clear the OS page cache. "
            "Fairness comes from shuffling case order, rotating variant order, and storing first and repeat runs separately.",
            "",
        ]
    )
    (run_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def serializable_config(args: argparse.Namespace) -> dict[str, Any]:
    config = vars(args).copy()
    config["active_bucket_pairs"] = [
        {"party_count": party_count, "service_count": service_count}
        for party_count, service_count in active_bucket_pairs(args)
    ]
    for key, value in list(config.items()):
        if isinstance(value, Path):
            config[key] = str(value)
    return config


def write_config(run_dir: Path, args: argparse.Namespace, variants: list[Variant]) -> None:
    config = serializable_config(args)
    config["command_line"] = command_line(args)
    config["variants"] = [{"name": variant.name, "path": str(variant.path)} for variant in variants]
    (run_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")


def create_run_dir(output_dir: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / timestamp
    suffix = 1
    while run_dir.exists():
        suffix += 1
        run_dir = output_dir / f"{timestamp}-{suffix}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark multi-party PostgreSQL Dialog permission query shapes with EXPLAIN ANALYZE."
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--dbname")
    parser.add_argument("--user")
    parser.add_argument("--queries-dir", default="queries")
    parser.add_argument("--queries-glob", default="*.sql", help="glob within --queries-dir selecting query templates")
    parser.add_argument("--output-dir", default="runs")
    parser.add_argument("--party-limit", type=int, default=5000)
    parser.add_argument(
        "--cases",
        type=int,
        default=20,
        help="target number of benchmark cases",
    )
    parser.add_argument("--passes", type=int, default=3)
    parser.add_argument("--runs-per-variant", type=int, default=2)
    parser.add_argument("--party-buckets", type=parse_int_list, default=parse_int_list("1,5,20,100,500"))
    parser.add_argument("--service-buckets", type=parse_int_list, default=parse_int_list("1,5,20,100,500"))
    parser.add_argument(
        "--buckets",
        type=parse_bucket_pairs,
        help='explicit party/service bucket pairs, e.g. "1;20,5;50,100;20"',
    )
    parser.add_argument("--permission-groups", type=int, default=1)
    parser.add_argument(
        "--candidate-source",
        choices=[
            "pg-stats-mcv",
            "sampled-dialog-distribution",
            "mcv-plus-sampled-dialog-distribution",
        ],
        default="pg-stats-mcv",
        help="source used to discover party candidates",
    )
    parser.add_argument("--candidate-strategy", choices=["stratified", "top"], default="stratified")
    parser.add_argument(
        "--sampled-candidate-table",
        default="benchmark_dialog_party_sample",
        help="unlogged table populated from Dialog TABLESAMPLE for sampled candidate sources",
    )
    parser.add_argument(
        "--sampled-candidate-percent",
        type=float,
        default=0.1,
        help="TABLESAMPLE SYSTEM percentage printed in the prerequisite population query",
    )
    parser.add_argument(
        "--sampled-candidate-seed",
        type=int,
        default=12345,
        help="TABLESAMPLE REPEATABLE seed printed in the prerequisite population query",
    )
    parser.add_argument("--as-of", default=utc_now_iso())
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--statement-timeout", default="0")
    parser.add_argument("--lock-timeout", default="5s")
    parser.add_argument("--shuffle-seed", type=int, default=random.SystemRandom().randint(1, 2**31 - 1))
    parser.add_argument(
        "--analyze-dialog-party-stats",
        action="store_true",
        help='explicitly allow ANALYZE "Dialog" ("Party") when pg_stats MCV data is missing',
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-going", action="store_true")
    parser.add_argument(
        "--offline-self-test",
        action="store_true",
        help="validate template rendering and report parsing without connecting to PostgreSQL",
    )
    args = parser.parse_args(argv)
    args.original_argv = list(argv)

    if not args.offline_self_test:
        missing = [
            option
            for option in ["host", "port", "dbname", "user"]
            if getattr(args, option) in (None, "")
        ]
        if missing:
            parser.error("missing required connection arguments: " + ", ".join("--" + value for value in missing))

    if args.party_limit <= 0:
        parser.error("--party-limit must be positive")
    if args.cases <= 0:
        parser.error("--cases must be positive")
    if args.passes <= 0:
        parser.error("--passes must be positive")
    if args.runs_per_variant <= 0:
        parser.error("--runs-per-variant must be positive")
    if args.permission_groups != 1:
        parser.error("only --permission-groups 1 is supported by this implementation")
    if args.sampled_candidate_percent <= 0 or args.sampled_candidate_percent > 100:
        parser.error("--sampled-candidate-percent must be > 0 and <= 100")
    try:
        quote_qualified_identifier(args.sampled_candidate_table)
    except ValueError as exc:
        parser.error(str(exc))

    args.queries_dir = str((ROOT / args.queries_dir).resolve()) if not Path(args.queries_dir).is_absolute() else args.queries_dir
    args.output_dir = str((ROOT / args.output_dir).resolve()) if not Path(args.output_dir).is_absolute() else args.output_dir
    return args


def run_offline_self_test(args: argparse.Namespace) -> int:
    variants = load_variants(Path(args.queries_dir), args.queries_glob)
    if parse_bucket_pairs("1;20,5;50,100;20,5;50") != [(1, 20), (5, 50), (100, 20)]:
        raise RuntimeError("offline self-test failed bucket pair parsing")
    parties = (
        "urn:altinn:organization:identifier-no:911612860",
        "urn:altinn:organization:identifier-no:312240505",
    )
    services = (
        "urn:altinn:resource:resource-a",
        "urn:altinn:resource:resource-b",
    )
    case = BenchmarkCase(
        case_id="case_0001_p_2_s_2",
        hot_party=parties[0],
        unprefixed_party_identifier="911612860",
        estimated_dialog_count=12345,
        estimated_dialog_count_band="hot",
        permission_group_count=1,
        party_count=2,
        service_count=2,
        parties=parties,
        services=services,
        payload_json=build_payload(parties, services),
    )
    rendered = [render_query(variant, case, args.as_of) for variant in variants]
    for query in rendered:
        if "{{" in query or "}}" in query:
            raise RuntimeError("offline self-test rendered unresolved placeholders")
        if not query.startswith("EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)"):
            raise RuntimeError("offline self-test rendered query without expected EXPLAIN prefix")
        if "jsonb_to_recordset" not in query:
            raise RuntimeError("offline self-test rendered query without JSON recordset expansion")
        if case.payload_json not in query:
            raise RuntimeError("offline self-test rendered query without JSON payload")

    fake_explain = [
        {
            "Plan": {
                "Node Type": "Limit",
                "Actual Rows": 101,
                "Actual Loops": 1,
                "Shared Hit Blocks": 1000,
                "Shared Read Blocks": 25,
                "Shared Dirtied Blocks": 0,
                "Shared Written Blocks": 0,
                "Local Hit Blocks": 0,
                "Local Read Blocks": 0,
                "Local Dirtied Blocks": 0,
                "Local Written Blocks": 0,
                "Temp Read Blocks": 0,
                "Temp Written Blocks": 0,
                "I/O Read Time": 12.5,
                "I/O Write Time": 0.0,
                "Plans": [
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "Dialog",
                        "Index Name": "IX_Dialog_Party_ServiceResource_ContentUpdatedAt",
                        "Rows Removed by Filter": 3,
                    }
                ],
            },
            "Planning Time": 1.25,
            "Execution Time": 42.5,
        }
    ]

    with tempfile.TemporaryDirectory(prefix="multi-party-sql-benchmark-") as tmp:
        run_dir = Path(tmp)
        manifest_rows = []
        for index, variant in enumerate(variants, start=1):
            raw_path = run_dir / "raw" / case.case_id / "pass_001" / "run_1" / variant.name / "explain.json"
            sql_path = raw_path.with_name("query.sql")
            case_payload_path = payload_path(run_dir, case)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            case_payload_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(json.dumps(fake_explain), encoding="utf-8")
            sql_path.write_text(render_query(variant, case, args.as_of), encoding="utf-8")
            case_payload_path.write_text(case.payload_json + "\n", encoding="utf-8")
            manifest_rows.append(
                {
                    "run_id": f"self_test_{variant.name}",
                    "pass": 1,
                    "case_id": case.case_id,
                    "run_index": 1,
                    "variant": variant.name,
                    "variant_order": index,
                    "permission_group_count": case.permission_group_count,
                    "party_count": case.party_count,
                    "service_count": case.service_count,
                    "hot_party": case.hot_party,
                    "unprefixed_party_identifier": case.unprefixed_party_identifier,
                    "estimated_dialog_count": case.estimated_dialog_count,
                    "estimated_dialog_count_band": case.estimated_dialog_count_band,
                    "as_of": args.as_of,
                    "sql_file": relpath(sql_path, run_dir),
                    "raw_output_file": relpath(raw_path, run_dir),
                    "stderr_file": "",
                    "payload_file": relpath(case_payload_path, run_dir),
                    "started_at": utc_now_iso(),
                    "finished_at": utc_now_iso(),
                    "exit_code": 0,
                }
            )
        metadata = {
            "version": "offline-self-test",
            "current_database": "offline",
            "dialog_table": {"estimated_rows": "250000000", "total_relation_size_pretty": "offline"},
            "settings": {"track_io_timing": "on"},
        }
        candidate = Candidate(
            party=case.hot_party,
            unprefixed_party_identifier=case.unprefixed_party_identifier,
            partyresource_short_prefix="o",
            estimated_dialog_count=case.estimated_dialog_count,
            service_resources=case.services,
            band=case.estimated_dialog_count_band,
            selected=True,
            skip_reason="",
        )
        write_reports(run_dir, args, variants, metadata, [candidate], [case], manifest_rows)
        if not (run_dir / "report.md").exists():
            raise RuntimeError("offline self-test did not create report.md")

    print(f"Offline self-test passed for {len(variants)} variant(s).", file=sys.stderr)
    return 0


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.offline_self_test:
        return run_offline_self_test(args)

    queries_dir = Path(args.queries_dir)
    output_dir = Path(args.output_dir)
    run_dir = create_run_dir(output_dir)

    variants = load_variants(queries_dir, args.queries_glob)
    write_config(run_dir, args, variants)

    print(f"Writing benchmark run to {run_dir}", file=sys.stderr)
    print(f"Command line: {command_line(args)}", file=sys.stderr)
    print("Checking database metadata and selecting candidates...", file=sys.stderr)
    metadata = capture_metadata(args)
    all_candidates = build_candidates(args)
    write_candidates_csv(run_dir / "candidates.csv", all_candidates)
    selected_candidates = select_candidates(args, all_candidates)
    cases = build_cases(args, selected_candidates)
    write_cases_csv(run_dir / "cases.csv", cases)
    if not cases:
        max_resources = max((len(candidate.service_resources) for candidate in all_candidates), default=0)
        eligible = sum(1 for candidate in all_candidates if candidate.selected)
        min_service_bucket = min(service_bucket for _, service_bucket in active_bucket_pairs(args))
        raise RuntimeError(
            "no benchmark cases were generated. "
            f"Candidates from {args.candidate_source}: {len(all_candidates)}; "
            f"candidates with at least {min_service_bucket} service resources: {eligible}; "
            f"maximum matched service resources for any candidate: {max_resources}. "
            "Lower --party-buckets/--service-buckets, increase --party-limit, or verify partyresource mappings."
        )

    manifest_handle, manifest_writer = open_manifest(run_dir / "manifest.csv")
    try:
        print(
            f"Running {len(cases)} cases, {len(variants)} variants, "
            f"{args.passes} passes, {args.runs_per_variant} run(s) per variant...",
            file=sys.stderr,
        )
        manifest_rows = execute_benchmark(
            args, run_dir, variants, cases, manifest_handle, manifest_writer
        )
    finally:
        manifest_handle.close()

    print("Generating reports...", file=sys.stderr)
    write_reports(run_dir, args, variants, metadata, all_candidates, cases, manifest_rows)
    print(f"Done. Report: {run_dir / 'report.md'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise SystemExit(130)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
