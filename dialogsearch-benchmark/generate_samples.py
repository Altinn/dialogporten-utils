#!/usr/bin/env python3
import os
import sys
import subprocess

DEFAULT_TIMEOUT_SECONDS = 30
HOT_PARTY_SERVICE_CANDIDATE_MULTIPLIER = 5


def run_query(conn_str, sql, timeout_s=DEFAULT_TIMEOUT_SECONDS):
    """Executes a SQL command via psql and returns the stdout."""
    try:
        process = subprocess.Popen(
            ['psql', conn_str, '-t', '-A', '-q', '-c', sql],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate(timeout=timeout_s)
        if process.returncode != 0:
            if stderr:
                print(f"SQL Error: {stderr.strip()}", file=sys.stderr)
            return None
        return stdout.strip()
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        print(f"SQL Error: query timed out after {timeout_s} seconds.", file=sys.stderr)
        return None
    except FileNotFoundError:
        print("Error: 'psql' not found in PATH.", file=sys.stderr)
        sys.exit(1)

def main():
    # 1. Environment and Args Check
    conn_str = os.getenv('PG_CONNECTION_STRING')
    if not conn_str:
        print("Error: PG_CONNECTION_STRING is missing.", file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) < 3:
        print("Usage: ./generate_samples.py <type> <count> [query-timeout-seconds]", file=sys.stderr)
        print("Types: party, service, party-services", file=sys.stderr)
        sys.exit(1)

    sample_type = sys.argv[1].lower()
    try:
        target_count = int(sys.argv[2])
    except ValueError:
        print("Error: Count must be an integer.", file=sys.stderr)
        sys.exit(1)
    if target_count < 1:
        print("Error: Count must be >= 1.", file=sys.stderr)
        sys.exit(1)
    try:
        query_timeout_s = int(sys.argv[3]) if len(sys.argv) >= 4 else DEFAULT_TIMEOUT_SECONDS
    except ValueError:
        print("Error: Query timeout must be an integer.", file=sys.stderr)
        sys.exit(1)
    if query_timeout_s < 1:
        print("Error: Query timeout must be >= 1.", file=sys.stderr)
        sys.exit(1)

    if sample_type == "party-services":
        candidate_count = target_count * HOT_PARTY_SERVICE_CANDIDATE_MULTIPLIER
        query = f"""
WITH hot_parties AS (
    SELECT
        unnest(most_common_vals::text::text[]) AS "Party",
        (
            unnest(most_common_freqs::float8[])
            * (SELECT reltuples FROM pg_class WHERE relname = 'Dialog')
        )::bigint AS "EstimatedDialogCount"
    FROM pg_stats
    WHERE tablename = 'Dialog'
      AND attname = 'Party'
),
hot_party_candidates AS (
    SELECT "Party", "EstimatedDialogCount"
    FROM hot_parties
    ORDER BY "EstimatedDialogCount" DESC
    LIMIT {candidate_count}
),
parsed_hot_parties AS (
    SELECT
        "Party",
        "EstimatedDialogCount",
        CASE
            WHEN "Party" LIKE 'urn:altinn:person:identifier-no:%' THEN 'p'
            WHEN "Party" LIKE 'urn:altinn:organization:identifier-no:%' THEN 'o'
        END AS "ShortPrefix",
        CASE
            WHEN "Party" LIKE 'urn:altinn:person:identifier-no:%'
                THEN substring("Party" from length('urn:altinn:person:identifier-no:') + 1)
            WHEN "Party" LIKE 'urn:altinn:organization:identifier-no:%'
                THEN substring("Party" from length('urn:altinn:organization:identifier-no:') + 1)
        END AS "UnprefixedPartyIdentifier"
    FROM hot_party_candidates
)
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'Party', hp."Party",
            'EstimatedDialogCount', hp."EstimatedDialogCount",
            'Services', hp."Services"
        )
        ORDER BY hp."EstimatedDialogCount" DESC, hp."Party"
    ),
    '[]'::jsonb
)::text
FROM (
    SELECT
        hp."Party",
        hp."EstimatedDialogCount",
        jsonb_agg(
            'urn:altinn:resource:' || r."UnprefixedResourceIdentifier"
            ORDER BY r."UnprefixedResourceIdentifier"
        ) AS "Services"
    FROM parsed_hot_parties hp
    JOIN partyresource."Party" p
      ON p."ShortPrefix" = hp."ShortPrefix"
     AND p."UnprefixedPartyIdentifier" = hp."UnprefixedPartyIdentifier"
    JOIN partyresource."PartyResource" pr ON pr."PartyId" = p."Id"
    JOIN partyresource."Resource" r ON pr."ResourceId" = r."Id"
    WHERE hp."ShortPrefix" IS NOT NULL
      AND hp."UnprefixedPartyIdentifier" IS NOT NULL
    GROUP BY hp."Party", hp."EstimatedDialogCount"
    ORDER BY hp."EstimatedDialogCount" DESC
    LIMIT {target_count}
) hp
"""
        output = run_query(conn_str, query, timeout_s=query_timeout_s)
        if output:
            print(output)
        else:
            print("No party/service values found.", file=sys.stderr)
        return

    # 2. Map type to Column Name
    mapping = {
        "party": '"Party"',
        "service": '"ServiceResource"'
    }

    if sample_type not in mapping:
        print(f"Error: Unknown type '{sample_type}'. Use 'party', 'service', or 'party-services'.", file=sys.stderr)
        sys.exit(1)

    col_name = mapping[sample_type]

    # 3. Get Row Estimate for Table
    est_sql = "SELECT reltuples::bigint FROM pg_class WHERE relname = 'Dialog' LIMIT 1"
    res = run_query(conn_str, est_sql, timeout_s=query_timeout_s)
    
    # Default to 1B if estimate is missing or zero
    total_est = int(res) if res and res.isdigit() and int(res) > 0 else 1000000000

    # 4. Sampling Loop
    distinct_values = set()
    iteration = 1
    
    # We use a 50x multiplier to over-sample blocks, ensuring we find distinct values faster
    percent = (target_count / total_est) * 100 * 50
    percent = min(percent, 100.0)

    while len(distinct_values) < target_count and iteration <= 20:
        prev_count = len(distinct_values)
        query = (
            f'SELECT DISTINCT {col_name} FROM "Dialog" '
            f'TABLESAMPLE SYSTEM ({percent}) REPEATABLE ({iteration}) '
            f'LIMIT {target_count}'
        )
        
        output = run_query(conn_str, query, timeout_s=query_timeout_s)
        
        if output:
            for val in output.splitlines():
                if val.strip():
                    distinct_values.add(val.strip())

        if len(distinct_values) == prev_count:
            percent = min(percent * 2, 100.0)
        iteration += 1

    # 5. Output Clean Results
    results = list(distinct_values)[:target_count]
    if results:
        print("\n".join(results))
    else:
        print(f"No values found for {sample_type}.", file=sys.stderr)

if __name__ == "__main__":
    main()
