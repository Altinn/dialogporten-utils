# PostgreSQL Single-Party Query Benchmark Plan

## Goal

Build a repeatable benchmark harness for comparing different SQL shapes that are semantically equivalent:

- Filter a single `Dialog."Party"`.
- Filter multiple `Dialog."ServiceResource"` values.
- Apply the same status, visibility, expiry, and deletion predicates.
- Fetch the same ordered page of at most 101 dialogs.
- Store every `EXPLAIN (ANALYZE, BUFFERS, TIMING)` result.
- Run enough first-run and repeat-run samples to show cold-ish versus warm behavior.
- Produce a report comparing runtime, buffer usage, disk reads, index usage, row counts, and plan shape.

The benchmark must avoid comparing query variants on already-warmed data for one variant while another variant pays the first cache access cost.

## Proposed Files

```text
single-party-sql-benchmark/
  benchmark.py
  plan.md
  queries/
    q1_direct_page.sql
    q2_id_subquery_join.sql
  runs/
    <timestamp>/
      config.json
      candidates.csv
      manifest.csv
      raw/
        <case>/
          <pass>/
            <variant>/
              run_1.json
              run_2.json
              query.sql
      report.md
      report.csv
      report.json
```

`runs/` should be ignored by git if this repo later gains a `.gitignore`.

## Query Template Design

Each `queries/*.sql` file should contain the query body with a simple placeholder:

```sql
{{WHERE_CLAUSE}}
```

The benchmark runner should load every `*.sql` in lexical order from `queries/`, inject the generated where clause, and execute:

```sql
EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)
<injected query>
```

Using `FORMAT JSON` is important because the report needs robust extraction of:

- planning time
- execution time
- shared hit/read/dirtied/written blocks
- local and temp blocks
- read/write IO timing, if `track_io_timing` is enabled
- node types
- relation and index names
- actual rows and loops
- rows removed by filters
- sort method and memory

The raw JSON output should be stored exactly as returned by PostgreSQL. The injected SQL should also be stored beside it for auditability.

The template files should not include `EXPLAIN`; the runner should add it consistently and fail if a template already starts with `EXPLAIN`, to prevent accidental double wrapping or mixed options.

## Query Variants

### `queries/q1_direct_page.sql`

This is the direct query shape. It should use `now()` or a runner-injected fixed timestamp consistently. Prefer replacing `NOW()` with `{{AS_OF}}::timestamptz` for fair comparison with query 2.

```sql
SELECT d."Id", d."ContentUpdatedAt", d."CreatedAt", d."Deleted", d."DeletedAt", d."DueAt", d."ExpiresAt", d."ExtendedStatus", d."ExternalReference", d."FromPartyTransmissionsCount", d."FromServiceOwnerTransmissionsCount", d."Frozen", d."HasUnopenedContent", d."IdempotentKey", d."IsApiOnly", d."IsSeenSinceLastContentUpdate", d."Org", d."Party", d."PrecedingProcess", d."Process", d."Progress", d."Revision", d."ServiceResource", d."ServiceResourceType", d."StatusId", d."SystemLabelsMask", d."UpdatedAt", d."VisibleFrom"
FROM (
    SELECT d.*
    FROM "Dialog" d
    {{WHERE_CLAUSE}}
    AND d."StatusId" = ANY(ARRAY[7, 2, 8]::int[])
    AND (d."VisibleFrom" IS NULL OR d."VisibleFrom" <= {{AS_OF}}::timestamptz)
    AND (d."ExpiresAt" IS NULL OR d."ExpiresAt" > {{AS_OF}}::timestamptz)
    AND d."Deleted" = false::boolean
    ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
    LIMIT 101
) AS d
ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
LIMIT 101
```

### `queries/q2_id_subquery_join.sql`

This is the ID-subquery then join-back shape.

```sql
SELECT d."Id", d."ContentUpdatedAt", d."CreatedAt", d."Deleted", d."DeletedAt", d."DueAt", d."ExpiresAt", d."ExtendedStatus", d."ExternalReference", d."FromPartyTransmissionsCount", d."FromServiceOwnerTransmissionsCount", d."Frozen", d."HasUnopenedContent", d."IdempotentKey", d."IsApiOnly", d."IsSeenSinceLastContentUpdate", d."Org", d."Party", d."PrecedingProcess", d."Process", d."Progress", d."Revision", d."ServiceResource", d."ServiceResourceType", d."StatusId", d."SystemLabelsMask", d."UpdatedAt", d."VisibleFrom"
FROM (
    SELECT d.*
    FROM "Dialog" d
    JOIN (
        SELECT d."Id"
        FROM "Dialog" d
        {{WHERE_CLAUSE}}
        AND d."StatusId" = ANY(ARRAY[7, 2, 8]::int[])
        AND (d."VisibleFrom" IS NULL OR d."VisibleFrom" <= {{AS_OF}}::timestamptz)
        AND (d."ExpiresAt" IS NULL OR d."ExpiresAt" > {{AS_OF}}::timestamptz)
        AND d."Deleted" = false::boolean
        ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
        LIMIT 101
    ) AS sub ON d."Id" = sub."Id"
    ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
    LIMIT 101
) AS d
ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
LIMIT 101
```

## CLI

Required arguments:

```text
--host
--port
--dbname
--user
```

Useful optional arguments:

```text
--queries-dir queries
--output-dir runs
--party-limit 5000
--cases 30
--passes 3
--runs-per-variant 2
--service-resource-buckets 10,25,50,100,200
--candidate-strategy stratified
--as-of 2026-04-23T22:37:24.5741080+00:00
--psql psql
--statement-timeout 0
--lock-timeout 5s
--shuffle-seed <integer>
--analyze-dialog-party-stats
--dry-run
--keep-going
--offline-self-test
```

Defaults should be conservative:

- `--passes 3`
- `--runs-per-variant 2`
- `--cases 20`
- fixed `--as-of` defaulting to current UTC at benchmark start, not per query
- `--statement-timeout 0`, because some cold plans may be slow and timing out would bias the report unless explicitly requested

## Connection Strategy

Use `psql`, not a Python PostgreSQL driver.

Run each measured query through `psql` with:

```text
psql
  --host <host>
  --port <port>
  --dbname <dbname>
  --username <user>
  --no-psqlrc
  --set ON_ERROR_STOP=1
  --tuples-only
  --no-align
```

Feed SQL through stdin so generated SQL does not leak through process arguments. Rely on `.pgpass` for authentication.

Each query execution should start a fresh `psql` process. This avoids client-side/session effects and keeps the measured unit simple. Server-side shared buffers and OS cache will still behave naturally.

Statement and lock timeouts should be applied through `PGOPTIONS`, not by prepending `SET` statements to the SQL sent through stdin. This keeps `psql` stdout clean for CSV and JSON parsing.

Before measured runs, execute metadata and candidate-selection queries separately and store their results.

## Candidate Party Selection

Candidate discovery must use `pg_stats`; the runner must not scan or group the `Dialog` table to find parties. First verify that `pg_stats` contains usable `most_common_vals` and `most_common_freqs` data for `Dialog."Party"`.

If the stats row or MCV arrays are missing, do not use a fallback query against `Dialog`. In an interactive TTY, ask whether to run `ANALYZE "Dialog" ("Party")`; in non-interactive mode, stop with a clear error unless the user explicitly supplies `--analyze-dialog-party-stats`.

Then fetch the top candidate parties using the provided stats-based query, adjusted to only return the party identifier and estimated count:

```sql
SELECT *
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
LIMIT :party_limit;
```

Then classify candidates into estimated row-count bands. The default `stratified` strategy should pick parties from several bands instead of only the largest parties:

- very hot: highest 1 percent of candidates
- hot: 1-5 percent
- medium: 5-25 percent
- long tail: remaining selected candidates that still have enough service resources

For each candidate party, derive:

- full party URN
- unprefixed party identifier
- estimated dialog row count
- service resource count
- generated where clause

The where-clause query should be parameterized by unprefixed party identifier. The script should not construct SQL by string-concatenating untrusted CLI input into that lookup query. Since `psql` is used, pass values using psql variables and `:'var'` quoting.

Store all selected candidates in `candidates.csv`.

## Service Resource Bucket Handling

The generated where clause may include all resources for a party. To compare input sizes, the runner should support buckets such as `10,25,50,100,200`.

Recommended approach:

1. Query the party's resource URNs as rows, ordered by `UnprefixedResourceIdentifier`.
2. Join `partyresource."Party"` using both `UnprefixedPartyIdentifier` and `ShortPrefix`, deriving the short prefix from the full `Dialog."Party"` URN.
3. For each bucket size, take the first N resources from that ordered list.
4. Generate the injected `WHERE` clause from Python:

```sql
WHERE d."Party" = '<full party urn>'
    AND d."ServiceResource" = ANY(ARRAY[
        '<resource urn 1>',
        '<resource urn 2>'
    ]::text[])
```

This is more flexible than using the string-producing SQL query as-is and makes the bucket size explicit in the manifest.

If a party has fewer resources than a requested bucket, skip that party/bucket combination and record the skip reason.

## Fairness And Cache Strategy

Perfect cold-cache measurement is not possible from an unprivileged script because PostgreSQL shared buffers and the OS page cache cannot safely be flushed without privileged operations and a server restart. The benchmark should instead be explicit and fair:

### Within A Case

A case is:

```text
party + service_resource_bucket + as_of timestamp
```

For each case, run all variants in a randomized order for run 1, then all variants in another randomized order for run 2.

Example with variants A and B:

```text
case 001, pass 1:
  run_index 1: B, A
  run_index 2: A, B
```

This means:

- every variant gets sampled as a first touch for some cases
- every variant gets sampled after the same party/resource set has been warmed
- repeated runs are stored separately, not averaged away

### Across Passes

Use multiple passes over the selected cases. Shuffle case order per pass using the configured seed:

```text
pass 1: case order shuffled with seed + 1
pass 2: case order shuffled with seed + 2
pass 3: case order shuffled with seed + 3
```

Rotating variant order is more important than attempting global cold-cache purity. On a 250M-row table, OS cache state will depend on recent production traffic, autovacuum, prior benchmark cases, and unrelated queries.

### Cold-ish Versus Warm Labels

Use careful terminology in the report:

- `first_run_for_case`: first measured run for that party/resource bucket in that pass
- `second_run_for_case`: immediate repeat run for the same case

Do not claim the first run is truly cold unless the user explicitly runs the benchmark immediately after a controlled PostgreSQL restart and OS cache drop.

### Optional Manual Cold Mode

The script may print an optional note before pass 1:

```text
For stronger cold-cache measurements, restart PostgreSQL and clear OS page cache before starting this script.
```

The script itself should not attempt to restart PostgreSQL or clear OS cache.

## Run Manifest

Create `manifest.csv` with one row per measured execution:

```text
run_id
pass
case_id
run_index
variant
variant_order
party
unprefixed_party_identifier
estimated_dialog_count
service_resource_count
as_of
sql_file
raw_output_file
rendered_sql_file
started_at
finished_at
exit_code
```

The manifest is the source of truth for reproducing and auditing the report.

## Execution Flow

1. Parse CLI arguments.
2. Create timestamped output directory.
3. Discover and validate `queries/*.sql`.
4. Check database connectivity with `SELECT version();`.
5. Capture environment metadata:
   - PostgreSQL version
   - selected relevant settings:
     - `shared_buffers`
     - `effective_cache_size`
     - `work_mem`
     - `random_page_cost`
     - `seq_page_cost`
     - `track_io_timing`
     - `jit`
     - `max_parallel_workers_per_gather`
   - table size for `Dialog`
   - relevant index definitions for `Dialog`
6. Fetch candidate parties from `pg_stats`.
7. Fetch service resources for candidates.
8. Build benchmark cases by row-count band and resource bucket.
9. Write `config.json`, `candidates.csv`, and initial `manifest.csv`.
10. For each pass:
    - shuffle case order
    - for each case:
      - render each SQL variant
      - execute run 1 for all variants in randomized/rotated order
      - execute run 2 for all variants in randomized/rotated order
      - store raw JSON and rendered SQL
      - append manifest rows immediately
11. Parse all raw JSON outputs.
12. Generate `report.csv`, `report.json`, and `report.md`.

## Report Metrics

For each run:

- planning time
- execution time
- total time if available
- top-level actual rows
- shared hit blocks
- shared read blocks
- shared dirtied blocks
- shared written blocks
- local hit/read/dirtied/written blocks
- temp read/written blocks
- IO read/write time when present
- main scan node types
- index names used
- number of index scans, bitmap scans, seq scans, nested loops, sorts
- rows removed by filter
- sort method and sort memory
- whether JIT was used and JIT timing if present

For summaries, group by:

```text
variant
service_resource_count
estimated_dialog_count_band
run_index
```

Show:

- count of samples
- median execution time
- p90 execution time
- min/max execution time
- median shared reads
- median shared hits
- median hit ratio:

```text
shared_hit_blocks / nullif(shared_hit_blocks + shared_read_blocks, 0)
```

- median temp blocks
- most common index names
- most common top-level plan node patterns

The Markdown report should include:

1. Benchmark configuration.
2. Environment metadata.
3. Candidate distribution.
4. Summary by variant and run index.
5. Summary by service-resource bucket.
6. Summary by estimated row-count band.
7. Per-case winner table.
8. Notes and caveats about cache state.

## Parsing EXPLAIN JSON

The parser should recursively walk `Plan` nodes. It should collect:

- node type counts
- relation names
- index names
- actual rows
- loops
- buffer counters
- IO timings
- rows removed
- sort details

Important: PostgreSQL reports buffer data at each node and at the top-level plan. For total buffer comparison, use the top-level plan block counters, not the sum of all child nodes, because child sums can double-count depending on interpretation. Use recursive node data for qualitative plan-shape reporting.

## Error Handling

Default behavior should stop on first failed query because partial benchmark data can be misleading.

With `--keep-going`, failed runs should be recorded with:

- exit code
- stderr
- rendered SQL path
- skip/failure reason

Failed runs must be excluded from aggregate timing comparisons unless the report has a separate failure section.

## Validation

Before executing the full benchmark:

- `--dry-run` should render all selected SQL files and write a preview manifest without running `EXPLAIN ANALYZE`.
- Validate every rendered query contains exactly one `{{WHERE_CLAUSE}}` replacement point before replacement.
- Validate no unresolved `{{...}}` placeholders remain.
- Validate each case has enough service resources for its bucket.
- Validate all variants return a top-level JSON explain result.

After implementation, test locally with:

```text
python3 benchmark.py --help
python3 benchmark.py --offline-self-test
python3 benchmark.py ... --cases 1 --passes 1 --runs-per-variant 1 --service-resource-buckets 10 --dry-run
python3 benchmark.py ... --cases 1 --passes 1 --runs-per-variant 2 --service-resource-buckets 10
```

## Implementation Notes

- Use only Python standard library if practical:
  - `argparse`
  - `csv`
  - `json`
  - `pathlib`
  - `random`
  - `statistics`
  - `subprocess`
  - `datetime`
  - `tempfile`
- Use `subprocess.run()` with stdin for SQL execution.
- Do not use shell interpolation for generated SQL.
- Keep generated SQL deterministic for a given seed and candidate set.
- Quote SQL string literals with a small helper that doubles single quotes.
- Make every output file path relative to the timestamped run directory in reports.
- Flush manifest writes after every run so interrupted benchmarks still leave usable partial data.

## Resolved Implementation Decisions

1. Benchmark the full plan: run every requested service-resource bucket for every selected party that has enough resources.
2. `Party` values from `pg_stats.most_common_vals` match the full URN format used in `Dialog."Party"`.
3. `Dialog."ServiceResource"` is `text`; generated service-resource arrays should use `::text[]`.
4. `track_io_timing` is enabled on the target database, so the report should include IO timing metrics when PostgreSQL returns them.
5. There is no expected concurrent production traffic pattern that needs special annotation in the report.
