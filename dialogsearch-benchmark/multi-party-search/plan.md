# PostgreSQL Multi-Party Permissions Query Benchmark Plan

## Goal

Build an independent benchmark harness in this directory for comparing the two multi-party query shapes in `queries/`:

- `party-driven.sql`
- `service-driven.sql`

The benchmark input is not a generated SQL `WHERE` clause. Each rendered query receives one JSON payload shaped like:

```json
[
  {
    "Parties": [
      "urn:altinn:organization:identifier-no:312240505"
    ],
    "Services": [
      "urn:altinn:resource:app_digdir_bli-tjenesteeier"
    ]
  }
]
```

The runner should produce the same class of output as `../single-party-search`: execution timing, buffer and I/O metrics, raw `EXPLAIN` JSON, rendered SQL, manifests, and summarized reports.

Important: `../single-party-search` is only a reference. Do not modify files there. Copy the useful implementation patterns into this directory and make `multi-party-search` self-contained.

## Proposed Files

```text
multi-party-search/
  benchmark.py
  plan.md
  queries/
    party-driven.sql
    service-driven.sql
  runs/
    <timestamp>/
      config.json
      candidates.csv
      cases.csv
      manifest.csv
      raw/
        <case>/
          <pass>/
            run_<n>/
              <variant>/
                query.sql
                explain.json
                stderr.txt
      report.csv
      report.json
      report.md
```

Add a local `.gitignore` if needed:

```text
runs/
```

## Independence From Single-Party Search

Create a new `benchmark.py` in this directory by copying and adapting the relevant pieces from `../single-party-search/benchmark.py`:

- CLI parsing
- `psql` execution through stdin
- run directory creation
- variant discovery and validation
- metadata capture
- candidate discovery from `pg_stats`
- stratified candidate selection
- manifest writing
- raw explain storage
- explain JSON parsing
- CSV, JSON, and Markdown reporting
- offline self-test structure

After copying, all paths should resolve relative to `multi-party-search/`:

- default `--queries-dir queries`
- default `--output-dir runs`
- local `ROOT = Path(__file__).resolve().parent`

The script should not import from `../single-party-search`, shell out to it, or depend on its `queries/`, `runs/`, or `.gitignore`.

## Query Template Design

Each query file should remain a plain SQL body without `EXPLAIN`. The runner adds:

```sql
EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)
```

Templates should use these placeholders:

```text
{{PARTIES_AND_SERVICES_JSON}}
{{AS_OF}}
```

`{{PARTIES_AND_SERVICES_JSON}}` is replaced with one SQL string literal containing the JSON payload. The template should cast it to `jsonb`, as the current queries already do.

`{{AS_OF}}` should replace all use of `NOW()` or hard-coded timestamps, so both variants run against the same visibility and expiry instant. `party-driven.sql` currently uses `NOW()`, and `service-driven.sql` currently has a hard-coded timestamp. Normalize both to:

```sql
{{AS_OF}}::timestamptz
```

Template validation should fail if:

- the SQL starts with `EXPLAIN`
- `{{PARTIES_AND_SERVICES_JSON}}` is missing or appears more than once
- `{{AS_OF}}` is missing
- any `{{...}}` placeholders remain after rendering

## Benchmark Case Model

A case should represent a realistic permissions payload:

```text
case_id
permission_group_count
parties_per_group
services_per_group
total_party_references
total_service_references
estimated_dialog_count_band
hot_party_count
parties
services
json_payload
```

The initial implementation can use one permission group per case by default:

```json
[
  {
    "Parties": ["party 1", "party 2", "..."],
    "Services": ["service 1", "service 2", "..."]
  }
]
```

Keep the data model ready for multiple groups, because the production input format allows multiple permission groups and overlapping party/service permissions.

## Candidate Selection

Use the same hot-party strategy as the single-party benchmark:

1. Verify usable `pg_stats` MCV data for `Dialog."Party"`.
2. Refuse to scan or group the full `Dialog` table to discover parties.
3. Optionally run `ANALYZE "Dialog" ("Party")` only when explicitly requested.
4. Fetch top candidate parties from `pg_stats.most_common_vals` and `most_common_freqs`.
5. Classify candidates into bands:
   - `very_hot`: top 1 percent
   - `hot`: 1-5 percent
   - `medium`: 5-25 percent
   - `long_tail`: remaining fetched candidates
6. Select cases with the default `stratified` strategy so the run is not only the top few parties.

For each candidate party, derive:

- full party URN
- unprefixed identifier
- `partyresource."Party"."ShortPrefix"` lookup key (`o` for organization, `p` for person)
- estimated dialog count
- dialog-count band
- service resources available through `partyresource`

Unsupported party URN formats should be written to `candidates.csv` with a skip reason instead of failing the whole benchmark.

## Service Resource Lookup

For selected parties, fetch their allowed services from `partyresource` once per benchmark setup:

```sql
WITH wanted(unprefixed_party_identifier, short_prefix) AS (
    VALUES ...
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
```

This mirrors the single-party benchmark, but the selected resources are used to build JSON arrays instead of an injected `ANY(ARRAY[...]::text[])` predicate.

## Case Generation

Expose separate bucket controls for parties and services:

```text
--party-buckets 1,5,20,100,500
--service-buckets 1,5,20,100,500
--permission-groups 1
--cases 20
--candidate-strategy stratified
```

For each selected hot-party seed and each bucket combination:

1. Build a party list starting with the hot seed party.
2. Fill the remaining party slots from other eligible candidates, preferring the same band first and then neighboring bands.
3. Build the service list as the union of services available to the selected parties.
4. If the union has at least the requested service bucket, take a deterministic sample of that size using the configured shuffle seed.
5. Emit one JSON permission group:

```json
[
  {
    "Parties": ["..."],
    "Services": ["..."]
  }
]
```

If there are not enough parties or services for a requested bucket, skip that case and record the reason.

Recommended default buckets:

```text
--party-buckets 1,5,20,100,500
--service-buckets 1,5,20,100,500
```

The case target should mean "generate at least this many runnable cases when possible", not "run every cartesian combination forever". Stop selecting more candidate seeds once the generated case count reaches `--cases`.

## JSON Rendering

Use Python's `json.dumps()` to build the payload. Then pass it into SQL via a normal escaped SQL string literal:

```python
def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
```

Rendering should perform:

```text
{{PARTIES_AND_SERVICES_JSON}} -> sql_literal(json_payload)
{{AS_OF}} -> sql_literal(as_of)
```

Store both the rendered SQL and the raw JSON payload for auditability. Either include the JSON payload in `cases.csv` if it stays reasonably small, or write it beside each case as:

```text
raw/<case>/payload.json
```

## CLI

Required connection arguments:

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
--cases 20
--passes 3
--runs-per-variant 2
--party-buckets 1,5,20,100,500
--service-buckets 1,5,20,100,500
--permission-groups 1
--candidate-strategy stratified
--as-of <UTC timestamp; default fixed at benchmark start>
--psql psql
--statement-timeout 0
--lock-timeout 5s
--shuffle-seed <integer>
--analyze-dialog-party-stats
--dry-run
--keep-going
--offline-self-test
```

Use `psql` rather than a Python database driver. Start a fresh `psql` process for each measured query, pass SQL through stdin, and apply statement and lock timeouts through `PGOPTIONS`.

## Fairness And Cache Strategy

Use the same fairness model as the single-party benchmark:

- multiple passes over the generated cases
- case order shuffled per pass
- variant order shuffled per case and run index
- first and repeat runs stored separately

Do not claim true cold-cache measurements. Label run indexes carefully:

- `run_index = 1`: first measured run for the same payload in that pass
- `run_index > 1`: immediate repeats for the same payload

The report should explain that the script does not restart PostgreSQL or clear OS cache.

## Run Manifest

`manifest.csv` should contain one row per measured execution:

```text
run_id
pass
case_id
run_index
variant
variant_order
permission_group_count
parties_per_group
services_per_group
total_party_references
total_service_references
hot_party
estimated_dialog_count
estimated_dialog_count_band
as_of
sql_file
raw_output_file
stderr_file
payload_file
started_at
finished_at
exit_code
```

Flush each manifest row immediately so interrupted runs still leave useful partial data.

## Report Metrics

For every successful `EXPLAIN` output, parse and report:

- planning time
- execution time
- top-level actual rows and loops
- shared hit/read/dirtied/written blocks
- local hit/read/dirtied/written blocks
- temp read/written blocks
- I/O read/write timing when available
- hit ratio
- node type counts
- index names
- relation names
- sort methods
- rows removed by filter or join filter
- JIT information when present

Summaries should group by:

```text
variant
run_index
party_count
service_count
estimated_dialog_count_band
```

The Markdown report should include:

1. Benchmark configuration.
2. Environment metadata.
3. Candidate distribution.
4. Case distribution by party bucket, service bucket, and hot-party band.
5. Summary by variant and run index.
6. Summary by party bucket.
7. Summary by service bucket.
8. Summary by estimated dialog-count band.
9. Per-case winner table.
10. Cache and concurrency caveats.

## Execution Flow

1. Parse CLI arguments.
2. Create timestamped run directory under local `runs/`.
3. Discover and validate local `queries/*.sql`.
4. Capture database metadata:
   - PostgreSQL version
   - database name
   - `Dialog` estimated rows and size
   - relevant `Dialog` indexes
   - settings such as `shared_buffers`, `work_mem`, `track_io_timing`, and `jit`
5. Fetch hot-party candidates from `pg_stats`.
6. Fetch partyresource services for supported candidates.
7. Build stratified multi-party JSON cases.
8. Write `config.json`, `candidates.csv`, `cases.csv`, and initial `manifest.csv`.
9. For each pass:
   - shuffle case order
   - for each case and run index:
     - shuffle variants
     - render SQL with the JSON payload and fixed `as_of`
     - write rendered SQL and payload
     - run `EXPLAIN ANALYZE`
     - store stdout and stderr
     - append manifest
10. Parse raw explain JSON.
11. Generate `report.csv`, `report.json`, and `report.md`.

## Validation

Before running against PostgreSQL:

```text
python3 benchmark.py --help
python3 benchmark.py --offline-self-test
```

The offline self-test should:

- load the local query templates
- render a representative JSON payload
- verify no unresolved placeholders remain
- verify the rendered query starts with the expected `EXPLAIN` prefix
- parse a small fake `EXPLAIN (FORMAT JSON)` result
- generate report files in a temporary directory

Then test a live dry run:

```text
python3 benchmark.py \
  --host <host> \
  --port <port> \
  --dbname <dbname> \
  --user <user> \
  --cases 1 \
  --passes 1 \
  --runs-per-variant 1 \
  --party-buckets 1 \
  --service-buckets 5 \
  --dry-run
```

Finally run a tiny live benchmark:

```text
python3 benchmark.py \
  --host <host> \
  --port <port> \
  --dbname <dbname> \
  --user <user> \
  --cases 1 \
  --passes 1 \
  --runs-per-variant 2 \
  --party-buckets 1 \
  --service-buckets 5
```

## Implementation Notes

- Use only the Python standard library unless a real need appears.
- Keep generated SQL deterministic for a fixed seed and candidate set.
- Use structured JSON generation instead of manually concatenating JSON.
- Use SQL literal escaping only at the final SQL rendering boundary.
- Store rendered SQL and payloads for reproducibility.
- Exclude failed runs from aggregate timing comparisons, but list failures separately when `--keep-going` is used.
- Keep report path references relative to the timestamped run directory.
- Do not touch `../single-party-search` during implementation.
