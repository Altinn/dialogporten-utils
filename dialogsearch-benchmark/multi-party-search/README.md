# Multi-Party Search Benchmark

This directory contains a small PostgreSQL benchmark for comparing the query
shapes in `queries/`:

- `party-driven.sql`
- `service-driven.sql`

The benchmark renders each query with the same JSON permission payload and
`as_of` timestamp, wraps it in `EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)`,
and stores both raw plans and summarized CSV/JSON/Markdown reports.

## Running The Benchmark

Typical usage:

```bash
./benchmark.py \
  --host HOST \
  --dbname dialogporten \
  --user USER \
  --passes 3 \
  --runs-per-variant 2
```

Useful options:

- `--queries-dir queries`: directory containing query templates.
- `--queries-glob "*.sql"`: glob within `--queries-dir` selecting which query
  templates to include for this run.
- `--output-dir runs`: directory where timestamped run folders are written.
- `--party-buckets 1,5,20,100,500`: party counts to test.
- `--service-buckets 1,5,20,100,500`: service counts to test.
- `--buckets "1;20,5;50,100;20"`: explicit party/service bucket pairs.
  When provided, this overrides the Cartesian product from `--party-buckets`
  and `--service-buckets`.
- `--cases N`: number of generated benchmark cases. Defaults to the number of
  active party/service bucket pairs.
- `--passes N`: number of full sweeps over all cases.
- `--runs-per-variant N`: immediate repeats per variant for each case/pass.
- `--candidate-source pg-stats-mcv`: candidate discovery source. Use
  `sampled-dialog-distribution` to read from a pre-populated sampled table, or
  `mcv-plus-sampled-dialog-distribution` to merge PostgreSQL MCV candidates with
  sampled-table candidates.
- `--candidate-strategy stratified`: select candidates across hotness bands.
- `--analyze-dialog-party-stats`: explicitly run `ANALYZE "Dialog" ("Party")`
  before selecting candidates.

For sampled candidate sources, first populate the unlogged distribution table:

```sql
DROP TABLE IF EXISTS "benchmark_dialog_party_sample";

CREATE UNLOGGED TABLE "benchmark_dialog_party_sample" AS
SELECT
    "Party"::text AS "Party",
    count(*)::bigint AS sample_count,
    greatest(1, round(count(*) / (0.1::numeric / 100.0))::bigint) AS estimated_count
FROM "Dialog" TABLESAMPLE SYSTEM (0.1) REPEATABLE (12345)
WHERE "Party" IS NOT NULL
GROUP BY "Party";

CREATE INDEX ON "benchmark_dialog_party_sample" (estimated_count DESC);
CREATE INDEX ON "benchmark_dialog_party_sample" ("Party");
ANALYZE "benchmark_dialog_party_sample";
```

The script exits with this query if `--candidate-source sampled-dialog-distribution`
or `--candidate-source mcv-plus-sampled-dialog-distribution` is used before the
table exists or when it is empty. Adjust `--sampled-candidate-table`,
`--sampled-candidate-percent`, and `--sampled-candidate-seed` if you want a
different table name or sampling rate.

Each run directory contains:

```text
runs/<run-id>/
  config.json       # benchmark configuration and environment
  candidates.csv    # selected candidate parties and available services
  cases.csv         # generated JSON payloads
  manifest.csv      # every planned/measured execution
  report.csv        # parsed EXPLAIN metrics per execution
  report.json
  report.md
  raw/              # rendered SQL, EXPLAIN JSON, stderr per execution
```

## Passes And Runs

`pass` is a full sweep over all benchmark cases.

`run_index` is the immediate repeat number inside one pass for the same case and
variant.

With 20 cases, 3 passes, 2 runs per variant, and 2 variants:

```text
20 cases * 3 passes * 2 runs * 2 variants = 240 measured executions
```

Pass 1 is often affected by cache warming. Passes 2 and 3 are usually more
stable when the goal is warm-cache comparison.

## Summarizing Buffer And I/O Use

Use `summarize-explain-io.sh` to aggregate top-level EXPLAIN metrics by variant.

```bash
./summarize-explain-io.sh --run-dir runs/yt01-20260428T174033Z
```

The script reports:

- `hit_blocks`: shared buffer blocks found in PostgreSQL cache.
- `read_blocks`: shared blocks PostgreSQL had to read into shared buffers.
- `total_blocks`: `hit_blocks + read_blocks`, a practical proxy for total shared
  buffer page access.
- `total_GiB`: `total_blocks * 8 KiB`, shown as GiB.
- `read_GiB`: physical shared reads only.
- `read_time`: physical read I/O time only.
- `planning_time`: summed PostgreSQL planning time.
- `execution_time`: summed PostgreSQL execution time.

`read_blocks` and `read_time` measure physical PostgreSQL reads, not total work.
For warm-cache passes, they can be zero even when the query still touches many
cached buffers. Use `total_blocks` when comparing buffer access regardless of
cache hit/read status.

### Common Examples

Compare only pass 2:

```bash
./summarize-explain-io.sh \
  --run-dir runs/yt01-20260428T174033Z \
  --pass 2
```

Compare pass 2 and pass 3 together:

```bash
./summarize-explain-io.sh \
  --run-dir runs/yt01-20260428T174033Z \
  --pass 2 \
  --pass 3
```

Compare one case:

```bash
./summarize-explain-io.sh \
  --run-dir runs/yt01-20260428T174033Z \
  --pass 2 \
  --case case_0019_p_500_s_100
```

Compare all single-service cases in pass 2:

```bash
./summarize-explain-io.sh \
  --run-dir runs/yt01-20260428T174033Z \
  --pass 2 \
  --case-glob 'case_*_s_1'
```

Compare all 500-party cases in pass 2:

```bash
./summarize-explain-io.sh \
  --run-dir runs/yt01-20260428T174033Z \
  --pass 2 \
  --case-glob 'case_*_p_500_*'
```

Compare the largest fan-out cases in warm-cache passes:

```bash
./summarize-explain-io.sh \
  --run-dir runs/yt01-20260428T174033Z \
  --pass 2 \
  --pass 3 \
  --case-glob 'case_*_p_500_s_100'
```

## Interpreting Results

Prefer paired comparisons: compare both variants for the same case, pass, and
run index. Aggregate totals are useful, but they can be dominated by the largest
payloads.

For cold-ish behavior, inspect pass 1 and `read_blocks`/`read_time`.

For stable warm-cache behavior, inspect pass 2 or later and compare:

- `execution_time`
- `total_blocks`
- per-case results in `report.csv`

Small differences in `total_blocks` or execution time should not be over-read.
The generated cases are sampled payloads, not a complete production workload
model.
