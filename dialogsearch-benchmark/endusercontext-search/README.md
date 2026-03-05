# End-User Context SQL Benchmark

This directory contains tooling to benchmark SQL variants against PostgreSQL using `EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)`.

The setup is designed to reduce variance from party-level data skew by:
- generating a fixed benchmark party set,
- running variants in a paired design (same party across variants per round),
- alternating/randomizing variant order per round.

## Files

- `sql/`:
  - SQL variants (`*.sql`) to benchmark.
  - Each SQL must contain the placeholder `$party`.
- `parties.txt`:
  - Large party pool (one party per line).
- `benchmark_parties.txt`:
  - Fixed sampled party set for benchmarking (generated).
- `build_party_benchmark_set.py`:
  - Generates `benchmark_parties.txt` from `parties.txt` using DB counts.
- `bench_sql.py`:
  - Runs SQL benchmarks and writes run artifacts/summary.
- `output/`:
  - Timestamped benchmark run outputs.

## Requirements

- `python3`
- `psql` on PATH
- PostgreSQL credentials through `.pgpass` (or equivalent libpq mechanism)
- Recommended: run generator with a DB user that has only `SELECT` on `"Dialog"`

No password argument is supported by scripts.

## 1) Generate Benchmark Party Set

Use this once (or occasionally) to produce a stable, representative party sample.

```bash
python3 build_party_benchmark_set.py \
  --host <host> \
  --port <port> \
  --dbname <db> \
  --user <user> \
  --parties-file parties.txt \
  --sample-size 2000 \
  --strategy stratified \
  --allocation balanced \
  --count-mode batched-selects \
  --batch-size 200 \
  --pause-ms 50 \
  --statement-timeout-ms 30000 \
  --seed 1337 \
  --output-parties-file benchmark_parties.txt \
  --output-prefix benchmark_parties
```

### What it does

Count modes:
- `batched-selects` (default): many smaller SELECTs, safer for prod and works with SELECT-only users.
- `temp-table`: faster in some environments, but uses temp table/COPY and is not SELECT-only.

- Loads all parties from `parties.txt`.
- By default runs many smaller batched `SELECT` queries to compute dialog counts using `Dialog` (works with a SELECT-only user).
- Assigns strata by count:
  - `0`
  - `1-10`
  - `11-100`
  - `101-1000`
  - `1001-10000`
  - `10001+`
- Samples parties (default: `stratified` + `balanced`).
- Writes:
  - `benchmark_parties.txt` (one party per line)
  - `benchmark_parties.csv` (sampled set with counts/strata)
  - `benchmark_parties.party_counts.csv` (full pool counts)
  - `benchmark_parties.strata_summary.csv`
  - `benchmark_parties.metadata.json`

## 2) Run Benchmark

`bench_sql.py` now defaults to `benchmark_parties.txt`.

```bash
python3 bench_sql.py \
  --host <host> \
  --port <port> \
  --dbname <db> \
  --user <user> \
  --runs 50 \
  --design paired \
  --order-mode alternate \
  --seed 1337
```

Override parties file if needed:

```bash
python3 bench_sql.py ... --parties-file some_other_parties.txt
```

### Benchmark design options

- `--design paired` (default, recommended):
  - One party selected per round, then all SQL variants run for that same party.
  - Greatly improves fairness across variants.
- `--design random`:
  - Independent random party per SQL run.
  - Higher variance.

- `--order-mode alternate` (default):
  - Alternates variant order each round (helps reduce order bias).
- `--order-mode random`:
  - Random order each round.

### Runtime progress output

While running, script prints:
- benchmark start summary,
- per-round party + variant order (paired mode),
- per-run progress with `%`, round/position, sql id, status, execution time, elapsed, ETA.

## Output Structure

Each run writes to `output/<timestamp>/`:

- `manifest.json`:
  - run config (db args, design/order mode, seed, files)
- `runs.jsonl`:
  - one record per SQL execution
- `runs/<sql_id>/run_XXXX.stdout.txt`
- `runs/<sql_id>/run_XXXX.stderr.txt`
- `runs/<sql_id>/run_XXXX.explain.json` (if parsing succeeded)
- `summary.json`
- `summary.csv`
- `summary.md`

The script prints:
- output folder path,
- markdown filename/path,
- ASCII summary table.

## Statistical Recommendations

For production benchmarking with skewed party distributions:

1. Generate a fixed party sample (`benchmark_parties.txt`) and reuse it across comparisons.
2. Use `--design paired`.
3. Use `--order-mode alternate` (or `random`).
4. Keep seed fixed for reproducibility when comparing query variants.
5. Compare variants across repeated full runs; inspect p50/p95/p99 and run-level outliers.

## SQL Requirements

- SQL files must be in `sql/*.sql`.
- Each file must contain `$party` placeholder.
- `$party` is SQL-escaped by the benchmark script before execution.

## Common Issues

- `psql` exits non-zero:
  - check `.pgpass` credentials and DB connectivity.
- Missing placeholder error:
  - ensure each SQL has `$party`.
- Too-small party file:
  - benchmark requires at least 2 distinct parties.
- Highly variable tails:
  - increase sampled party set and/or run count, keep paired design.

## Example End-to-End

```bash
# Build stable party sample from the large pool
python3 build_party_benchmark_set.py \
  --host prod-db \
  --port 5432 \
  --dbname appdb \
  --user bench \
  --sample-size 2000 \
  --strategy stratified \
  --allocation balanced \
  --count-mode batched-selects \
  --batch-size 200 \
  --pause-ms 50 \
  --statement-timeout-ms 30000 \
  --seed 1337

# Run benchmark for all SQL variants in sql/
python3 bench_sql.py \
  --host prod-db \
  --port 5432 \
  --dbname appdb \
  --user bench \
  --runs 50 \
  --design paired \
  --order-mode alternate \
  --seed 1337
```
