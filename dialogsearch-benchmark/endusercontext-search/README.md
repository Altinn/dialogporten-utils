# End-User Context SQL Benchmark

This directory contains tooling to benchmark SQL variants against PostgreSQL using `EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)`.

The setup is designed to reduce variance from party-level data skew by:
- generating a fixed benchmark party set,
- running variants in a paired design (same party across variants per round),
- alternating/randomizing variant order per round.

## Files

- `sql/`
  - SQL variants (`*.sql`) to benchmark.
  - Each SQL must contain the placeholder `$party`.
- `parties.txt`
  - Large party pool (one party per line).
- `benchmark_parties.txt`
  - Fixed sampled party set for benchmarking (generated).
- `build_party_benchmark_set.py`
  - Generates `benchmark_parties.txt` from `parties.txt`.
- `bench_sql.py`
  - Runs SQL benchmarks and writes run artifacts/summary.
- `output/`
  - Timestamped benchmark run outputs.

## Requirements

- `python3`
- `psql` on PATH
- PostgreSQL credentials through `.pgpass` (or equivalent libpq mechanism)
- Recommended: run generator with a DB user that has only `SELECT` on `"Dialog"`

No password argument is supported by scripts.

## 1) Generate Benchmark Party Set

Use this once (or occasionally) to produce a stable, representative party sample.

### Recommended safe command (prod)

```bash
python3 build_party_benchmark_set.py \
  --host <host> \
  --port <port> \
  --dbname <db> \
  --user <readonly_user> \
  --parties-file parties.txt \
  --sample-size 2000 \
  --strategy stratified \
  --allocation balanced \
  --count-mode batched-selects \
  --count-cap 10001 \
  --batch-size 50 \
  --min-batch-size 1 \
  --pause-ms 100 \
  --statement-timeout-ms 120000 \
  --resume \
  --state-file benchmark_parties.state.json \
  --checkpoint-interval 500 \
  --seed 1337 \
  --output-parties-file benchmark_parties.txt \
  --output-prefix benchmark_parties
```

### Count modes

- `batched-selects` (default)
  - Uses many smaller `SELECT` statements.
  - Works with SELECT-only users.
  - Supports timeout split retry and resume.
- `temp-table`
  - Uses temp table + COPY.
  - Can be faster in some environments.
  - Not SELECT-only.

### Timeout hardening (batched-selects)

- `--count-cap 10001`
  - Counts at most 10001 rows per party, which is enough for the `10001+` stratum.
  - Prevents hot parties from forcing very long scans.
- `--retry-on-timeout` (default on)
  - If a batch times out, script splits it into smaller batches.
- `--timeout-single-party-as-top-stratum` (default on)
  - If a single party still times out, it is assigned `count_cap`.
- `--resume` + `--state-file`
  - Resumes long runs after interruption/failure.

### Optional load reduction

- `--max-parties-to-count N`
  - Deterministically pre-sample candidate parties before counting.
  - Useful if full pool is very large.

### What the generator writes

- `benchmark_parties.txt` (one party per line)
- `benchmark_parties.csv` (sampled set with counts/strata)
- `benchmark_parties.party_counts.csv` (counted candidate pool)
- `benchmark_parties.strata_summary.csv`
- `benchmark_parties.metadata.json`
- `benchmark_parties.state.json` (when using resume/checkpoint)

## 2) Run Benchmark

`bench_sql.py` defaults to `benchmark_parties.txt`.

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

- `--design paired` (default, recommended)
  - One party selected per round, then all SQL variants run for that same party.
  - Greatly improves fairness across variants.
- `--design random`
  - Independent random party per SQL run.
  - Higher variance.

- `--order-mode alternate` (default)
  - Alternates variant order each round.
- `--order-mode random`
  - Random order each round.

### Runtime progress output

`bench_sql.py` prints:
- benchmark start summary,
- per-round party + variant order (paired mode),
- per-run progress with `%`, round/position, SQL id, status, execution time, elapsed, ETA.

## Output Structure

Each run writes to `output/<timestamp>/`:

- `manifest.json`
  - run config (db args, design/order mode, seed, files)
- `runs.jsonl`
  - one record per SQL execution
- `runs/<sql_id>/run_XXXX.stdout.txt`
- `runs/<sql_id>/run_XXXX.stderr.txt`
- `runs/<sql_id>/run_XXXX.explain.json` (if parsing succeeded)
- `summary.json`
- `summary.csv`
- `summary.md`

The script also prints an ASCII summary table.

## Statistical Recommendations

For production benchmarking with skewed party distributions:

1. Generate and reuse a fixed party sample (`benchmark_parties.txt`).
2. Use `--design paired` in `bench_sql.py`.
3. Use `--order-mode alternate` (or random).
4. Keep seed fixed when comparing query variants.
5. Compare p50/p95/p99 and inspect outlier explains.

## SQL Requirements

- SQL files must be in `sql/*.sql`.
- Each file must contain `$party` placeholder.
- `$party` is SQL-escaped by benchmark script before execution.

## Common Issues

- `psql` exits non-zero
  - Check `.pgpass`, DNS/connectivity, and SSL requirements.
- `statement timeout` during party generation
  - Lower `--batch-size`, increase `--statement-timeout-ms`, keep `--retry-on-timeout`, and enable `--resume`.
- Missing placeholder error
  - Ensure each SQL includes `$party`.
- Too-small party file
  - Benchmark requires at least 2 distinct parties.

## Example End-to-End

```bash
# 1) Build stable party sample
python3 build_party_benchmark_set.py \
  --host prod-db \
  --port 5432 \
  --dbname appdb \
  --user readonly_user \
  --sample-size 2000 \
  --strategy stratified \
  --allocation balanced \
  --count-mode batched-selects \
  --count-cap 10001 \
  --batch-size 50 \
  --pause-ms 100 \
  --statement-timeout-ms 120000 \
  --resume \
  --seed 1337

# 2) Run SQL benchmark variants
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
