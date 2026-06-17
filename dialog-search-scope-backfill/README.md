# dialog-search-scope-backfill

> Started from `maintenance-job-scaffold/_template`, then **adapted to a keyset-batched UPDATE**
> because this is a near-total backfill (essentially every row in `search."DialogSearch"`), for which
> the scaffold's candidate-table model is the wrong shape — it would copy the whole ~1.1B-row keyspace
> into a transient table inside one giant transaction. See the scaffold's
> [`README.md`](../maintenance-job-scaffold/README.md) for the shared safety rationale.

## Scope

One-shot backfill of the two columns added by the additive migration
`AddDialogSearchContentUpdatedAtAndServiceResource`:

- `search."DialogSearch"."ContentUpdatedAt"` <- `public."Dialog"."ContentUpdatedAt"`
- `search."DialogSearch"."ServiceResource"` <- `public."Dialog"."ServiceResource"`, with the static
  `urn:altinn:resource:` prefix stripped (matching `UpsertDialogSearchOne`/`RebuildDialogSearchOnce`
  and `VDialogDocument`).

New/updated rows are already populated by those functions; this job fills the pre-existing backlog so
the columns can be relied on (and indexed) before the application change that consumes them.

A row is in scope when `"ContentUpdatedAt" IS NULL OR "ServiceResource" IS NULL`. Expected on first run:
≈ the entire table (~1.1B in production).

This is a **silent repair**: only those two columns are written. `"UpdatedAt"` (the reindex/staleness
watermark), `"Revision"`, and the MassTransit outbox are deliberately not touched.

## How it works (and why this shape)

- **Keyset, not candidate table.** Each batch claims the next `BATCH_SIZE` DialogIds via
  `WHERE "DialogId" > cursor ORDER BY "DialogId" LIMIT n` (a primary-key range scan) and UPDATEs the
  unpopulated ones, guarded by `IS NULL`. No 50–80 GB transient table; the PK is walked once.
- **Per-batch commit.** Each batch is one autocommit `SELECT … FROM …_batch(…)`, so the snapshot is
  released every batch — the global vacuum horizon advances and WAL flushes instead of one multi-hour
  transaction holding everything back.
- **Resumable.** The cursor is persisted per worker in `…_Checkpoint`. Kill and restart with the same
  `WORKER_ID` to resume; the `IS NULL` guard makes re-processing a no-op.
- **HOT-friendly.** It writes neither the GIN columns (`Party`, `SearchVector`) nor the TOASTed vector,
  so updates should be HOT (no GIN churn). **Validate this in the smoke step** (below).

## Run order

```bash
export PG='postgresql://repair_user@db.example.com:5432/dialogporten?sslmode=require'  # password in ~/.pgpass
cd dialog-search-scope-backfill

# 1. EXPLAIN sanity check — confirm the keyset PK access path (NOT a Seq Scan).
psql "$PG" -X -v ON_ERROR_STOP=1 -f 03_explain_sanity_check.sql

# 2. (optional) remaining count — full scan, minutes at prod scale.
psql "$PG" -X -v ON_ERROR_STOP=1 -f 02_dry_run_count.sql

# 3. Create checkpoint table + batch function (idempotent; drops obsolete candidate-table objects).
psql "$PG" -X -v ON_ERROR_STOP=1 -f 00_setup.sql

# 4. Smoke: one batch, then CHECK IT WENT HOT before scaling.
WORKER_ID=smoke BATCH_SIZE=1000 MAX_BATCHES=1 PG_CONNECTION_STRING="$PG" ./repair.sh
psql "$PG" -X -c 'SELECT relname, n_tup_upd, n_tup_hot_upd,
                         round(100.0*n_tup_hot_upd/NULLIF(n_tup_upd,0),1) AS hot_pct
                  FROM pg_stat_user_tables WHERE relname = '\''DialogSearch'\'';'
#   want hot_pct ~100. If low: lower BATCH_SIZE, consider a VACUUM, watch GIN/replication.

# 5. Run. Single worker:
WORKER_ID=w1 BATCH_SIZE=10000 PG_CONNECTION_STRING="$PG" ./repair.sh
#   …or parallel, disjoint uuid ranges (each its own shell/tmux pane):
#   WORKER_ID=a UNTIL=80000000-0000-0000-0000-000000000000 PG_CONNECTION_STRING="$PG" ./repair.sh
#   WORKER_ID=b START_AT=80000000-0000-0000-0000-000000000000 PG_CONNECTION_STRING="$PG" ./repair.sh

# 6. Monitor (cheap; reads the checkpoint).
psql "$PG" -X -f 04_progress.sql

# 7. Verify once all workers report "range exhausted".
psql "$PG" -X -v ON_ERROR_STOP=1 -f 05_verify.sql   # remaining = 0

# 8. Clean up.
psql "$PG" -X -v ON_ERROR_STOP=1 -f 99_cleanup.sql
```

### Splitting the range for parallel workers

`DialogId` is a time-ordered uuid, so a simple split by the leading hex digit gives time-ordered
chunks (older data in the low ranges). Uneven chunk sizes are fine — workers just finish at different
times. For even chunks, compute boundaries from the data, e.g.:

```sql
SELECT ntile_bound FROM (
  SELECT percentile_disc(ARRAY[0.25,0.5,0.75]) WITHIN GROUP (ORDER BY "DialogId") AS ntile_bound
  FROM search."DialogSearch"
) s;  -- gives 3 split points -> 4 ranges
```

Assign each worker a disjoint `[START_AT, UNTIL)` from those boundaries.
