# dialog-search-scope-backfill

> Generated from `maintenance-job-scaffold/_template`. For the full runbook,
> safety rationale, and "why this shape", see
> [`../maintenance-job-scaffold/README.md`](../maintenance-job-scaffold/README.md).
> For the editing contract, see
> [`../maintenance-job-scaffold/AGENTS.md`](../maintenance-job-scaffold/AGENTS.md).

## Scope

One-shot backfill of the two columns added by the additive migration
`AddDialogSearchContentUpdatedAtAndServiceResource`:

- `search."DialogSearch"."ContentUpdatedAt"` <- `public."Dialog"."ContentUpdatedAt"`
- `search."DialogSearch"."ServiceResource"` <- `public."Dialog"."ServiceResource"`,
  with the static `urn:altinn:resource:` prefix stripped (to match the
  `UpsertDialogSearchOne` / `RebuildDialogSearchOnce` functions and `VDialogDocument`).

New and updated rows are already populated by those functions; this job fills the
pre-existing backlog so the columns can be relied on (and indexed) before the
application change that consumes them.

An entity is in scope when:

- it is a row in `search."DialogSearch"`, and
- `"ContentUpdatedAt" IS NULL OR "ServiceResource" IS NULL` (not yet backfilled).

Expected candidate count: on the first run, ~the entire `search."DialogSearch"` row
count (every pre-existing row needs backfilling -- ~1B in production). The
candidate-build scan is therefore a full sequential scan **by design**; unlike a
typical selective repair, a Seq Scan in the `03` sanity check is expected here.

This is a **silent repair**: only the target column(s) are written. `UpdatedAt`,
`Revision`, and the MassTransit outbox are deliberately not touched, so no domain
event is emitted.

## Run order

Set the connection string once; do NOT put the password in the URI -- put it in
`~/.pgpass` (mode 0600). See the top-level runbook for connecting to prod
(forwarder / jump host).

```bash
export PG_CONNECTION_STRING='postgresql://repair_user@db.example.com:5432/dialogporten?sslmode=require'

cd dialog-search-scope-backfill

# 1. EXPLAIN sanity check -- confirm the candidate scan plan.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 03_explain_sanity_check.sql

# 2. Read-only dry-run count -- compare against the expectation above.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 02_dry_run_count.sql

# 3. Create tables, view, procedure (idempotent).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 00_setup.sql

# 4. Build the candidate set (safe to re-run).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 01_build_candidates.sql

# 5. Smoke-test one batch, then spot-check the repaired entities.
WORKER_ID=smoke BATCH_SIZE=100 MAX_BATCHES=1 ./repair.sh

# 6. Run workers. Start with one; add more in separate shells as throughput allows.
WORKER_ID=w1 ./repair.sh
# (second shell) WORKER_ID=w2 ./repair.sh

# 7. Monitor at any time.
psql "$PG_CONNECTION_STRING" -X -f 04_progress.sql

# 8. Verify once remaining = 0.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 05_verify.sql

# 9. Export the batch log (audit trail), then clean up.
psql "$PG_CONNECTION_STRING" -X -c "COPY (SELECT * FROM maintenance.\"DialogSearchScopeBackfill_BatchLog\" ORDER BY \"BatchId\") TO STDOUT WITH CSV HEADER" > dialog-search-scope-backfill-batchlog.csv
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 99_cleanup.sql
```
