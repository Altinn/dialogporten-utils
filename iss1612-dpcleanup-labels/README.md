# iss1612-dpcleanup-labels

> Generated from `maintenance-job-scaffold/_template`. For the full runbook,
> safety rationale, and "why this shape", see
> [`../maintenance-job-scaffold/README.md`](../maintenance-job-scaffold/README.md).
> For the editing contract, see
> [`../maintenance-job-scaffold/AGENTS.md`](../maintenance-job-scaffold/AGENTS.md).

## Scope

One-shot repair for [altinn-correspondence#1612](https://github.com/Altinn/altinn-correspondence/issues/1612):
restore end-user **system labels** that were lost when dialogs were migrated from
Altinn Correspondence. Input is `ISS1612_DPCleanup.csv`
(`dialogid, TimeStamp, ActorId, SystemLabel`). For each row we set the label
exactly as the application would have, including the `LabelAssignmentLog` audit
entry and its `Actor`/`ActorName` rows.

A `(dialog, label)` row is in scope when **all** of these hold:

- the `dialogid` resolves to a `DialogEndUserContext` (via `DialogId`);
- the `SystemLabel` is in scope — `Bin`, `Archive`, or `MarkedAsUnopened`
  (`Default`/`Sent`/unknown are excluded and reported);
- the actor urn resolves to an `ActorName` (an existing row, else a brreg
  lookup); unresolved actors are skipped and reported;
- the **skip guard** holds: for a folder label (`Bin`/`Archive`) the context has
  no non-default folder label yet (we never override a label the user already
  set); and that exact label is not already present.

Expected candidate count: ≈ the CSV line count, minus the reported exclusions.
Confirm against the dry run (`02`).

This is a **silent repair**: `DialogEndUserContext."UpdatedAt"`/`"Revision"` are
deliberately not touched and no MassTransit/outbox row is written, so no domain
event is emitted and dialogs are not re-sorted by update time.

### How this job deviates from the vanilla scaffold

This repair is more than a single-column `UPDATE`, so within the JOB-SPECIFIC
fences (the harness — claim/`marked` CTEs, batch log, `repair.sh`, `04_progress.sql`
— is unchanged):

- **Composite candidate key** `("EntityId" = DialogEndUserContext."Id", "SystemLabelId")`,
  plus payload columns (`ActorNameEntityId`, `SourceTs`, `LabelName`).
- The mutation is a **delete-Default + multi-insert** chained CTE: for folder
  labels it deletes the mutually-exclusive `Default` label, then inserts the new
  `DialogEndUserContextSystemLabel`, the `LabelAssignmentLog`, and the `Actor`.
  All new ids use **`uuidv7()`** (native on the Postgres 18 prod DB) and all new
  rows use the CSV timestamp as `CreatedAt`.
- Extra **prep files** load the CSV and resolve org names before the worker loop:
  `00a_load_csv.sql`, `resolve-actornames.sh`, `00b_ensure_actornames.sql`.

> `resolve-actornames.sh` needs outbound HTTPS to
> `data.brreg.no`, plus `curl` and `jq`.

## Run order

Set the connection string once; do NOT put the password in the URI -- put it in
`~/.pgpass` (mode 0600). See the top-level runbook for connecting to prod
(forwarder / jump host).

```bash
export PG_CONNECTION_STRING='postgresql://repair_user@db.example.com:5432/dialogporten?sslmode=require'

cd iss1612-dpcleanup-labels

# 1. Create tables, view, procedure (idempotent). Needed first because the prep
#    steps below populate the staging/resolution tables.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 00_setup.sql

# 2. Load the CSV into staging (shell wrapper: psql \copy can't take a variable
#    path). Pass the path as an argument. Review the printed label/actor breakdown.
./00a-load-csv.sh /mnt/backfilldata/corr-1612/ISS1612_DPCleanup.csv

# 3. Resolve an ActorName for every actor (existing row, else brreg lookup).
#    Needs outbound HTTPS to data.brreg.no, plus curl + jq.
./resolve-actornames.sh

# 4. Create ActorName rows for brreg-resolved actors and link their ids.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 00b_ensure_actornames.sql

# 5. EXPLAIN sanity check -- confirm the candidate-build scan plan.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 03_explain_sanity_check.sql

# 6. Read-only dry-run count -- compare against the CSV line count / exclusions.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 02_dry_run_count.sql

# 7. Build the candidate set (safe to re-run). Review the left-behind report.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 01_build_candidates.sql

# 8. Smoke-test one batch, then spot-check the repaired dialogs.
WORKER_ID=smoke BATCH_SIZE=100 MAX_BATCHES=1 ./repair.sh

# 9. Run workers. Start with one; add more in separate shells as throughput allows.
WORKER_ID=w1 ./repair.sh
# (second shell) WORKER_ID=w2 ./repair.sh

# 10. Monitor at any time.
psql "$PG_CONNECTION_STRING" -X -f 04_progress.sql

# 11. Verify once remaining = 0 (every invariant must return 0).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 05_verify.sql

# 12. Export the batch log (audit trail), then clean up.
psql "$PG_CONNECTION_STRING" -X -c "COPY (SELECT * FROM maintenance.\"Iss1612DpcleanupLabels_BatchLog\" ORDER BY \"BatchId\") TO STDOUT WITH CSV HEADER" > iss1612-dpcleanup-labels-batchlog.csv
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 99_cleanup.sql
```
