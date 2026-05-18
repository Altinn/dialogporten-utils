# HasUnopenedContent Repair

Temporary standalone PostgreSQL scripts for repairing dialogs where
`"Dialog"."HasUnopenedContent"` stayed `true` after affected
`TransmissionOpened` or `CorrespondenceOpened` activities were created by the
regressed `CreateActivity` endpoint.

The repair mirrors `DialogUnopenedContent.HasUnopenedContent` and only changes
`"HasUnopenedContent"` from `true` to `false`.

Affected activity window:

- start: `2026-02-25 07:26:42Z`
- end: `2026-03-20 10:10:58Z`

## Files

- `00_create_objects.sql`: maintenance schema, unlogged progress tables, procedures and summary view.
- `01_create_indexes.sql`: concurrent temporary repair indexes.
- `02_discover_candidates.sql`: runs one discovery batch.
- `03_process_candidates.sql`: runs one processing batch.
- `04_verify.sql`: summary and invariant checks.
- `05_drop_indexes.sql`: removes temporary repair indexes.
- `run-discovery.sh`: loops `02_discover_candidates.sql` until discovery is complete.
- `run-processing.sh`: loops `03_process_candidates.sql` until no unprocessed candidates remain.

## Jump Host Invocation

Run from `tmux` on the jump host:

```bash
tmux new -s huc-repair
export DATABASE_URL='postgresql://...'
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f repair-has-unopened-content/00_create_objects.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f repair-has-unopened-content/01_create_indexes.sql
BATCH_SIZE=100000 ./repair-has-unopened-content/run-discovery.sh "$DATABASE_URL"
BATCH_SIZE=1000 ./repair-has-unopened-content/run-processing.sh "$DATABASE_URL"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f repair-has-unopened-content/04_verify.sql
```

Do not run `01_create_indexes.sql` or `05_drop_indexes.sql` with
`--single-transaction`; PostgreSQL requires concurrent index operations to run
outside an explicit transaction block.

The shell runners accept optional throttling:

```bash
SLEEP_SECONDS=1 BATCH_SIZE=500 ./repair-has-unopened-content/run-processing.sh "$DATABASE_URL"
```

The SQL procedures emit `RAISE NOTICE` progress lines with batch counts,
elapsed seconds and rows per second.

## Cancel And Resume

Cancel with `Ctrl-C` in `psql` or stop the shell runner. Each procedure call is
one transaction, so previously committed batches remain recorded in
`maintenance.has_unopened_content_repair_candidate` and
`maintenance.has_unopened_content_repair_state`.

Rerun the same command to resume.

The maintenance tables are `UNLOGGED` to reduce WAL pressure. If PostgreSQL
crashes or restarts uncleanly during the repair, PostgreSQL may truncate those
tables. In that case rerun `00_create_objects.sql` and start discovery again.

## Cleanup

After production sign-off, remove the temporary indexes:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f repair-has-unopened-content/05_drop_indexes.sql
```

Keep the maintenance tables until counts have been reviewed. They can be
dropped later with:

```sql
DROP VIEW IF EXISTS maintenance.has_unopened_content_repair_summary;
DROP TABLE IF EXISTS maintenance.has_unopened_content_repair_batch_log;
DROP TABLE IF EXISTS maintenance.has_unopened_content_repair_candidate;
DROP TABLE IF EXISTS maintenance.has_unopened_content_repair_state;
```
