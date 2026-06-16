# maintenance-job-scaffold

A scaffold for **one-off, large-scale Postgres maintenance jobs** against the
Dialogporten database: populate a candidate table of entity IDs, then iterate
over it in batches applying a mutation -- safely, resumably, in parallel, and
observably.

Each job is plain, reviewable SQL plus a shell worker loop. `new-job.sh` stamps
out a concrete job from `_template/`; you fill in a handful of clearly fenced
edit points. See [`AGENTS.md`](AGENTS.md) for the editing contract (it applies
to humans as much as to LLM agents).

## When NOT to use this

This harness does **silent, set-based repairs** via direct SQL. It deliberately
does **not**:

- emit domain events -- it bypasses the MassTransit outbox;
- bump `"UpdatedAt"` or `"Revision"` (the EF optimistic-concurrency token);
- run any application/domain logic.

If a job needs any of those, do it in the application layer (e.g. the Janitor
tool) instead.

Note: "silent" means no **outbox** event. The `UPDATE`s still go through WAL, so
any logical-replication / CDC consumer subscribed to the target table will see
the row changes. Account for that if such a consumer exists.

## Generating a job

```bash
./new-job.sh my-fix-name          # creates ../my-fix-name/
```

The single kebab-case name drives every identifier:
`my-fix-name` -> `MyFixName_*` tables, `myfixname_run_batch` procedure,
`maint-my-fix-name-<worker>` application_name. Then fill in the `TODO(job)`
fences and confirm none remain:

```bash
grep -rn 'TODO(job)\|TODO_\|{{' ../my-fix-name/   # empty == ready
```

## Lifecycle checklist

1. **Scaffold** the job and **fill the fences** (see `AGENTS.md`).
2. **Review** the resulting SQL -- it's plain and small on purpose.
3. *(Optional)* **Rehearse** against a restored backup. The forwarder and
   restore flow live in the sibling utils:
   [`../database-forwarder`](../database-forwarder) and the main repo's
   `docs/RestoreDatabase.md`.
4. **Connect.** Set `PG_CONNECTION_STRING` and put the password in `~/.pgpass`
   (mode 0600) -- never in the URI (keeps it out of shell history / process
   listings). Run long jobs from `tmux` on the jump host so a dropped laptop
   connection doesn't kill the workers.
5. **`03` EXPLAIN sanity check** -- confirm the candidate scan plan.
6. **`02` dry-run count** -- compare to the expected order of magnitude.
7. **`00` setup** -- create the schema objects (idempotent).
8. **`01` populate** the candidate table (safe to re-run).
9. **Smoke batch:** `WORKER_ID=smoke BATCH_SIZE=100 MAX_BATCHES=1 ./repair.sh`,
   then spot-check the repaired rows.
10. **Scale workers** one shell at a time, watching app latency, replication lag,
    and `pg_stat_activity`. Add workers only while throughput keeps climbing.
11. **`04` monitor** -- progress, ETA, recent throughput, live workers.
12. **`05` verify** -- `remaining = 0` and the job-specific invariant returns 0.
13. **Export** the batch log to CSV (audit trail), then **`99` cleanup**.

## Safety & operations

- **Silent-repair caveat** -- see "When NOT to use this" above. The mutation must
  touch only the columns being fixed.
- **Resumability** -- killing a worker is safe: the worst case is one in-flight
  batch rolls back and its claimed rows become re-claimable. Re-running the
  candidate build is safe (`ON CONFLICT DO NOTHING`). To pause, just stop the
  workers; to resume, start them again.
- **`SYNC_COMMIT=off`** (default) lets each batch return without waiting on WAL
  fsync, for throughput. Safe precisely because re-claim is idempotent -- a lost
  last batch is simply redone. Set `SYNC_COMMIT=on` for durable commits.
- **Timeouts** -- `STATEMENT_TIMEOUT` (default `15min`) bounds a pathological
  plan; `LOCK_TIMEOUT` (default `10s`) turns an indefinite stall behind an app
  transaction or DDL into a visible error. Override per-worker; `0` disables.
- **Throttling** -- `SLEEP_BETWEEN_BATCHES=<secs>` if you see contention on the
  target table. Smaller `BATCH_SIZE` also reduces per-statement lock footprint.
- **Off-peak** -- the candidate build can scan large tables; prefer off-peak and
  watch I/O.
- **Poison rows** -- there is no automatic quarantine (fail-fast by design:
  `ON_ERROR_STOP` + `set -e` halt the worker on a deterministic error so you can
  investigate). To shelve a known-bad row by hand:
  `UPDATE maintenance."<Job>_Candidates" SET "ProcessedAt" = now(), "Outcome" = 'manual_skip' WHERE "EntityId" = '...';`

## Why this shape

- **`FOR UPDATE SKIP LOCKED`** lets N workers compete for batches with no overlap
  and no global coordination layer -- each claims a disjoint set.
- **One chained CTE per batch** (claim -> mutate -> mark) keeps the locks the
  claim acquired held through the dependent writes, so no other worker can steal
  a claimed row between statements.
- **Partial index** `WHERE "ProcessedAt" IS NULL` -- processed rows drop out of
  the index, so the claim query stays cheap as the run drains.
- **Minimal write** -- touching only the fixed column(s) avoids a phantom
  concurrency-token bump for concurrent EF readers and emits no outbox event.

## What's intentionally left out (v1)

Per-batch exception trapping / poison-row auto-quarantine, generator-level
parameterized key shapes, multi-worker orchestration wrappers, and an automatic
`VACUUM` step. Add them per-job if a specific run needs them.
