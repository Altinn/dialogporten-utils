# iss1712-dpcleanup-actors

> Generated from `maintenance-job-scaffold/_template`. For the full runbook,
> safety rationale, and "why this shape", see
> [`../maintenance-job-scaffold/README.md`](../maintenance-job-scaffold/README.md).
> For the editing contract, see
> [`../maintenance-job-scaffold/AGENTS.md`](../maintenance-job-scaffold/AGENTS.md).

## Scope

One-shot repair for altinn-correspondence
[#1716](https://github.com/Altinn/altinn-correspondence/issues/1716) and its
follow-up [#1951](https://github.com/Altinn/altinn-correspondence/issues/1951):
when "Read" (`CorrespondenceOpened`) and "Confirm" (`CorrespondenceConfirmed`)
status events were synced from Altinn 2 to Dialogporten, the resulting
`DialogActivity` rows got the **recipient** as their `PerformedBy` actor instead
of the **person who actually performed the action**. This job repoints each
affected `PerformedByActor` to the correct person.

The fix is a single column write: `public."Actor"."ActorNameEntityId"` is
repointed to an `public."ActorName"` row carrying the real performer's
`(ActorId, Name)`. `ActorTypeId` (1, PartyRepresentative) and `Discriminator`
are unchanged.

Input: two CSVs with the same 6-column shape
`DialogId,DialogActivityId,Timestamp,ActorId,ActorName,ActivityType`:
- `ISS1712_DPCleanup.csv` — **~6,359,589** rows (original batch)
- `ISS1951_DPCleanup.csv` — **~140,000,000** rows / ~30 GB (follow-up)

The same job/procedure handles both — they are loaded into the same staging
table. The CSVs may contain **duplicate `DialogActivityId`s** (Correspondence has
no explicit event→activity link), so the agreed match key is `DialogId` +
`DialogActivityId` + `Timestamp`.

A candidate (one per PerformedBy actor) is built when **all** hold:

- the CSV row matches a real activity: `DialogActivity."Id" = DialogActivityId`
  **and** `"DialogId" = DialogId` **and** `"CreatedAt" = Timestamp` **and**
  `"TypeId"` matches `ActivityType` (16/17),
- all CSV rows matched to that activity agree on a single `(ActorId, Name)`
  (conflicting actors are excluded as ambiguous, for manual follow-up),
- the activity has a `DialogActivityPerformedByActor`,
- its `(ActorId, Name)` resolves to an `ActorName` row, and
- the actor is not already pointing there.

Expected candidate count: ~`10^6` for 1712, up to ~`10^8` for 1951 (the dry run
reports the figure + a per-reason breakdown; 01 also logs per-bucket counts).
Most CSV actors already have an `ActorName` row; see the `00b` resolution
breakdown.

This is a **silent repair**: only `ActorNameEntityId` is written. `UpdatedAt`,
`Revision`, and the MassTransit outbox are deliberately not touched, so no domain
event is emitted.

### Scale (ISS-1951) — what's different

Against billion-row live tables (`Actor` ≈ 3.16 B / 629 GB, `DialogActivity` ≈
2.29 B / 521 GB), the logic is unchanged but:
- `Staging` and `Candidates` are **`UNLOGGED`** (rebuildable; big WAL savings).
  If the DB restarts mid-run they truncate — reload (`00a`) / rebuild (`01`) and
  resume; the mutation guard makes re-processing already-fixed rows a no-op.
- `01_build_candidates.sql` runs as a **resumable, chunked `DO` loop** over 256
  `dialog_activity_id` byte-buckets (index range scans; per-activity rows stay in
  one bucket so the ambiguity check holds). Re-running skips done work.
- `02_dry_run_count.sql` is expensive at 1951 scale — sample it (see the note at
  the top of that file) or rely on 01's per-bucket counts.

**Run ISS-1712 first as a prod warm-up** (calibrate worker count / sleep), then
load ISS-1951 into the same job objects and repeat — see the run order below.

#### Throttling & ops (live, no maintenance window)

- Moderate `BATCH_SIZE` (≈2k–5k) + `SLEEP_BETWEEN_BATCHES` > 0; several parallel
  `WORKER_ID`s, tuned up from the 1712 warm-up. (`repair.sh` defaults
  `SYNC_COMMIT=off`, `LOCK_TIMEOUT=10s`, `STATEMENT_TIMEOUT=15min`.)
- Watch replication lag / WAL rate, `Actor` dead tuples + autovacuum, prod
  latency; back off if any climbs. ~146M updated rows = ~146M dead tuples on a
  629 GB table + `IX_Actor_ActorNameEntityId` churn — confirm autovacuum keeps up
  and run a post-run `VACUUM (ANALYZE) public."Actor"`. **Flag to DBA/ops.**
- Settle the **search/read-model reindex** question (silent repair emits no
  event) with the team *before* the 1951 run — 146M activities is not negligible.

### Note on the pending #3171 reorder repair

Person names in `ActorName` are currently stored `LAST FIRST MIDDLE`; the CSV
uses the same order. Dialogporten
[#3171](https://github.com/Altinn/dialogporten/issues/3171) will later reorder
**every** `urn:altinn:person:%` row in place, and its drafted SQL has no
`(ActorId, Name)` collision handling. This job runs **independently, before
#3171**, and `00b_resolve_actornames.sql` is deliberately collision-safe: it
**reuses** any existing row matching the CSV name in either ordering and only
**inserts** a new row (in `LAST FIRST MIDDLE` order) when the person has none —
so it never creates a duplicate that would collide under #3171. Give the #3171
owners a heads-up so they include collision/dedup handling in their final UPDATE
regardless.

## Run order

Set the connection string once; do NOT put the password in the URI -- put it in
`~/.pgpass` (mode 0600). The CSVs live on the prod-adjacent host, so run
`00a-load-csv.sh` there (or anywhere the file and the DB are both reachable).

### Part A — ISS-1712 (warm-up)

```bash
export PG_CONNECTION_STRING='postgresql://repair_user@db.example.com:5432/dialogporten?sslmode=require'

cd iss1712-dpcleanup-actors

# 1. Create tables, view, procedure (idempotent; Staging + Candidates UNLOGGED).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 00_setup.sql

# 2. Load the CSV into staging (truncates, loads, builds the index, reports).
./00a-load-csv.sh /mnt/backfilldata/corr-1712/ISS1712_DPCleanup.csv

# 3. Resolve each (actor, name) to an ActorName id (collision-safe; see above).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 00b_resolve_actornames.sql

# 4. EXPLAIN sanity check -- confirm index lookups into DialogActivity/Actor.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 03_explain_sanity_check.sql

# 5. Read-only dry-run -- candidate count + per-reason breakdown.
#    Review '2_timestamp_mismatch' (drift) and '6_ambiguous_multiple_actors'.
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 02_dry_run_count.sql

# 6. Build the candidate set (chunked DO loop; safe to re-run).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 01_build_candidates.sql

# 7. Smoke-test one batch, then spot-check the repaired actors (table below).
WORKER_ID=smoke BATCH_SIZE=100 MAX_BATCHES=1 ./repair.sh

# 8. Run workers (throttled). Start with one; add more in separate shells.
WORKER_ID=w1 BATCH_SIZE=5000 SLEEP_BETWEEN_BATCHES=0.2 ./repair.sh
# (second shell) WORKER_ID=w2 BATCH_SIZE=5000 SLEEP_BETWEEN_BATCHES=0.2 ./repair.sh

# 9. Monitor at any time.
psql "$PG_CONNECTION_STRING" -X -f 04_progress.sql

# 10. Verify once remaining = 0.  (Do NOT 99_cleanup yet -- 1951 reuses the objects.)
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 05_verify.sql
```

### Part B — ISS-1951 (the big one, after the warm-up looks good)

```bash
# Reuses the same job objects from Part A. Calibrate BATCH_SIZE / SLEEP / worker
# count from what the warm-up showed; watch the ops signals above throughout.

# 1. Re-load staging with the 1951 CSV (TRUNCATEs, then loads ~140M rows + index).
./00a-load-csv.sh /mnt/backfilldata/corr-1951/ISS1951_DPCleanup.csv

# 2. Resolve the new distinct persons (already-resolved rows untouched).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 00b_resolve_actornames.sql

# 3. (optional) sampled dry-run + EXPLAIN (see note atop 02_dry_run_count.sql).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 03_explain_sanity_check.sql

# 4. Build candidates (chunked; appends ~140M, deduped vs 1712 via ON CONFLICT).
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 01_build_candidates.sql

# 5. Run workers (more of them than the warm-up), monitor, verify.
WORKER_ID=w1 BATCH_SIZE=5000 SLEEP_BETWEEN_BATCHES=0.2 ./repair.sh
# ... add w2..wN in separate shells ...
psql "$PG_CONNECTION_STRING" -X -f 04_progress.sql
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 05_verify.sql
```

### Finish (after BOTH datasets are done)

```bash
# Export the batch log (audit trail), VACUUM the churned table, then clean up.
psql "$PG_CONNECTION_STRING" -X -c "COPY (SELECT * FROM maintenance.\"Iss1712DpcleanupActors_BatchLog\" ORDER BY \"BatchId\") TO STDOUT WITH CSV HEADER" > iss1712-dpcleanup-actors-batchlog.csv
psql "$PG_CONNECTION_STRING" -X -c 'VACUUM (ANALYZE) public."Actor";'
psql "$PG_CONNECTION_STRING" -X -v ON_ERROR_STOP=1 -f 99_cleanup.sql
```

## Spot-check

After the smoke batch (or full run), pick a handful of processed candidates and
confirm each one's `PerformedBy` actor flipped **from a recipient organization to
the person named in that activity's CSV row** — i.e. the actor's `ActorName` now
matches the CSV `(ActorId, Name)`, `ActorTypeId` is still `1`, and `UpdatedAt` is
unchanged. Drive this from the CSV / candidate table rather than hard-coding
examples here, so no personal identifiers live in the repo. `05_verify.sql`
asserts the same invariant across every `'updated'` candidate.
