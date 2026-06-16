# AGENTS.md -- generating a maintenance job from this scaffold

This file is the contract for an LLM agent (or human) turning the template in
`_template/` into a concrete, runnable repair job. Follow it top to bottom.

The harness is a proven pattern: a candidate table of entity IDs, a batch
procedure that claims rows with `FOR UPDATE SKIP LOCKED` and mutates them, and a
shell worker loop. Your job is to fill in **only** the job-specific parts. The
rest is load-bearing and must not change.

## 0. Scope check -- is this harness the right tool?

Use it for **silent, set-based data repairs**: "for every entity matching some
predicate, change these columns to these values." Do NOT use it if the job needs
to:

- emit domain events / notify downstream consumers (the repair bypasses the
  MassTransit outbox), or
- bump `UpdatedAt` / `Revision` (the EF optimistic-concurrency token), or
- run application/domain logic per entity.

Those belong in the application layer (e.g. the Janitor tool), not here. If the
task needs any of the above, stop and say so.

## 1. Collect these inputs before writing anything

Ask the user (or confirm from context) all of the following. Do not guess.

1. **Target table(s)** and the **key column** that identifies an entity
   (default assumption: `public."<Table>"."Id"`, a uuid).
2. **Broken-state predicate** -- how to identify entities needing repair.
3. **The fix** -- which column(s) to set, and to what value(s).
4. **Guard predicate** -- the condition (usually the broken state itself) that
   must STILL hold at mutation time for the fix to apply. This is what makes the
   job safe against rows that changed between candidate-build and processing.
5. **Expected candidate count** (order of magnitude) -- to sanity-check the dry run.
6. **Verification invariant** -- how we know afterwards the repair held.
7. **Index needs** -- will the candidate-build predicate be fast at production
   scale, or is a temporary index / a per-partition build loop required?

## 2. Workflow

1. Run the generator -- never hand-copy `_template/`:
   ```bash
   ./new-job.sh <job-name-in-kebab-case>
   ```
2. Fill in each region fenced by `-- >>> JOB-SPECIFIC (edit me)` /
   `-- <<< JOB-SPECIFIC`. Every fence contains a `TODO(job):` note telling you
   what goes there. Edit **only inside the fences** (plus the `## Scope` section
   and TODO blanks in the generated `README.md`).
3. Confirm nothing is left unfilled:
   ```bash
   grep -rn 'TODO(job)' <job-dir>/          # lists remaining work
   grep -rn 'TODO(job)\|TODO_\|{{' <job-dir>/   # MUST be empty when done
   ```
   The stubs reference a nonexistent `public."TODO_TargetTable"`, so any
   unfilled file fails loudly under `ON_ERROR_STOP` rather than doing something
   wrong -- but don't rely on that; the greps must come back clean.
4. You do **not** have production access. Your deliverable is reviewable SQL +
   the per-job README. A human runs the scripts against the database following
   the run order in the generated README.

## 3. Hard rules (do not violate)

- **Silent repair only.** In the mutation, set only the columns being fixed.
  Never write `"UpdatedAt"` or `"Revision"`, and never `INSERT` into any
  `MassTransit*` / outbox table.
- **Keep the guard predicate.** The `updated` CTE must re-assert the broken-state
  condition in its `WHERE`, so a row that changed since candidate-build is left
  alone and recorded as `skipped_state_changed`, never double-fixed.
- **Do not alter the harness.** The claim CTE (`FOR UPDATE SKIP LOCKED`, the
  `ORDER BY`, the `LIMIT`), the `marked` CTE, the single-chained-statement
  structure, the batch-log writes, and `repair.sh`'s control flow are invariant.
  The mutation lives in exactly one place: the `updated` CTE in `00_setup.sql`.
- **Idempotent candidate build.** Keep `ON CONFLICT (...) DO NOTHING` so a
  re-run after an interruption inserts no duplicates.
- **Dry run mirrors the build.** `02_dry_run_count.sql` must use the same access
  pattern as `01_build_candidates.sql`, or the count is misleading.

## 4. Escalation patterns (when the simple stub isn't enough)

- **Planner flips to a seq scan on the candidate build.** A highly-selective
  predicate combined with a low-selectivity one can make Postgres choose a
  parallel seq scan over a huge table. Wall the selective inner query off in a
  `MATERIALIZED` CTE and apply the rest outside it, iterating per partition
  (e.g. per resource) in a `DO $$ ... $$` loop with `RAISE NOTICE` progress.
  Worked example: `../a1-status-fix/02_build_candidates.sql`.
- **The candidate-build or mutation needs an index that doesn't exist.** Add a
  `00b_create_indexes.sql` using `CREATE INDEX CONCURRENTLY` (non-blocking) and a
  matching `98_drop_indexes.sql`. Worked example:
  `../repair-has-unopened-content/01_create_indexes.sql`.
- **Composite or non-uuid candidate key.** Adapt the key shape -- the candidate
  table comment in `00_setup.sql` lists the exact local edits.

## 5. Summary you must produce

When you hand the job back, include:

- the **scope definition** (the in-scope predicate, in words);
- the **expected candidate count** and how it compares to the dry-run plan;
- the **list of files you edited** and what each fence now contains;
- the **verification query** and what result proves success;
- any **escalation** you applied (temp index, per-partition build) and why.
