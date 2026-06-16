-- {{JOB_KEBAB}} step 01: build the candidate set.
--
-- Populates maintenance."{{JOB_PASCAL}}_Candidates" with one row per entity that
-- needs repair. Safe to re-run -- ON CONFLICT DO NOTHING absorbs duplicates if the
-- script errors partway and is restarted.
--
-- Run the read-only 02_dry_run_count.sql FIRST to validate the predicate and
-- estimate scope before inserting anything.

-- >>> JOB-SPECIFIC (edit me) >>>
-- TODO(job): select the entities to repair.
--   * The SELECT must yield exactly the candidate key column(s) ("EntityId").
--   * This predicate defines the scope of the repair -- get it right; the
--     dry-run count (02) must use the SAME predicate so the count is meaningful.
--   * If at production scale the planner flips to a parallel seq scan over a
--     large table, do NOT just accept it -- see the per-resource MATERIALIZED
--     CTE loop in ../a1-status-fix/02_build_candidates.sql, which walls the
--     selective inner scan off so the index plan is preserved. Escalate to that
--     shape (a DO $$ ... $$ loop with RAISE NOTICE progress) when needed.
INSERT INTO maintenance."{{JOB_PASCAL}}_Candidates" ("EntityId")
SELECT t."Id"
FROM public."TODO_TargetTable" t
WHERE t."TODO_Column" = 'TODO_broken_value'
ON CONFLICT ("EntityId") DO NOTHING;
-- <<< JOB-SPECIFIC <<<

-- Report what was built (generic).
SELECT COUNT(*)                                       AS candidates_total,
       COUNT(*) FILTER (WHERE "ProcessedAt" IS NULL)  AS unprocessed
FROM maintenance."{{JOB_PASCAL}}_Candidates";
