-- {{JOB_KEBAB}} sanity check: confirm the candidate-build scan uses the plan you
-- expect BEFORE kicking off the full build against production.
--
-- What to look for in the output:
--   - the index you expect is used (Index Scan / Index Only Scan), NOT a
--     "Parallel Seq Scan" over a large table you did not intend to scan fully
--   - for an Index Only Scan, Heap Fetches close to 0 (run a VACUUM first if not)
--   - selective filters applied at the scan, not late in the plan
--
-- Use plain EXPLAIN (no ANALYZE) so nothing is executed. Only add ANALYZE if you
-- are certain the query has no side effects and the runtime is acceptable.

-- >>> JOB-SPECIFIC (edit me) >>>
-- TODO(job): EXPLAIN the inner candidate-selection query from
-- 01_build_candidates.sql (the SELECT, not the INSERT).
EXPLAIN
SELECT t."Id"
FROM public."TODO_TargetTable" t
WHERE t."TODO_Column" = 'TODO_broken_value';
-- <<< JOB-SPECIFIC <<<
