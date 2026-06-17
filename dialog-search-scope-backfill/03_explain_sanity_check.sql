-- dialog-search-scope-backfill sanity check: confirm the candidate-build scan uses the plan you
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
-- EXPLAIN the inner candidate-selection query from 01_build_candidates.sql (the SELECT, not the
-- INSERT). NOTE: on the first run nearly every row matches (all freshly-added columns are NULL),
-- so a Seq Scan is EXPECTED and correct here -- this is a full-table backfill, not a selective repair.
EXPLAIN
SELECT ds."DialogId"
FROM search."DialogSearch" ds
WHERE ds."ContentUpdatedAt" IS NULL OR ds."ServiceResource" IS NULL;
-- <<< JOB-SPECIFIC <<<
