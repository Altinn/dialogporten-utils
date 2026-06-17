-- dialog-search-scope-backfill dry-run: read-only count of the candidates that
-- 01_build_candidates.sql would insert. Use this to validate the predicate and
-- estimate scope BEFORE running the actual build.
--
-- IMPORTANT: this must mirror the access pattern of 01_build_candidates.sql.
-- If the build uses a per-resource MATERIALIZED CTE loop to stay on an index
-- plan, this dry run must use the same shape -- otherwise it validates the
-- scope but not the plan, and the real build may behave very differently.

-- >>> JOB-SPECIFIC (edit me) >>>
-- COUNT(*) over the SAME predicate as 01_build_candidates.sql.
SELECT COUNT(*) AS candidates_that_would_be_inserted
FROM search."DialogSearch" ds
WHERE ds."ContentUpdatedAt" IS NULL OR ds."ServiceResource" IS NULL;
-- <<< JOB-SPECIFIC <<<
