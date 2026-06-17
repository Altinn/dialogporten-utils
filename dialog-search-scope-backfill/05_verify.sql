-- dialog-search-scope-backfill verification: run after the workers report the candidate table is
-- drained, BEFORE 99_cleanup.sql. Confirms the repair did what it should and
-- nothing is left behind.

\echo '-- 1. No unprocessed candidates remain (generic; must be 0)'
SELECT COUNT(*) AS unprocessed_remaining
FROM maintenance."DialogSearchScopeBackfill_Candidates"
WHERE "ProcessedAt" IS NULL;

\echo ''
\echo '-- 2. Outcome breakdown (generic)'
SELECT "Outcome", COUNT(*)
FROM maintenance."DialogSearchScopeBackfill_Candidates"
GROUP BY "Outcome"
ORDER BY "Outcome" NULLS FIRST;

\echo ''
\echo '-- 3. Job-specific invariant'
-- >>> JOB-SPECIFIC (edit me) >>>
-- Of the candidates marked 'updated', how many are STILL unpopulated? Expect 0.
SELECT COUNT(*) AS updated_but_still_broken
FROM maintenance."DialogSearchScopeBackfill_Candidates" c
JOIN search."DialogSearch" ds ON ds."DialogId" = c."EntityId"
WHERE c."Outcome" = 'updated'
  AND (ds."ContentUpdatedAt" IS NULL OR ds."ServiceResource" IS NULL);
-- <<< JOB-SPECIFIC <<<
