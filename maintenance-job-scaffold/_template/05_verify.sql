-- {{JOB_KEBAB}} verification: run after the workers report the candidate table is
-- drained, BEFORE 99_cleanup.sql. Confirms the repair did what it should and
-- nothing is left behind.

\echo '-- 1. No unprocessed candidates remain (generic; must be 0)'
SELECT COUNT(*) AS unprocessed_remaining
FROM maintenance."{{JOB_PASCAL}}_Candidates"
WHERE "ProcessedAt" IS NULL;

\echo ''
\echo '-- 2. Outcome breakdown (generic)'
SELECT "Outcome", COUNT(*)
FROM maintenance."{{JOB_PASCAL}}_Candidates"
GROUP BY "Outcome"
ORDER BY "Outcome" NULLS FIRST;

\echo ''
\echo '-- 3. Job-specific invariant'
-- >>> JOB-SPECIFIC (edit me) >>>
-- TODO(job): assert the repair held. The strongest check is: of the candidates
-- marked 'updated', how many are STILL in the broken state? Expect 0.
SELECT COUNT(*) AS updated_but_still_broken
FROM maintenance."{{JOB_PASCAL}}_Candidates" c
JOIN public."TODO_TargetTable" t ON t."Id" = c."EntityId"
WHERE c."Outcome" = 'updated'
  AND t."TODO_Column" = 'TODO_broken_value';
-- <<< JOB-SPECIFIC <<<
