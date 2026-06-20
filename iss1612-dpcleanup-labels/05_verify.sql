-- iss1612-dpcleanup-labels verification: run after the workers report the candidate table is
-- drained, BEFORE 99_cleanup.sql. Confirms the repair did what it should and
-- nothing is left behind.

\echo '-- 1. No unprocessed candidates remain (generic; must be 0)'
SELECT COUNT(*) AS unprocessed_remaining
FROM maintenance."Iss1612DpcleanupLabels_Candidates"
WHERE "ProcessedAt" IS NULL;

\echo ''
\echo '-- 2. Outcome breakdown (generic)'
SELECT "Outcome", COUNT(*)
FROM maintenance."Iss1612DpcleanupLabels_Candidates"
GROUP BY "Outcome"
ORDER BY "Outcome" NULLS FIRST;

\echo ''
\echo '-- 3. Job-specific invariants (each count must be 0)'
-- >>> JOB-SPECIFIC (edit me) >>>
-- 3a. Every 'updated' candidate must now have its target label present.
\echo '3a. updated candidates missing the target label (expect 0):'
SELECT COUNT(*) AS updated_missing_label
FROM maintenance."Iss1612DpcleanupLabels_Candidates" c
WHERE c."Outcome" = 'updated'
  AND NOT EXISTS (
      SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
      WHERE l."DialogEndUserContextId" = c."EntityId"
        AND l."SystemLabelId" = c."SystemLabelId"
  );

-- 3b. For folder-label targets (Bin/Archive), the mutually-exclusive Default(1)
--     must have been removed.
\echo ''
\echo '3b. folder-label updates that still carry Default (expect 0):'
SELECT COUNT(*) AS folder_update_still_has_default
FROM maintenance."Iss1612DpcleanupLabels_Candidates" c
WHERE c."Outcome" = 'updated'
  AND c."SystemLabelId" IN (2, 3)
  AND EXISTS (
      SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
      WHERE l."DialogEndUserContextId" = c."EntityId"
        AND l."SystemLabelId" = 1
  );

-- 3c. Every 'updated' candidate must have exactly one matching 'set'
--     LabelAssignmentLog with a well-formed LabelAssignmentLogActor.
\echo ''
\echo '3c. updated candidates with no matching set-log + actor (expect 0):'
SELECT COUNT(*) AS updated_without_matching_log
FROM maintenance."Iss1612DpcleanupLabels_Candidates" c
WHERE c."Outcome" = 'updated'
  AND NOT EXISTS (
      SELECT 1
      FROM public."LabelAssignmentLog" lal
      JOIN public."Actor" a
             ON a."LabelAssignmentLogId" = lal."Id"
            AND a."Discriminator" = 'LabelAssignmentLogActor'
            AND a."ActorTypeId" = 1
            AND a."ActorNameEntityId" IS NOT NULL
      WHERE lal."ContextId" = c."EntityId"
        AND lal."Action" = 'set'
        AND lal."Name" = c."LabelName"
  );
-- <<< JOB-SPECIFIC <<<
