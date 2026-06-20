-- iss1712-dpcleanup-actors verification: run after the workers report the candidate table is
-- drained, BEFORE 99_cleanup.sql. Confirms the repair did what it should and
-- nothing is left behind.

\echo '-- 1. No unprocessed candidates remain (generic; must be 0)'
SELECT COUNT(*) AS unprocessed_remaining
FROM maintenance."Iss1712DpcleanupActors_Candidates"
WHERE "ProcessedAt" IS NULL;

\echo ''
\echo '-- 2. Outcome breakdown (generic)'
SELECT "Outcome", COUNT(*)
FROM maintenance."Iss1712DpcleanupActors_Candidates"
GROUP BY "Outcome"
ORDER BY "Outcome" NULLS FIRST;

\echo ''
\echo '-- 3. Job-specific invariants (each count must be 0)'
-- >>> JOB-SPECIFIC (edit me) >>>
-- 3a. Every 'updated' candidate's Actor now points at the resolved target
--     ActorName (the repoint held).
\echo '3a. updated candidates whose Actor is NOT at the target ActorName (expect 0):'
SELECT COUNT(*) AS updated_not_at_target
FROM maintenance."Iss1712DpcleanupActors_Candidates" c
JOIN public."Actor" a ON a."Id" = c."EntityId"
WHERE c."Outcome" = 'updated'
  AND a."ActorNameEntityId" IS DISTINCT FROM c."TargetActorNameEntityId";

-- 3b. Every candidate's Actor is still a PerformedByActor with ActorTypeId 1 and
--     a non-null ActorName -- we must not have changed type/discriminator.
\echo ''
\echo '3b. candidates whose Actor is no longer a PartyRepresentative PerformedByActor (expect 0):'
SELECT COUNT(*) AS unexpected_actor_shape
FROM maintenance."Iss1712DpcleanupActors_Candidates" c
JOIN public."Actor" a ON a."Id" = c."EntityId"
WHERE a."Discriminator" <> 'DialogActivityPerformedByActor'
   OR a."ActorTypeId"   <> 1
   OR a."ActorNameEntityId" IS NULL;

-- 3c. The target ActorName must be in a RECOGNISED Dialogporten actor scheme.
--     Org performers must be organization:identifier-no -- NOT the legacy
--     organizationnumber scheme (00a normalises it; the remediation script fixes
--     any earlier run). A hit here means a foreign-scheme row slipped through.
\echo ''
\echo '3c. updated candidates whose target ActorName urn is an unrecognised scheme (expect 0):'
SELECT COUNT(*) AS target_unexpected_scheme
FROM maintenance."Iss1712DpcleanupActors_Candidates" c
JOIN public."ActorName" an ON an."Id" = c."TargetActorNameEntityId"
WHERE c."Outcome" = 'updated'
  AND an."ActorId" NOT LIKE 'urn:altinn:person:identifier-no:%'
  AND an."ActorId" NOT LIKE 'urn:altinn:organization:identifier-no:%'
  AND an."ActorId" NOT LIKE 'urn:altinn:systemuser:uuid:%'
  AND an."ActorId" NOT LIKE 'urn:altinn:person:idporten-email%'
  AND an."ActorId" NOT LIKE 'urn:altinn:person:legacy-selfidentified%';

-- 3d. Informational: person vs organization performer split (not an assertion).
\echo ''
\echo '3d. (info) updated performer scheme breakdown:'
SELECT split_part(an."ActorId", ':', 3) AS scheme, COUNT(*) AS updated
FROM maintenance."Iss1712DpcleanupActors_Candidates" c
JOIN public."ActorName" an ON an."Id" = c."TargetActorNameEntityId"
WHERE c."Outcome" = 'updated'
GROUP BY 1
ORDER BY updated DESC;
-- <<< JOB-SPECIFIC <<<
