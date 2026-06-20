-- iss1612-dpcleanup-labels step 00b: ensure an ActorName row exists for every
-- brreg-resolved actor, then link its id into the resolution table.
--
-- 'existing' actors already carry actor_name_entity_id (resolve-actornames.sh
-- reused a present row). 'brreg' actors have a name but no row yet -- create it
-- (UUIDv7 id, like the app), reusing any matching (ActorId, Name) row if one
-- happens to exist. 'unresolved' actors are left with a NULL id and are skipped
-- by 01_build_candidates.sql.
--
-- Idempotent: ON CONFLICT DO NOTHING + a re-derivable id linkage. Run AFTER
-- resolve-actornames.sql.

-- 1. Create missing ActorName rows for brreg-resolved actors.
--    The unique (ActorId, Name) index (nulls-not-distinct) dedupes against any
--    pre-existing row, matching PopulateActorNameInterceptor's behaviour.
INSERT INTO public."ActorName" ("Id", "ActorId", "Name", "CreatedAt")
SELECT uuidv7(), r."actor_id", r."name", now()
FROM maintenance."Iss1612DpcleanupLabels_ActorNames" r
WHERE r."source" = 'brreg'
  AND r."name" IS NOT NULL
ON CONFLICT ("ActorId", "Name") DO NOTHING;

-- 2. Link every resolved row to its ActorName id (covers both the rows just
--    inserted and any that already existed under the same ActorId+Name).
UPDATE maintenance."Iss1612DpcleanupLabels_ActorNames" r
SET "actor_name_entity_id" = an."Id"
FROM public."ActorName" an
WHERE an."ActorId" = r."actor_id"
  AND an."Name"    = r."name"
  AND r."name" IS NOT NULL
  AND r."actor_name_entity_id" IS NULL;

-- 3. Report: every resolved actor should now have an id; unresolved should not.
\echo ''
\echo '-- Resolution state after linking (expect actor_name_entity_id set for existing/brreg):'
SELECT "source",
       COUNT(*)                                              AS actors,
       COUNT(*) FILTER (WHERE "actor_name_entity_id" IS NOT NULL) AS with_id
FROM maintenance."Iss1612DpcleanupLabels_ActorNames"
GROUP BY "source"
ORDER BY "source";
