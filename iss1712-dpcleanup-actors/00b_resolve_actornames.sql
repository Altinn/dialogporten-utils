-- iss1712-dpcleanup-actors step 00b: resolve each (actor urn, name) from staging
-- to the public."ActorName" row the PerformedBy actor should point at.
--
-- Collision-safe w.r.t. the pending dialogporten#3171 reorder repair. Person
-- names are currently stored LAST FIRST MIDDLE; the CSV uses the same order;
-- #3171 will later reorder EVERY urn:altinn:person:% row in place (first word ->
-- end) with no (ActorId, Name) collision handling. To avoid adding rows that
-- would collide under that reorder, we resolve in three tiers and only ever
-- insert when the person has NO matching row at all:
--   tier 1 'existing_exact'     -- reuse a row matching the CSV name as-is
--   tier 2 'existing_reordered' -- reuse a row already in the #3171-corrected order
--   tier 3 'inserted'           -- create one, in CSV (LAST FIRST MIDDLE) order
--
-- Idempotent: re-running is safe (each tier only fills rows still NULL).
-- Prerequisite: 00_setup.sql and 00a-load-csv.sh must have run first.
--
-- The reorder(name) expression below MUST match #3171's transform: move the
-- first space-delimited word to the end ("LAST FIRST MIDDLE" -> "FIRST MIDDLE LAST").

-- One row per distinct (actor urn, name) seen in staging.
INSERT INTO maintenance."Iss1712DpcleanupActors_ActorNames" ("actor_id", "name")
SELECT DISTINCT "actor_id", "actor_name"
FROM maintenance."Iss1712DpcleanupActors_Staging"
ON CONFLICT ("actor_id", "name") DO NOTHING;

-- Tier 1: reuse an existing ActorName matching the CSV name EXACTLY.
UPDATE maintenance."Iss1712DpcleanupActors_ActorNames" r
SET "actor_name_entity_id" = an."Id",
    "source"               = 'existing_exact'
FROM public."ActorName" an
WHERE an."ActorId" = r."actor_id"
  AND an."Name"    = r."name"
  AND r."actor_name_entity_id" IS NULL;

-- Tier 2: reuse an existing ActorName already in the #3171-REORDERED order.
UPDATE maintenance."Iss1712DpcleanupActors_ActorNames" r
SET "actor_name_entity_id" = an."Id",
    "source"               = 'existing_reordered'
FROM public."ActorName" an
WHERE an."ActorId" = r."actor_id"
  AND an."Name"    = CASE WHEN trim(r."name") LIKE '% %'
                          THEN substring(trim(r."name") FROM position(' ' IN trim(r."name")) + 1)
                               || ' ' || left(trim(r."name"), position(' ' IN trim(r."name")) - 1)
                          ELSE trim(r."name") END
  AND r."actor_name_entity_id" IS NULL;

-- Tier 3: no matching row in either ordering -> create one, in CSV order
-- (LAST FIRST MIDDLE), so #3171 will normalise it along with the cohort. UUIDv7
-- like the app. The unique (ActorId, Name) index dedupes against concurrent
-- inserts; the follow-up link covers any row that already existed under that key.
INSERT INTO public."ActorName" ("Id", "ActorId", "Name", "CreatedAt")
SELECT uuidv7(), r."actor_id", r."name", now()
FROM maintenance."Iss1712DpcleanupActors_ActorNames" r
WHERE r."actor_name_entity_id" IS NULL
ON CONFLICT ("ActorId", "Name") DO NOTHING;

UPDATE maintenance."Iss1712DpcleanupActors_ActorNames" r
SET "actor_name_entity_id" = an."Id",
    "source"               = 'inserted'
FROM public."ActorName" an
WHERE an."ActorId" = r."actor_id"
  AND an."Name"    = r."name"
  AND r."actor_name_entity_id" IS NULL;

-- Report resolution breakdown. Every row must now have an id (no NULLs left);
-- 'inserted' should be the minority (most performers already have an ActorName).
\echo ''
\echo '-- Resolution source breakdown (expect 0 unresolved):'
SELECT COALESCE("source", 'UNRESOLVED') AS source, COUNT(*) AS actors
FROM maintenance."Iss1712DpcleanupActors_ActorNames"
GROUP BY "source"
ORDER BY actors DESC;
