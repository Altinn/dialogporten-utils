-- iss1712-dpcleanup-actors step 00b: resolve each (actor urn, name) from staging
-- to the public."ActorName" row the PerformedBy actor should point at.
--
-- Collision-safe w.r.t. the pending dialogporten#3171 reorder repair. Person
-- names are currently stored LAST FIRST MIDDLE; the CSV uses the same order;
-- #3171 will later reorder EVERY urn:altinn:person:% row in place (first word ->
-- end) with no (ActorId, Name) collision handling. To avoid adding rows that
-- would collide under that reorder, we resolve in three tiers and only ever
-- insert when there is NO matching row at all:
--   tier 1 'existing_exact'     -- reuse a row matching the CSV name as-is
--   tier 2 'existing_reordered' -- (persons only) reuse a row already in the
--                                  #3171-corrected order
--   tier 3 'inserted'           -- recognised schemes only; create a row, backdated
--                                  to the earliest event that used (actor, name)
-- Unrecognised actor schemes are left UNRESOLVED and excluded from the repair
-- (never inserted in a foreign format).
--
-- Idempotent: re-running is safe (each tier only fills rows still NULL).
-- Prerequisite: 00_setup.sql and 00a-load-csv.sh (which normalises legacy org urns
-- to organization:identifier-no) must have run first.
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
-- Restricted to PERSON urns: #3171 only reorders urn:altinn:person:% rows, so the
-- reorder transform is meaningless for organizations (and would wrongly permute an
-- org name when searching for a match).
UPDATE maintenance."Iss1712DpcleanupActors_ActorNames" r
SET "actor_name_entity_id" = an."Id",
    "source"               = 'existing_reordered'
FROM public."ActorName" an
WHERE an."ActorId" = r."actor_id"
  AND r."actor_id" LIKE 'urn:altinn:person:%'
  AND an."Name"    = CASE WHEN trim(r."name") LIKE '% %'
                          THEN substring(trim(r."name") FROM position(' ' IN trim(r."name")) + 1)
                               || ' ' || left(trim(r."name"), position(' ' IN trim(r."name")) - 1)
                          ELSE trim(r."name") END
  AND r."actor_name_entity_id" IS NULL;

-- Tier 3: no matching row in either ordering -> create one. Guarded to RECOGNISED
-- actor schemes only -- after 00a normalisation orgs are organization:identifier-no,
-- so anything still in an unrecognised scheme is left UNRESOLVED (reported below)
-- and excluded from candidates, rather than inserted in a foreign format (this is
-- what previously created the bad urn:altinn:organizationnumber rows).
-- Person names are inserted in CSV order (LAST FIRST MIDDLE) so #3171 normalises
-- them with the cohort. CreatedAt is BACKDATED to the earliest event that used this
-- (actor, name): names change over time and the CSV carries the name as of the
-- event, so the row reflects when that name was in use -- not now(). UUIDv7 id like
-- the app; the unique (ActorId, Name) index dedupes against concurrent inserts.
INSERT INTO public."ActorName" ("Id", "ActorId", "Name", "CreatedAt")
SELECT uuidv7(), r."actor_id", r."name", COALESCE(ts.min_ts, now())
FROM maintenance."Iss1712DpcleanupActors_ActorNames" r
JOIN LATERAL (
    SELECT min(s."source_ts") AS min_ts
    FROM maintenance."Iss1712DpcleanupActors_Staging" s
    WHERE s."actor_id" = r."actor_id" AND s."actor_name" = r."name"
) ts ON true
WHERE r."actor_name_entity_id" IS NULL
  AND ( r."actor_id" LIKE 'urn:altinn:person:identifier-no:%'
     OR r."actor_id" LIKE 'urn:altinn:organization:identifier-no:%'
     OR r."actor_id" LIKE 'urn:altinn:systemuser:uuid:%'
     OR r."actor_id" LIKE 'urn:altinn:person:idporten-email%'
     OR r."actor_id" LIKE 'urn:altinn:person:legacy-selfidentified%' )
ON CONFLICT ("ActorId", "Name") DO NOTHING;

UPDATE maintenance."Iss1712DpcleanupActors_ActorNames" r
SET "actor_name_entity_id" = an."Id",
    "source"               = 'inserted'
FROM public."ActorName" an
WHERE an."ActorId" = r."actor_id"
  AND an."Name"    = r."name"
  AND r."actor_name_entity_id" IS NULL;

-- Report resolution breakdown. 'inserted' should be the minority; rows left
-- UNRESOLVED are unrecognised-scheme actors that are intentionally excluded.
\echo ''
\echo '-- Resolution source breakdown:'
SELECT COALESCE("source", 'UNRESOLVED') AS source, COUNT(*) AS actors
FROM maintenance."Iss1712DpcleanupActors_ActorNames"
GROUP BY "source"
ORDER BY actors DESC;

-- Surface exactly which schemes (if any) were left unresolved, for manual review.
\echo ''
\echo '-- Unresolved actor schemes (excluded from the repair; expect none/known noise):'
SELECT split_part("actor_id", ':', 1) || ':' || split_part("actor_id", ':', 2)
         || ':' || split_part("actor_id", ':', 3) AS scheme,
       COUNT(*) AS actors
FROM maintenance."Iss1712DpcleanupActors_ActorNames"
WHERE "actor_name_entity_id" IS NULL
GROUP BY 1
ORDER BY actors DESC;
