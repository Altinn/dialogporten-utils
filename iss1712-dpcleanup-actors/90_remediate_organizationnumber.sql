-- iss1712-dpcleanup-actors REMEDIATION: fix the malformed org performer urns that
-- the first ISS-1712 run introduced.
--
-- WHAT HAPPENED: the CSV expresses organization performers in Correspondence's
-- legacy scheme urn:altinn:organizationnumber:<orgno>. The original 00b resolved
-- the CSV ActorId verbatim, so tier 3 INSERTed ActorName rows in that foreign
-- scheme (Dialogporten's canonical org scheme is urn:altinn:organization:identifier-no),
-- and ~630k PerformedBy actors were pointed at them. There were ZERO
-- organizationnumber rows before this job, so every such row is ours to clean up.
--
-- WHAT THIS DOES, per malformed urn:altinn:organizationnumber:<orgno> ActorName row:
--   * find the canonical urn:altinn:organization:identifier-no:<orgno> row with the
--     SAME name; if none exists (the org was renamed and the CSV carries the
--     historical name), CREATE it -- backdated to the earliest event that used that
--     (org, name), since the CSV name is the name as of the event, not today;
--   * repoint every Actor (and the candidate/resolution bookkeeping) from the
--     malformed row to the canonical one;
--   * delete the now-unreferenced malformed rows.
--
-- Person performers were inserted in the correct scheme and are NOT touched.
-- Run with the WRITABLE repair role, BEFORE loading ISS-1951. Wrapped in a
-- transaction with a safety assert -- it aborts (rolls back) if anything is off.

\set ON_ERROR_STOP on
BEGIN;

-- 1. Every malformed row (scheme alone identifies them -- the table had none before us).
CREATE TEMP TABLE orgnum_bad ON COMMIT DROP AS
SELECT an."Id"                          AS bad_id,
       an."Name"                        AS nm,
       split_part(an."ActorId", ':', 4) AS orgno
FROM public."ActorName" an
WHERE an."ActorId" LIKE 'urn:altinn:organizationnumber:%';

-- 2. Create the canonical rows that don't exist yet (the renamed-org cases),
--    backdated to the earliest event that used this (org, name).
INSERT INTO public."ActorName" ("Id", "ActorId", "Name", "CreatedAt")
SELECT uuidv7(),
       'urn:altinn:organization:identifier-no:' || b.orgno,
       b.nm,
       COALESCE(ev.min_ts, now())
FROM orgnum_bad b
JOIN LATERAL (
    SELECT min(a."CreatedAt") AS min_ts
    FROM public."Actor" act
    JOIN public."DialogActivity" a ON a."Id" = act."ActivityId"
    WHERE act."ActorNameEntityId" = b.bad_id
) ev ON true
WHERE NOT EXISTS (
    SELECT 1 FROM public."ActorName" g
    WHERE g."ActorId" = 'urn:altinn:organization:identifier-no:' || b.orgno
      AND g."Name"    = b.nm)
ON CONFLICT ("ActorId", "Name") DO NOTHING;

-- 3. Map each malformed row -> the canonical same-name row (now exists for all).
CREATE TEMP TABLE orgnum_remap ON COMMIT DROP AS
SELECT b.bad_id, g."Id" AS good_id
FROM orgnum_bad b
JOIN public."ActorName" g
  ON g."ActorId" = 'urn:altinn:organization:identifier-no:' || b.orgno
 AND g."Name"    = b.nm;

-- 4. Safety: every malformed row must map to exactly one canonical row, or abort.
DO $$
DECLARE n_bad bigint; n_map bigint; n_null bigint;
BEGIN
    SELECT count(*) INTO n_bad  FROM orgnum_bad;
    SELECT count(*) INTO n_map  FROM orgnum_remap;
    SELECT count(*) INTO n_null FROM orgnum_remap WHERE good_id IS NULL;
    IF n_map <> n_bad OR n_null > 0 THEN
        RAISE EXCEPTION 'remap mismatch: bad=%, mapped=%, null_good=% -- aborting', n_bad, n_map, n_null;
    END IF;
    RAISE NOTICE 'remap OK: % malformed rows -> canonical rows', n_bad;
END $$;

-- 5. Repoint the affected actors.
UPDATE public."Actor" a
SET "ActorNameEntityId" = r.good_id
FROM orgnum_remap r
WHERE a."ActorNameEntityId" = r.bad_id;

-- 6. Keep the maintenance bookkeeping consistent (so 05_verify still passes).
UPDATE maintenance."Iss1712DpcleanupActors_Candidates" c
SET "TargetActorNameEntityId" = r.good_id
FROM orgnum_remap r
WHERE c."TargetActorNameEntityId" = r.bad_id;

UPDATE maintenance."Iss1712DpcleanupActors_ActorNames" rn
SET "actor_name_entity_id" = r.good_id
FROM orgnum_remap r
WHERE rn."actor_name_entity_id" = r.bad_id;

-- 7. Delete the malformed rows (now unreferenced by any Actor).
DELETE FROM public."ActorName" bad
USING orgnum_remap r
WHERE bad."Id" = r.bad_id
  AND NOT EXISTS (SELECT 1 FROM public."Actor" a WHERE a."ActorNameEntityId" = bad."Id");

-- 8. Report: zero malformed rows should remain.
\echo ''
\echo '-- organizationnumber rows remaining (expect 0):'
SELECT COUNT(*) AS organizationnumber_remaining
FROM public."ActorName"
WHERE "ActorId" LIKE 'urn:altinn:organizationnumber:%';

COMMIT;
