-- iss1712-dpcleanup-actors sanity check: confirm the candidate-build scan uses the plan you
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
-- EXPLAIN the candidate-selection query from 01_build_candidates.sql (the SELECT,
-- not the INSERT). Expect: a scan of the staging table, then index lookups into
-- DialogActivity (PK_DialogActivity on "Id") and Actor (IX_Actor_ActivityId) --
-- NOT a seq scan of those large tables.
EXPLAIN
WITH matched AS (
    SELECT a."Id" AS activity_id, s."actor_id", s."actor_name"
    FROM maintenance."Iss1712DpcleanupActors_Staging" s
    JOIN public."DialogActivity" a
           ON a."Id"        = s."dialog_activity_id"
          AND a."DialogId"  = s."dialog_id"
          AND a."CreatedAt" = s."source_ts"
          AND a."TypeId"    = CASE s."activity_type"
                                  WHEN 'CorrespondenceOpened'   THEN 16
                                  WHEN 'CorrespondenceConfirmed' THEN 17
                              END
),
unambiguous AS (
    SELECT activity_id, min("actor_id") AS actor_id, min("actor_name") AS actor_name
    FROM matched GROUP BY activity_id
    HAVING COUNT(DISTINCT ("actor_id", "actor_name")) = 1
)
SELECT act."Id", an."actor_name_entity_id"
FROM unambiguous u
JOIN public."Actor" act
       ON act."ActivityId"    = u.activity_id
      AND act."Discriminator" = 'DialogActivityPerformedByActor'
JOIN maintenance."Iss1712DpcleanupActors_ActorNames" an
       ON an."actor_id" = u.actor_id
      AND an."name"     = u.actor_name
      AND an."actor_name_entity_id" IS NOT NULL
WHERE act."ActorNameEntityId" IS DISTINCT FROM an."actor_name_entity_id";
-- <<< JOB-SPECIFIC <<<
