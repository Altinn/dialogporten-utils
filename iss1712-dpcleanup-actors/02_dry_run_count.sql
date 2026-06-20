-- iss1712-dpcleanup-actors dry-run: read-only count of the candidates that
-- 01_build_candidates.sql would insert. Use this to validate the predicate and
-- estimate scope BEFORE running the actual build.
--
-- IMPORTANT: this must mirror the access pattern of 01_build_candidates.sql.
-- If the build uses a per-resource MATERIALIZED CTE loop to stay on an index
-- plan, this dry run must use the same shape -- otherwise it validates the
-- scope but not the plan, and the real build may behave very differently.

-- >>> JOB-SPECIFIC (edit me) >>>
-- SCALE NOTE: run this whole file as-is for ISS-1712 (6.36M rows). For ISS-1951
-- (~140M rows) it is expensive -- the breakdown scans all staging and double-probes
-- DialogActivity. To sample instead, swap each
--     FROM maintenance."Iss1712DpcleanupActors_Staging" s
-- for a TABLESAMPLE (the drift/ambiguity *ratios* are what matter), e.g.
--     FROM maintenance."Iss1712DpcleanupActors_Staging" TABLESAMPLE SYSTEM (1) s
-- and read the breakdown as proportions. (Don't sample by a dialog_activity_id
-- prefix: these are UUIDv7, so essentially every id starts 0x01 and a prefix range
-- is not a representative sample.) Otherwise rely on 01's per-chunk RAISE NOTICE
-- counts for the candidate total.
--
-- Headline: candidates that 01_build_candidates.sql would insert -- mirrors its
-- access pattern exactly (matched -> unambiguous -> PerformedBy actor -> resolved
-- -> not already-correct).
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
SELECT COUNT(*) AS candidates_that_would_be_inserted
FROM unambiguous u
JOIN public."Actor" act
       ON act."ActivityId"    = u.activity_id
      AND act."Discriminator" = 'DialogActivityPerformedByActor'
JOIN maintenance."Iss1712DpcleanupActors_ActorNames" an
       ON an."actor_id" = u.actor_id
      AND an."name"     = u.actor_name
      AND an."actor_name_entity_id" IS NOT NULL
WHERE act."ActorNameEntityId" IS DISTINCT FROM an."actor_name_entity_id";

-- Left-behind / scope breakdown: classify EVERY staging row by why it does or
-- does not become a candidate. Review before building. In particular
-- '2_timestamp_mismatch' is the drift check (CSV Timestamp vs activity CreatedAt)
-- and '6_ambiguous_multiple_actors' are activities with conflicting CSV actors,
-- intentionally left for manual follow-up.
\echo ''
\echo '-- Staging rows by classification (0_candidate = would be repaired):'
WITH classified AS (
    SELECT s."dialog_id", s."dialog_activity_id", s."source_ts",
           s."actor_id", s."actor_name",
           CASE s."activity_type"
               WHEN 'CorrespondenceOpened'   THEN 16
               WHEN 'CorrespondenceConfirmed' THEN 17
           END                                   AS expected_type,
           a."Id"        AS a_id,
           a."DialogId"  AS a_dialog,
           a."CreatedAt" AS a_created,
           a."TypeId"    AS a_type
    FROM maintenance."Iss1712DpcleanupActors_Staging" s
    LEFT JOIN public."DialogActivity" a ON a."Id" = s."dialog_activity_id"
),
matched AS (
    SELECT * FROM classified
    WHERE a_id IS NOT NULL
      AND a_created = "source_ts"
      AND a_dialog  = "dialog_id"
      AND a_type    = expected_type
),
ambiguity AS (
    SELECT "dialog_activity_id",
           COUNT(DISTINCT ("actor_id", "actor_name")) AS distinct_actors
    FROM matched GROUP BY "dialog_activity_id"
)
SELECT reason, COUNT(*) AS staging_rows
FROM (
    SELECT CASE
        WHEN c.a_id IS NULL                              THEN '1_no_matching_activity_id'
        WHEN c.a_created IS DISTINCT FROM c."source_ts"  THEN '2_timestamp_mismatch'
        WHEN c.a_dialog  IS DISTINCT FROM c."dialog_id"  THEN '3_dialog_id_mismatch'
        WHEN c.expected_type IS NULL                     THEN '4_unknown_activity_type'
        WHEN c.a_type IS DISTINCT FROM c.expected_type   THEN '5_type_mismatch'
        WHEN amb.distinct_actors > 1                     THEN '6_ambiguous_multiple_actors'
        WHEN act."Id" IS NULL                            THEN '7_no_performed_by_actor'
        WHEN an."actor_name_entity_id" IS NULL           THEN '8_actor_unresolved'
        WHEN act."ActorNameEntityId" IS NOT DISTINCT FROM an."actor_name_entity_id"
                                                         THEN '9_already_correct'
        ELSE '0_candidate'
    END AS reason
    FROM classified c
    LEFT JOIN ambiguity amb ON amb."dialog_activity_id" = c."dialog_activity_id"
    LEFT JOIN public."Actor" act
           ON act."ActivityId"    = c.a_id
          AND act."Discriminator" = 'DialogActivityPerformedByActor'
    LEFT JOIN maintenance."Iss1712DpcleanupActors_ActorNames" an
           ON an."actor_id" = c."actor_id" AND an."name" = c."actor_name"
) z
GROUP BY reason
ORDER BY reason;
-- <<< JOB-SPECIFIC <<<
