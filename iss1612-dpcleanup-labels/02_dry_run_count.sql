-- iss1612-dpcleanup-labels dry-run: read-only count of the candidates that
-- 01_build_candidates.sql would insert. Use this to validate the predicate and
-- estimate scope BEFORE running the actual build.
--
-- IMPORTANT: this must mirror the access pattern of 01_build_candidates.sql.
-- If the build uses a per-resource MATERIALIZED CTE loop to stay on an index
-- plan, this dry run must use the same shape -- otherwise it validates the
-- scope but not the plan, and the real build may behave very differently.

-- >>> JOB-SPECIFIC (edit me) >>>
-- Mirrors 01_build_candidates.sql: count the distinct (context, label) pairs that
-- would be inserted. Requires 00a_load_csv.sql + resolve-actornames.sql +
-- 00b_ensure_actornames.sql to have run. Compare against the CSV line count.
SELECT COUNT(*) AS candidates_that_would_be_inserted
FROM (
    SELECT DISTINCT deuc."Id", lbl."SystemLabelId"
    FROM maintenance."Iss1612DpcleanupLabels_Staging" s
    JOIN public."DialogEndUserContext" deuc
           ON deuc."DialogId" = s."dialog_id"
    JOIN maintenance."Iss1612DpcleanupLabels_ActorNames" an
           ON an."actor_id" = s."actor_id"
          AND an."actor_name_entity_id" IS NOT NULL
    JOIN (VALUES ('Bin', 2), ('Archive', 3), ('MarkedAsUnopened', 4))
           AS lbl("system_label", "SystemLabelId")
           ON lbl."system_label" = s."system_label"
    WHERE (lbl."SystemLabelId" NOT IN (2, 3) OR NOT EXISTS (
              SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
              WHERE l."DialogEndUserContextId" = deuc."Id"
                AND l."SystemLabelId" IN (2, 3)
          ))
      AND NOT EXISTS (
              SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
              WHERE l."DialogEndUserContextId" = deuc."Id"
                AND l."SystemLabelId" = lbl."SystemLabelId"
          )
) q;
-- <<< JOB-SPECIFIC <<<
