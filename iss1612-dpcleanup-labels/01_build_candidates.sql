-- iss1612-dpcleanup-labels step 01: build the candidate set.
--
-- Populates maintenance."Iss1612DpcleanupLabels_Candidates" with one row per entity that
-- needs repair. Safe to re-run -- ON CONFLICT DO NOTHING absorbs duplicates if the
-- script errors partway and is restarted.
--
-- Run the read-only 02_dry_run_count.sql FIRST to validate the predicate and
-- estimate scope before inserting anything.

-- >>> JOB-SPECIFIC (edit me) >>>
-- Build one candidate per (DialogEndUserContext, target label) from the staged
-- CSV. Prerequisites: 00a_load_csv.sql (staging) and resolve-actornames.sql +
-- 00b_ensure_actornames.sql (the resolution table) must have run first.
--
-- A staging row becomes a candidate only when ALL hold:
--   * its dialogid resolves to a DialogEndUserContext (via DialogId),
--   * its label maps to an in-scope target (Bin/Archive/MarkedAsUnopened;
--     Default/Sent/unknown are excluded),
--   * its actor resolved to an ActorName row (unresolved actors are skipped),
--   * the skip guard holds: for a folder label the context has no non-default
--     folder label yet; for any label that exact label is not already set.
-- When a dialog appears multiple times we keep the latest-timestamped row.
INSERT INTO maintenance."Iss1612DpcleanupLabels_Candidates"
    ("EntityId", "SystemLabelId", "ActorNameEntityId", "SourceTs", "LabelName")
SELECT DISTINCT ON (deuc."Id", lbl."SystemLabelId")
       deuc."Id",
       lbl."SystemLabelId",
       an."actor_name_entity_id",
       s."source_ts",
       'systemlabel:' || s."system_label"
FROM maintenance."Iss1612DpcleanupLabels_Staging" s
JOIN public."DialogEndUserContext" deuc
       ON deuc."DialogId" = s."dialog_id"
JOIN maintenance."Iss1612DpcleanupLabels_ActorNames" an
       ON an."actor_id" = s."actor_id"
      AND an."actor_name_entity_id" IS NOT NULL
JOIN (VALUES ('Bin', 2), ('Archive', 3), ('MarkedAsUnopened', 4))
       AS lbl("system_label", "SystemLabelId")
       ON lbl."system_label" = s."system_label"
WHERE
    -- folder-label guard: skip if a non-default folder label is already set
    (lbl."SystemLabelId" NOT IN (2, 3) OR NOT EXISTS (
        SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
        WHERE l."DialogEndUserContextId" = deuc."Id"
          AND l."SystemLabelId" IN (2, 3)
    ))
    -- this exact label not already set
    AND NOT EXISTS (
        SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
        WHERE l."DialogEndUserContextId" = deuc."Id"
          AND l."SystemLabelId" = lbl."SystemLabelId"
    )
ORDER BY deuc."Id", lbl."SystemLabelId", s."source_ts" DESC
ON CONFLICT ("EntityId", "SystemLabelId") DO NOTHING;
-- <<< JOB-SPECIFIC <<<

-- Report what was built (generic).
SELECT COUNT(*)                                       AS candidates_total,
       COUNT(*) FILTER (WHERE "ProcessedAt" IS NULL)  AS unprocessed
FROM maintenance."Iss1612DpcleanupLabels_Candidates";

-- >>> JOB-SPECIFIC (edit me) >>>
-- Left-behind report: staging rows that did NOT become candidates, by reason.
-- Review this before running the workers -- these dialogs are intentionally not
-- repaired and may need manual follow-up.
\echo ''
\echo '-- Staging rows excluded from the candidate set, by reason:'
SELECT
    CASE
        WHEN deuc."Id" IS NULL                               THEN 'no_matching_context'
        WHEN s."system_label" NOT IN
                ('Bin','Archive','MarkedAsUnopened')          THEN 'out_of_scope_label'
        WHEN an."actor_name_entity_id" IS NULL                THEN 'actor_unresolved'
        ELSE 'guard_skipped_or_already_set'
    END                                                       AS reason,
    COUNT(*)                                                  AS staging_rows
FROM maintenance."Iss1612DpcleanupLabels_Staging" s
LEFT JOIN public."DialogEndUserContext" deuc ON deuc."DialogId" = s."dialog_id"
LEFT JOIN maintenance."Iss1612DpcleanupLabels_ActorNames" an ON an."actor_id" = s."actor_id"
WHERE NOT EXISTS (
    -- exclude rows that DID produce a candidate
    SELECT 1 FROM maintenance."Iss1612DpcleanupLabels_Candidates" cand
    WHERE cand."EntityId" = deuc."Id"
      AND cand."LabelName" = 'systemlabel:' || s."system_label"
)
GROUP BY 1
ORDER BY 1;
-- <<< JOB-SPECIFIC <<<
