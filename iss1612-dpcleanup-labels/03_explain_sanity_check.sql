-- iss1612-dpcleanup-labels sanity check: confirm the candidate-build scan uses the plan you
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
-- EXPLAIN the candidate-selection join from 01_build_candidates.sql. Confirm the
-- staging -> DialogEndUserContext join rides the unique index on "DialogId"
-- (IX_DialogEndUserContext_DialogId_IncludeId), and that the
-- DialogEndUserContextSystemLabel guard lookups are index scans -- not seq scans
-- over the large dialog/label tables. (Staging is small, so a seq scan of staging
-- itself is fine.)
EXPLAIN
SELECT deuc."Id", lbl."SystemLabelId"
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
      );
-- <<< JOB-SPECIFIC <<<
