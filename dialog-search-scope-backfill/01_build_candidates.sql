-- dialog-search-scope-backfill step 01: build the candidate set.
--
-- Populates maintenance."DialogSearchScopeBackfill_Candidates" with one row per entity that
-- needs repair. Safe to re-run -- ON CONFLICT DO NOTHING absorbs duplicates if the
-- script errors partway and is restarted.
--
-- Run the read-only 02_dry_run_count.sql FIRST to validate the predicate and
-- estimate scope before inserting anything.

-- >>> JOB-SPECIFIC (edit me) >>>
-- Scope: every DialogSearch row not yet backfilled (ContentUpdatedAt or ServiceResource still NULL).
-- On the first run right after the additive migration this is the WHOLE table, so a full sequential
-- scan here is expected and correct (not the usual selective-repair index scan).
INSERT INTO maintenance."DialogSearchScopeBackfill_Candidates" ("EntityId")
SELECT ds."DialogId"
FROM search."DialogSearch" ds
WHERE ds."ContentUpdatedAt" IS NULL OR ds."ServiceResource" IS NULL
ON CONFLICT ("EntityId") DO NOTHING;
-- <<< JOB-SPECIFIC <<<

-- Report what was built (generic).
SELECT COUNT(*)                                       AS candidates_total,
       COUNT(*) FILTER (WHERE "ProcessedAt" IS NULL)  AS unprocessed
FROM maintenance."DialogSearchScopeBackfill_Candidates";
