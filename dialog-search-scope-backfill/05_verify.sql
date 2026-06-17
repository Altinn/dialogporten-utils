-- dialog-search-scope-backfill verification: run after the workers report scanned = 0 (range
-- exhausted), BEFORE 99_cleanup.sql.

\echo '-- 1. Rows still needing backfill (must be 0). Full scan -- minutes at prod scale.'
SELECT count(*) AS remaining
FROM search."DialogSearch" ds
WHERE ds."ContentUpdatedAt" IS NULL OR ds."ServiceResource" IS NULL;

\echo ''
\echo '-- 2. Checkpoint summary (per worker).'
SELECT "WorkerId", "LastDialogId", "UpdatedTotal", "ScannedTotal", "Batches", "StartedAt", "UpdatedAt"
FROM maintenance."DialogSearchScopeBackfill_Checkpoint"
ORDER BY "WorkerId";

\echo ''
\echo '-- 3. Spot-check on a small sample that populated values match the source Dialog (expect 0).'
\echo '--    Exact equality can show transient false positives from async reindex lag, so this is'
\echo '--    a sampled, ServiceResource-only structural check (the value that must be prefix-stripped).'
SELECT count(*) AS sampled_serviceresource_mismatches
FROM search."DialogSearch" ds TABLESAMPLE SYSTEM (0.01)
JOIN public."Dialog" d ON d."Id" = ds."DialogId"
WHERE ds."ServiceResource" IS DISTINCT FROM replace(d."ServiceResource", 'urn:altinn:resource:', '');
