-- dialog-search-scope-backfill: count rows still needing backfill (ContentUpdatedAt or
-- ServiceResource still NULL). Run before the job to estimate scope; on the first run this is
-- ~the entire search."DialogSearch" (every pre-existing row). NOTE: this is a full scan over the
-- whole table and takes minutes at production scale -- during the run prefer 04_progress.sql
-- (cheap, reads the checkpoint) and use this only for an occasional exact remaining count.
SELECT count(*) AS remaining_to_backfill
FROM search."DialogSearch" ds
WHERE ds."ContentUpdatedAt" IS NULL OR ds."ServiceResource" IS NULL;
