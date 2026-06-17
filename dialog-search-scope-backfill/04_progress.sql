-- dialog-search-scope-backfill progress. Cheap and read-only: reads the checkpoint, NOT the
-- 1.1B-row table. Safe to run repeatedly while workers run.

\echo '-- Per-worker checkpoint (cursor position, totals, throughput)'
SELECT "WorkerId",
       "LastDialogId",
       "UpdatedTotal",
       "ScannedTotal",
       "Batches",
       "StartedAt",
       "UpdatedAt",
       round("ScannedTotal" / GREATEST(extract(epoch FROM (now() - "StartedAt")), 1))::bigint
         AS scanned_per_sec
FROM maintenance."DialogSearchScopeBackfill_Checkpoint"
ORDER BY "WorkerId";

\echo ''
\echo '-- Live workers (pg_stat_activity); repair.sh sets application_name = maint-dialog-search-scope-backfill-<WORKER_ID>'
SELECT application_name,
       state,
       wait_event_type,
       wait_event,
       now() - query_start AS query_age,
       now() - state_change AS since_state_change
FROM pg_stat_activity
WHERE application_name LIKE 'maint-dialog-search-scope-backfill-%'
ORDER BY application_name;
