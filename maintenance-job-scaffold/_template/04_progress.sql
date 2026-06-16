-- {{JOB_KEBAB}}: at-a-glance progress. Safe to run repeatedly while workers are
-- running -- entirely read-only. Fully generic; no edits needed.

\echo '-- Overall progress + ETA'
SELECT
    p.total,
    p.processed,
    p.remaining,
    p.updated,
    p.skipped,
    ROUND(100.0 * p.processed / NULLIF(p.total, 0), 2) AS pct_done,
    rate.rows_per_sec,
    CASE WHEN rate.rows_per_sec > 0
         THEN (p.remaining / rate.rows_per_sec) * interval '1 second'
    END AS eta
FROM maintenance."{{JOB_PASCAL}}_Progress" p
CROSS JOIN LATERAL (
    -- Overall rate since the first batch started (lifetime average).
    SELECT SUM("Claimed")::numeric
           / NULLIF(EXTRACT(EPOCH FROM (MAX("FinishedAt") - MIN("StartedAt"))), 0)
           AS rows_per_sec
    FROM maintenance."{{JOB_PASCAL}}_BatchLog"
    WHERE "FinishedAt" IS NOT NULL
) rate;

\echo ''
\echo '-- Recent throughput (last 15 min of finished batches)'
\echo '-- More honest than the lifetime average if the run was paused/throttled.'
SELECT
    COUNT(*)                                                       AS batches,
    SUM("Claimed")                                                 AS claimed,
    SUM("Updated")                                                 AS updated,
    ROUND(
        SUM("Claimed")::numeric
        / NULLIF(EXTRACT(EPOCH FROM (MAX("FinishedAt") - MIN("StartedAt"))), 0),
        2
    )                                                              AS rows_per_sec
FROM maintenance."{{JOB_PASCAL}}_BatchLog"
WHERE "FinishedAt" >= now() - interval '15 minutes';

\echo ''
\echo '-- Per-worker throughput'
SELECT
    "WorkerId",
    COUNT(*)                                                       AS batches,
    MIN("StartedAt")                                               AS first_batch_at,
    MAX(COALESCE("FinishedAt", "StartedAt"))                       AS last_batch_at,
    SUM("Updated")                                                 AS updated_total,
    SUM("Skipped")                                                 AS skipped_total,
    ROUND(AVG(EXTRACT(EPOCH FROM ("FinishedAt" - "StartedAt"))), 3) AS avg_batch_secs,
    ROUND(
        SUM("Updated")::numeric
        / NULLIF(EXTRACT(EPOCH FROM (MAX("FinishedAt") - MIN("StartedAt"))), 0),
        2
    )                                                              AS updates_per_sec
FROM maintenance."{{JOB_PASCAL}}_BatchLog"
GROUP BY "WorkerId"
ORDER BY "WorkerId";

\echo ''
\echo '-- Live workers (pg_stat_activity)'
\echo '-- repair.sh sets application_name = maint-{{JOB_KEBAB}}-<WORKER_ID>.'
SELECT
    application_name,
    state,
    wait_event_type,
    wait_event,
    now() - query_start AS query_age,
    now() - state_change AS since_state_change
FROM pg_stat_activity
WHERE application_name LIKE 'maint-{{JOB_KEBAB}}-%'
ORDER BY application_name;
