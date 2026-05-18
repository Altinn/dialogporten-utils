\set ON_ERROR_STOP on

\if :{?repair_name}
\else
\set repair_name has_unopened_content_repair_2026_02_25_2026_03_20
\endif

SELECT *
FROM maintenance.has_unopened_content_repair_summary
WHERE repair_name = :'repair_name';

SELECT skipped_reason, COUNT(*) AS count
FROM maintenance.has_unopened_content_repair_candidate
WHERE repair_name = :'repair_name'
  AND processed_at IS NOT NULL
GROUP BY skipped_reason
ORDER BY skipped_reason NULLS FIRST;

SELECT COUNT(*) AS unprocessed_candidates
FROM maintenance.has_unopened_content_repair_candidate
WHERE repair_name = :'repair_name'
  AND processed_at IS NULL;

SELECT COUNT(*) AS processed_candidates_still_repairable
FROM maintenance.has_unopened_content_repair_candidate c
JOIN "Dialog" d ON d."Id" = c.dialog_id
WHERE c.repair_name = :'repair_name'
  AND c.processed_at IS NOT NULL
  AND d."HasUnopenedContent" = true
  AND NOT maintenance.has_unopened_content_repair_has_unopened_content(d."Id");

SELECT c.dialog_id
FROM maintenance.has_unopened_content_repair_candidate c
JOIN "Dialog" d ON d."Id" = c.dialog_id
WHERE c.repair_name = :'repair_name'
  AND c.processed_at IS NOT NULL
  AND d."HasUnopenedContent" = true
  AND NOT maintenance.has_unopened_content_repair_has_unopened_content(d."Id")
ORDER BY c.processed_at, c.dialog_id
LIMIT 100;

SELECT phase,
       COUNT(*) AS batches,
       SUM(scanned_activities) AS scanned_activities,
       SUM(candidates_touched) AS candidates_touched,
       SUM(rows_selected) AS rows_selected,
       SUM(rows_updated) AS rows_updated,
       ROUND(SUM(batch_seconds), 3) AS total_seconds,
       ROUND(AVG(batch_seconds), 3) AS avg_batch_seconds
FROM maintenance.has_unopened_content_repair_batch_log
WHERE repair_name = :'repair_name'
GROUP BY phase
ORDER BY phase;
