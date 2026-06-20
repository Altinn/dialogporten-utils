-- iss1712-dpcleanup-actors cleanup: drop all objects created by this repair.
--
-- Run only AFTER 05_verify.sql passes:
--   - unprocessed_remaining = 0
--   - the job-specific invariant returns 0
--   - any batch-log retention you want has been exported (see below)
--
-- Export the batch log first if you want an audit trail -- 99 drops it:
--   psql -X -c "COPY (SELECT * FROM maintenance.\"Iss1712DpcleanupActors_BatchLog\" \
--       ORDER BY \"BatchId\") TO STDOUT WITH CSV HEADER" > iss1712-dpcleanup-actors-batchlog.csv
--
-- The maintenance schema itself is preserved -- other jobs may live there.

DROP PROCEDURE IF EXISTS maintenance.iss1712dpcleanupactors_run_batch(integer, text, integer, integer, integer);
DROP VIEW      IF EXISTS maintenance."Iss1712DpcleanupActors_Progress";
DROP TABLE     IF EXISTS maintenance."Iss1712DpcleanupActors_BatchLog";
DROP TABLE     IF EXISTS maintenance."Iss1712DpcleanupActors_Candidates";
-- Job-specific prep tables (staging + actor-name resolution).
DROP TABLE     IF EXISTS maintenance."Iss1712DpcleanupActors_ActorNames";
DROP TABLE     IF EXISTS maintenance."Iss1712DpcleanupActors_Staging";
