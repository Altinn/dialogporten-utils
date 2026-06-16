-- {{JOB_KEBAB}} cleanup: drop all objects created by this repair.
--
-- Run only AFTER 05_verify.sql passes:
--   - unprocessed_remaining = 0
--   - the job-specific invariant returns 0
--   - any batch-log retention you want has been exported (see below)
--
-- Export the batch log first if you want an audit trail -- 99 drops it:
--   psql -X -c "COPY (SELECT * FROM maintenance.\"{{JOB_PASCAL}}_BatchLog\" \
--       ORDER BY \"BatchId\") TO STDOUT WITH CSV HEADER" > {{JOB_KEBAB}}-batchlog.csv
--
-- The maintenance schema itself is preserved -- other jobs may live there.

DROP PROCEDURE IF EXISTS maintenance.{{JOB_LOWER}}_run_batch(integer, text, integer, integer, integer);
DROP VIEW      IF EXISTS maintenance."{{JOB_PASCAL}}_Progress";
DROP TABLE     IF EXISTS maintenance."{{JOB_PASCAL}}_BatchLog";
DROP TABLE     IF EXISTS maintenance."{{JOB_PASCAL}}_Candidates";
