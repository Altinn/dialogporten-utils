-- dialog-search-scope-backfill cleanup: drop the job's schema objects once 05_verify.sql passes
-- (remaining = 0). The "maintenance" schema itself is preserved -- other jobs may live there.

DROP FUNCTION IF EXISTS maintenance.dialogsearch_scope_backfill_batch(text, integer, uuid, uuid);
DROP TABLE    IF EXISTS maintenance."DialogSearchScopeBackfill_Checkpoint";

-- Obsolete objects from the earlier candidate-table version of this job (if ever created):
DROP PROCEDURE IF EXISTS maintenance.dialogsearchscopebackfill_run_batch(integer, text, integer, integer, integer);
DROP VIEW      IF EXISTS maintenance."DialogSearchScopeBackfill_Progress";
DROP TABLE     IF EXISTS maintenance."DialogSearchScopeBackfill_BatchLog";
DROP TABLE     IF EXISTS maintenance."DialogSearchScopeBackfill_Candidates";
