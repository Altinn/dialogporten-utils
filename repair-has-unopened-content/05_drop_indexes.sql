\set ON_ERROR_STOP on

DROP INDEX CONCURRENTLY IF EXISTS "IX_repair_huc_DialogActivity_AffectedWindow";
DROP INDEX CONCURRENTLY IF EXISTS "IX_repair_huc_Dialog_HasUnopenedContent";
DROP INDEX CONCURRENTLY IF EXISTS "IX_repair_huc_DialogActivity_CorrespondenceOpened";
DROP INDEX CONCURRENTLY IF EXISTS "IX_repair_huc_DialogActivity_TransmissionOpened";
