\set ON_ERROR_STOP on

CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX_repair_huc_DialogActivity_AffectedWindow"
    ON "DialogActivity" ("CreatedAt", "Id")
    INCLUDE ("DialogId", "TypeId", "TransmissionId")
    WHERE "TypeId" IN (4, 16)
      AND "CreatedAt" >= timestamp with time zone '2026-02-25 07:26:42+00'
      AND "CreatedAt" < timestamp with time zone '2026-03-20 10:10:58+00';

CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX_repair_huc_Dialog_HasUnopenedContent"
    ON "Dialog" ("Id")
    INCLUDE ("ServiceResourceType")
    WHERE "HasUnopenedContent" = true;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX_repair_huc_DialogActivity_CorrespondenceOpened"
    ON "DialogActivity" ("DialogId")
    WHERE "TypeId" = 16;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX_repair_huc_DialogActivity_TransmissionOpened"
    ON "DialogActivity" ("TransmissionId")
    WHERE "TypeId" = 4;
