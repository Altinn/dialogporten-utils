SET lock_timeout = '2s';
SET statement_timeout = '60s';

-- Seen = Dialog does not have a SystemLabel MarkedAsUnopened AND there is a seen log entry more recent than the dialogs ContentUpdatedAt
WITH eligible AS (SELECT d."Id", d."ContentUpdatedAt", d."SystemLabelsMask"
                  FROM "Dialog" d
                  WHERE d."Org" = :'org'
                    AND d."ContentUpdatedAt" >= '2025-12-01'
                    AND d."ServiceResource" NOT LIKE 'urn:altinn:resource:app_' || :'org' || '_a2-%'
                    AND d."IsSeenSinceLastContentUpdate" = true),
     seen AS (SELECT DISTINCT s."DialogId"
              FROM "DialogSeenLog" s
              JOIN eligible e ON s."DialogId" = e."Id"
              WHERE e."ContentUpdatedAt" < s."CreatedAt"),
     updatable AS (SELECT e."Id"
             FROM eligible e
             LEFT JOIN seen ON e."Id" = seen."DialogId"
             WHERE e."SystemLabelsMask" & 8 != 0 OR seen."DialogId" IS NULL
             LIMIT :batchSize),
     updated AS (UPDATE "Dialog" d
         SET "IsSeenSinceLastContentUpdate" = false
         FROM updatable
         WHERE d."Id" = updatable."Id"
         RETURNING 1)
SELECT COUNT(*)::int
FROM updated;
