SET lock_timeout = '2s';
SET statement_timeout = '60s';

WITH CTE AS (SELECT "Id"
             FROM "Dialog" d
             WHERE "Org" = :'org'
               AND "ContentUpdatedAt" >= '2025-12-01'
               AND "ServiceResource" NOT LIKE 'urn:altinn:resource:app_' || :'org' || '_a2-%'
               AND "IsSeenSinceLastContentUpdate" = true
               AND NOT (
                 -- Seen = Dialog does not have a SystemLabel MarkedAsUnopened AND there is a seen log entry more recent than the dialogs ContentUpdatedAt
                 "SystemLabelsMask" & 8 = 0
                     AND EXISTS (SELECT 1
                                 FROM "DialogSeenLog" s
                                 WHERE d."Id" = s."DialogId"
                                   AND d."ContentUpdatedAt" < s."CreatedAt")
                 )
             LIMIT :batchSize)
UPDATE "Dialog" d
SET "IsSeenSinceLastContentUpdate" = false
FROM cte
WHERE d."Id" = cte."Id"
RETURNING 1;
