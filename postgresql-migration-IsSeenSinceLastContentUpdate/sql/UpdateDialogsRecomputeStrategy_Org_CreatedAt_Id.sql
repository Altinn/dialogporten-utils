SET lock_timeout = '2s';
SET statement_timeout = '60s';

WITH CTE AS (SELECT "Id"
             FROM "Dialog" d
             WHERE "Org" = :'org'
               AND "ContentUpdatedAt" >= '2025-12-01'
               AND "Id" > :'lastId'
             ORDER BY "CreatedAt"
             LIMIT :batchSize),
     UPDATED AS (UPDATE "Dialog" d
-- Seen = A2 ServiceResource or (Dialog does not have a SystemLabel MarkedAsUnopened AND there is a seen log entry more recent than the dialogs ContentUpdatedAt)
         SET "IsSeenSinceLastContentUpdate" =
                 d."ServiceResource" LIKE 'urn:altinn:resource:app_' || :'org' || '_a2-%' OR
                 (
                     "SystemLabelsMask" & 8 = 0
                         AND EXISTS (SELECT 1
                                     FROM "DialogSeenLog" s
                                     WHERE d."Id" = s."DialogId"
                                       AND d."ContentUpdatedAt" < s."CreatedAt")
                     )
         FROM cte
         WHERE d."Id" = cte."Id"
         RETURNING d."Id"::uuid as "Id")
SELECT COUNT(*)::int, (SELECT "Id" FROM UPDATED ORDER BY "Id" DESC LIMIT 1)
FROM UPDATED;
