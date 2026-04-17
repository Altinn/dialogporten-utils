SET lock_timeout = '2s';
SET statement_timeout = '60s';

WITH batch AS (SELECT d."Id"
               FROM "Dialog" d
               WHERE d."ContentUpdatedAt" >= DATE '2025-12-01'
                 AND d."Id" > :'lastId'
               ORDER BY d."Id"
               LIMIT :batchSize),
     calc AS (SELECT d."Id",
                     (
                         d."ServiceResource" LIKE 'urn:altinn:resource:app_%_a2-%'
                             OR (
                             d."SystemLabelsMask" & 8 = 0
                                 AND EXISTS (SELECT 1
                                             FROM "DialogSeenLog" s
                                             WHERE s."DialogId" = d."Id"
                                               AND s."CreatedAt" > d."ContentUpdatedAt")
                             )
                         ) AS new_value
              FROM batch b
                       JOIN "Dialog" d ON d."Id" = b."Id"),
     updated AS (
         UPDATE "Dialog" d
             SET "IsSeenSinceLastContentUpdate" = c.new_value
             FROM calc c
             WHERE d."Id" = c."Id"
                 AND d."IsSeenSinceLastContentUpdate" IS DISTINCT FROM c.new_value
             RETURNING d."Id")
SELECT COUNT(*)::int,
       (SELECT "Id" FROM batch ORDER BY "Id" DESC LIMIT 1) AS "lastId"
FROM updated;
