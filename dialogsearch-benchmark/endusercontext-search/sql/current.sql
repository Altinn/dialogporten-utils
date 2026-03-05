SELECT d."Id" AS "DialogId"
     , c."Revision" AS "EndUserContextRevision"
     , d."ContentUpdatedAt"
     , COALESCE(sl_agg."SystemLabels", ARRAY[]::int[]) AS "SystemLabels"
FROM (
    SELECT d."Id", d."ContentUpdatedAt"
    FROM "Dialog" d
    WHERE d."Org" = 'skd' AND d."Party" = '$party'  ORDER BY "d"."ContentUpdatedAt" DESC, "d"."Id" DESC  LIMIT 1001     ) d
 INNER JOIN "DialogEndUserContext" c ON c."DialogId" = d."Id"
 LEFT JOIN LATERAL (
     SELECT ARRAY_AGG(sl."SystemLabelId" ORDER BY sl."SystemLabelId") AS "SystemLabels"
     FROM "DialogEndUserContextSystemLabel" sl
     WHERE sl."DialogEndUserContextId" = c."Id"
 ) sl_agg ON TRUE ORDER BY "d"."ContentUpdatedAt" DESC, "d"."Id" DESC 
