WITH d AS (
  SELECT "Id", "ContentUpdatedAt"
  FROM "Dialog"
  WHERE "Org"='skd'
    AND "Party"='$party'
  ORDER BY "ContentUpdatedAt" DESC, "Id" DESC
  LIMIT 1001
)
SELECT
  d."Id" AS "DialogId",
  c."Revision" AS "EndUserContextRevision",
  d."ContentUpdatedAt",
  COALESCE(array_agg(sl."SystemLabelId" ORDER BY sl."SystemLabelId")
           FILTER (WHERE sl."SystemLabelId" IS NOT NULL),
           ARRAY[]::int[]) AS "SystemLabels"
FROM d
JOIN "DialogEndUserContext" c ON c."DialogId" = d."Id"
LEFT JOIN "DialogEndUserContextSystemLabel" sl
  ON sl."DialogEndUserContextId" = c."Id"
GROUP BY d."Id", c."Revision", d."ContentUpdatedAt"
ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC;
