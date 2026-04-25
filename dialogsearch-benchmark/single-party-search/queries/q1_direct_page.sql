SELECT d."Id", d."ContentUpdatedAt", d."CreatedAt", d."Deleted", d."DeletedAt", d."DueAt", d."ExpiresAt", d."ExtendedStatus", d."ExternalReference", d."FromPartyTransmissionsCount", d."FromServiceOwnerTransmissionsCount", d."Frozen", d."HasUnopenedContent", d."IdempotentKey", d."IsApiOnly", d."IsSeenSinceLastContentUpdate", d."Org", d."Party", d."PrecedingProcess", d."Process", d."Progress", d."Revision", d."ServiceResource", d."ServiceResourceType", d."StatusId", d."SystemLabelsMask", d."UpdatedAt", d."VisibleFrom"
FROM (
    SELECT d.*
    FROM "Dialog" d
    {{WHERE_CLAUSE}}
    AND d."StatusId" = ANY(ARRAY[7, 2, 8]::int[])
    AND (d."VisibleFrom" IS NULL OR d."VisibleFrom" <= {{AS_OF}}::timestamptz)
    AND (d."ExpiresAt" IS NULL OR d."ExpiresAt" > {{AS_OF}}::timestamptz)
    AND d."Deleted" = false::boolean
    ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
    LIMIT 101
) AS d
ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
LIMIT 101
