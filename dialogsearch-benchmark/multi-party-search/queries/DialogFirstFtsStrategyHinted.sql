/*+ 
	DisableIndex(d
		IX_Dialog_Party_ServiceResource_ContentUpdatedAt_Id_NotDeleted
	 	IX_Dialog_ServiceResource_Party_ContentUpdatedAt_Id_NotDeleted
	 	IX_Dialog_Party_CreatedAt_Id
	 	IX_Dialog_Party_UpdatedAt_Id
	 	IX_Dialog_Party_DueAt_Id
	 	IX_Dialog_Org_Party_ContentUpdatedAt_Id
	 	IX_Dialog_Org_ServiceResource_ContentUpdatedAt_Id
	 	IX_Dialog_ServiceResource
	)
*/
SELECT d."Id", d."ContentUpdatedAt", d."CreatedAt", d."Deleted", d."DeletedAt", d."DueAt", d."ExpiresAt", d."ExtendedStatus", d."ExternalReference", d."FromPartyTransmissionsCount", d."FromServiceOwnerTransmissionsCount", d."Frozen", d."HasUnopenedContent", d."IdempotentKey", d."IsApiOnly", d."IsSeenSinceLastContentUpdate", d."Org", d."Party", d."PrecedingProcess", d."Process", d."Progress", d."Revision", d."ServiceResource", d."ServiceResourceType", d."StatusId", d."SystemLabelsMask", d."UpdatedAt", d."VisibleFrom"
FROM (
    WITH permission_groups AS (
        SELECT x."Parties" AS parties
             , x."Services" AS services
        FROM jsonb_to_recordset({{PARTIES_AND_SERVICES_JSON}}::jsonb) AS x("Parties" text[], "Services" text[])
    )
    ,party_permissions AS (
        SELECT p.party
             , pg.services AS allowed_services
        FROM permission_groups pg
        CROSS JOIN LATERAL unnest(pg.parties) AS p(party)
    )
    ,dialog_candidates AS (
        SELECT d_inner."Id", d_inner."ContentUpdatedAt"
        FROM party_permissions pp
        CROSS JOIN LATERAL (
            SELECT d."Id", d."ContentUpdatedAt"
            FROM "Dialog" d
            WHERE d."Party" = pp.party
              AND d."ServiceResource" = ANY(pp.allowed_services)
               AND (d."VisibleFrom" IS NULL OR d."VisibleFrom" <= {{AS_OF}}::timestamptz)  AND (d."ExpiresAt" IS NULL OR d."ExpiresAt" > {{AS_OF}}::timestamptz)  AND d."Deleted" = 'f'::boolean  AND ('t'::boolean = false OR 't'::boolean = true AND d."IsApiOnly" = false)  ORDER BY "d"."ContentUpdatedAt" DESC, "d"."Id" DESC         LIMIT '2500'
        ) d_inner ORDER BY "d_inner"."ContentUpdatedAt" DESC, "d_inner"."Id" DESC     LIMIT '5000'
    )
    ,fts_matches AS (
        SELECT dc."Id", dc."ContentUpdatedAt"
        FROM dialog_candidates dc
        JOIN search."DialogSearch" ds ON ds."DialogId" = dc."Id"
        WHERE numnode(websearch_to_tsquery('norwegian'::regconfig, 'melding'::text)) > 0
    	AND querytree(websearch_to_tsquery('norwegian'::regconfig, 'melding'::text)) <> 'T'
    	AND ds."SearchVector" @@ websearch_to_tsquery('norwegian'::regconfig, 'melding'::text) ORDER BY "dc"."ContentUpdatedAt" DESC, "dc"."Id" DESC  LIMIT 101 ),
    candidate_dialogs AS (
        SELECT "Id", "ContentUpdatedAt"
        FROM fts_matches)
    SELECT d.*
    FROM (
        SELECT cd."Id"
        FROM candidate_dialogs cd ORDER BY "cd"."ContentUpdatedAt" DESC, "cd"."Id" DESC  LIMIT 101 ) cd
    JOIN "Dialog" d ON d."Id" = cd."Id" ORDER BY "d"."ContentUpdatedAt" DESC, "d"."Id" DESC  LIMIT 101 
) AS d
ORDER BY d."ContentUpdatedAt" DESC, d."Id" DESC
LIMIT '101';