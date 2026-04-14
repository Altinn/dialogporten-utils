EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT
    d."Id",
    d."ContentUpdatedAt",
    d."CreatedAt",
    d."Deleted",
    d."DeletedAt",
    d."DueAt",
    d."ExpiresAt",
    d."ExtendedStatus",
    d."ExternalReference",
    d."FromPartyTransmissionsCount",
    d."FromServiceOwnerTransmissionsCount",
    d."Frozen",
    d."HasUnopenedContent",
    d."IdempotentKey",
    d."IsApiOnly",
    d."Org",
    d."Party",
    d."PrecedingProcess",
    d."Process",
    d."Progress",
    d."Revision",
    d."ServiceResource",
    d."ServiceResourceType",
    d."StatusId",
    d."SystemLabelsMask",
    d."UpdatedAt",
    d."VisibleFrom"
FROM
    (
        WITH permission_groups AS (
            SELECT
                x."Parties" AS parties,
                x."Services" AS services
            FROM
                jsonb_to_recordset('--PARTIESANDSERVICESPLACEHOLDER--' :: jsonb) AS x("Parties" text [], "Services" text [])
        ),
        service_permissions AS (
            SELECT
                s.service,
                pg.parties AS allowed_parties
            FROM
                permission_groups pg
                CROSS JOIN LATERAL unnest(pg.services) AS s(service)
        ),
        permission_candidate_ids AS (
            SELECT
                d_inner."Id",
                d_inner."ContentUpdatedAt"
            FROM
                service_permissions sp
                CROSS JOIN LATERAL (
                    SELECT
                        d."Id",
                        d."ContentUpdatedAt"
                    FROM
                        "Dialog" d
                    WHERE
                        d."ServiceResource" = sp.service
                        AND d."Party" = ANY(sp.allowed_parties)
                        AND d."StatusId" = ANY(ARRAY [7, 2, 8] :: int [])
                        AND (
                            d."VisibleFrom" IS NULL
                            OR d."VisibleFrom" <= '2026-04-14T13:07:52.9539460+00:00' :: timestamptz
                        )
                        AND (
                            d."ExpiresAt" IS NULL
                            OR d."ExpiresAt" > '2026-04-14T13:07:52.9539460+00:00' :: timestamptz
                        )
                        AND d."Deleted" = false :: boolean
                        AND (d."SystemLabelsMask" & 1 :: smallint) = 1 :: smallint
                    ORDER BY
                        "d"."ContentUpdatedAt" DESC,
                        "d"."Id" DESC
                    LIMIT
                        101
                ) d_inner
        ), candidate_dialogs AS (
            SELECT
                "Id",
                "ContentUpdatedAt"
            FROM
                permission_candidate_ids
        )
        SELECT
            d.*
        FROM
            (
                SELECT
                    cd."Id"
                FROM
                    candidate_dialogs cd
                ORDER BY
                    "cd"."ContentUpdatedAt" DESC,
                    "cd"."Id" DESC
                LIMIT
                    101
            ) cd
            JOIN "Dialog" d ON d."Id" = cd."Id"
        ORDER BY
            "d"."ContentUpdatedAt" DESC,
            "d"."Id" DESC
        LIMIT
            101
    ) AS d
ORDER BY
    d."ContentUpdatedAt" DESC,
    d."Id" DESC
LIMIT
    101