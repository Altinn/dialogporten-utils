WITH RECURSIVE orgs AS (
    SELECT MIN("Org") AS org FROM "Dialog"
                UNION ALL
                SELECT (SELECT MIN("Org") FROM "Dialog" WHERE "Org" > orgs.org)
                                    FROM orgs
                                    WHERE orgs.org IS NOT NULL
                               )
                SELECT org FROM orgs WHERE org IS NOT NULL
