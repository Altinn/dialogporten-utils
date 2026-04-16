DELETE
FROM "DialogSeenLog";

-- Reset all test-data
UPDATE "Dialog"
SET "IsSeenSinceLastContentUpdate"= true,
    "Org"                         = 'ttd',
    "ContentUpdatedAt"='2000-01-01';

-- Make 20000 newer acn a2 dialogs
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='acn',
    "IsSeenSinceLastContentUpdate"= true,
    "ServiceResource"='urn:altinn:resource:app_acn_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 0);

-- Make 20000 newer skd a2 dialogs with system label MarkedAsUnopened and not recent seen logs
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='skd',
    "IsSeenSinceLastContentUpdate"= true,
    "SystemLabelsMask" = 8,
    "ServiceResource"='urn:altinn:resource:app_skd_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 20000);

INSERT INTO "DialogSeenLog" ("Id", "CreatedAt", "IsViaServiceOwner", "DialogId", "EndUserTypeId")
SELECT gen_random_uuid(), '2025-12-24', false, "Id", 1
FROM "Dialog"
ORDER BY "Id"
    LIMIT 20000 OFFSET 20000;

-- Make 20000 newer brg a2 dialogs with system label MarkedAsUnopened
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='brg',
    "IsSeenSinceLastContentUpdate"= true,
    "SystemLabelsMask" = 8,
    "ServiceResource"='urn:altinn:resource:app_brg_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 40000);

-- Make 20000 newer svv a2 dialogs with not recent seen log
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='svv',
    "IsSeenSinceLastContentUpdate"= true,
    "ServiceResource"='urn:altinn:resource:app_svv_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 60000);

INSERT INTO "DialogSeenLog" ("Id", "CreatedAt", "IsViaServiceOwner", "DialogId", "EndUserTypeId")
SELECT gen_random_uuid(), '2025-12-24', false, "Id", 1
FROM "Dialog"
ORDER BY "Id"
    LIMIT 20000 OFFSET 60000;

-- Make 100000 newer dialogs regular dialogs that have seen log entry newer than ContentUpdatedAt at
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "IsSeenSinceLastContentUpdate"= true
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 100000 OFFSET 100000);

INSERT INTO "DialogSeenLog" ("Id", "CreatedAt", "IsViaServiceOwner", "DialogId", "EndUserTypeId")
SELECT gen_random_uuid(), now(), false, "Id", 1
FROM "Dialog"
ORDER BY "Id"
    LIMIT 100000 OFFSET 100000;

-- Make 60000 newer dialogs that have seen log entry older than ContentUpdatedAt. With the wrong default
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "IsSeenSinceLastContentUpdate"= true
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 60000 OFFSET 200000);

INSERT INTO "DialogSeenLog" ("Id", "CreatedAt", "IsViaServiceOwner", "DialogId", "EndUserTypeId")
SELECT gen_random_uuid(), '2025-12-24', false, "Id", 1
FROM "Dialog"
ORDER BY "Id"
    LIMIT 60000 OFFSET 200000;

-- Make 40000 newer dialogs that have no seen log entries or system labels. With the wrong default
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-02-01',
    "IsSeenSinceLastContentUpdate" = true
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 40000 OFFSET 260000);

-- Make 30000 newer dialogs that have system label MarkedAsUnopened. With the wrong default
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-02-01',
    "IsSeenSinceLastContentUpdate" = true,
    "SystemLabelsMask" = 8
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 30000 OFFSET 300000);

-- Make 20000 newer dialogs that have system label MarkedAsUnopened and recent seen log. With the wrong default
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-02-01',
    "IsSeenSinceLastContentUpdate" = true,
    "SystemLabelsMask" = 8
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 330000);

INSERT INTO "DialogSeenLog" ("Id", "CreatedAt", "IsViaServiceOwner", "DialogId", "EndUserTypeId")
SELECT gen_random_uuid(), now(), false, "Id", 1
FROM "Dialog"
ORDER BY "Id"
    LIMIT 20000 OFFSET 330000;

-- Make 10000 newer dialogs that have system label MarkedAsUnopened and not recent seen log. With the wrong default
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-02-01',
    "IsSeenSinceLastContentUpdate" = true,
    "SystemLabelsMask" = 8
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 10000 OFFSET 350000);

INSERT INTO "DialogSeenLog" ("Id", "CreatedAt", "IsViaServiceOwner", "DialogId", "EndUserTypeId")
SELECT gen_random_uuid(), '2025-12-24', false, "Id", 1
FROM "Dialog"
ORDER BY "Id"
    LIMIT 10000 OFFSET 350000;

-- Total number of fixed dialogs should be 160000
