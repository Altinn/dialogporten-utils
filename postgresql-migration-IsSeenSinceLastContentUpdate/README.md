# Backfill for IsContentSeen query-parameter

- Backfill scripts for feature: https://github.com/Altinn/dialogporten/pull/3583

## How to run

#### Setup

1. Make a .pgpass file in this directory
   ```sh
   touch .pgpass
   ```
2. Add a line to .pgpass with the following format: hostname:port:database:username:password
3. Set the file permissions of .pgpass
   ```sh
   chmod 600 ~/.pgpass
   ```

#### Backfill old dialogs

```sh
# Migrate all dialogs before 2025-12-01
sh ./backfill-old-dialogs.sh
```

#### Backfill new dialogs

```sh
# Migrate all dialogs after 2025-12-01
sh ./backfill-new-dialogs.sh
```

#### Backfill new A2 dialogs

```sh
# Migrate all dialogs that contains A2 ServiceResource. A2 dialogs wont have seen-logs, but are to be marked as seen.
sh ./backfill-A2-dialogs.sh
```

## Test setup (local)
```sql

-- Delete all existing seen logs
DELETE
FROM "DialogSeenLog";

-- Reset all test-data
UPDATE "Dialog"
SET "IsSeenSinceLastContentUpdate"= false,
    "Org"='ttd',
    "ContentUpdatedAt"='2000-01-01';

-- Make 20000 newer acn a2 dialogs
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='acn',
    "ServiceResource"='urn:altinn:resource:app_acn_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 0);

-- Make 20000 newer skd a2 dialogs
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='skd',
    "ServiceResource"='urn:altinn:resource:app_skd_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 20000);

-- Make 20000 newer brg a2 dialogs
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='brg',
    "ServiceResource"='urn:altinn:resource:app_brg_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 40000);

-- Make 20000 newer svv a2 dialogs
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01',
    "Org"='svv',
    "ServiceResource"='urn:altinn:resource:app_svv_a2-2802-10793'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 20000 OFFSET 60000);

-- Make 200000 newer dialogs
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-01-01'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 200000 OFFSET 100000);

-- "See" all 200000 newer dialogs from above
INSERT INTO "DialogSeenLog" ("Id", "CreatedAt", "IsViaServiceOwner", "DialogId", "EndUserTypeId")
SELECT gen_random_uuid(), now(), false, "Id", 1
FROM "Dialog"
ORDER BY "Id"
LIMIT 200000 OFFSET 100000;

-- Make 100000 newer dialogs that is not seen
UPDATE "Dialog"
SET "ContentUpdatedAt"='2026-02-01'
WHERE "Id" IN (SELECT "Id" FROM "Dialog" ORDER BY "Id" LIMIT 100000 OFFSET 400000);
```
