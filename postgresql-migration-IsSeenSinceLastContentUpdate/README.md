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

#### Backfill dialogs

```sh
# Filter Strategy
bash ./BackfillDialogsFilterStrategy.sh
```

```sh
# Recompute Strategy on Org_CreatedAt_Id index
bash ./BackfillDialogsRecomputeStrategy_Org_CreatedAt_Id.sh
```

```sh
# Recompute Strategy on Id_Covering index (no org)
bash ./BackfillDialogsRecomputeStrategy_Id_Covering.sh
```


## Test setup (local)
```bash
PGPASSWORD='supersecret' psql -h localhost -p 15432 -U postgres -d Dialogporten -f ./sql/CreateTestData.sql 
```
