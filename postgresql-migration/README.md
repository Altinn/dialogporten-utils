# PostgreSQL Migration (pgcopydb)

This folder contains the operational script used to migrate Azure PostgreSQL Flexible Server databases with minimal downtime.

## Files

- `run-pgcopydb-migration.sh`: migration helper script (base copy + logical catch-up + cutover helpers).
- `cae-apps-parallel.sh`: parallel stop/start for all Container Apps in one CAE.
- `cae-jobs-parallel.sh`: parallel pause/resume for scheduled Container App Jobs.
- `update-keyvault-connection-strings.sh`: get/update Key Vault DB connection string secrets.

## Where to run

Run this script on a **jump host** that has network access to both source and target PostgreSQL servers.

Use `tmux` for the long-running `start` process so it survives SSH disconnects.

Example:

```bash
tmux new -s pgcopydb-migration
```

## Prerequisites

- `pgcopydb` installed on jump host.
- `psql` installed on jump host.
- Source server configured for logical replication:
  - `wal_level=logical`
  - `max_replication_slots >= 1`
  - `max_wal_senders >= 1`
- Source role has `REPLICATION` privilege.
- Target database exists (script can create it if missing when connected as admin role).

## Script actions

Set required secrets before every action:

```bash
export SOURCE_PASSWORD='...'
export TARGET_PASSWORD='...'
```

Optional common overrides:

```bash
export SOURCE_SERVER='dp-be-test-postgres-i7se3jtjey3lo'
export TARGET_SERVER='dp-be-test-postgres-i7se3jtjey3lo-v2'
export SOURCE_DB='dialogporten'
export TARGET_DB='dialogporten'
export SOURCE_USER='dialogportenPgAdmin'
export TARGET_USER='dialogportenPgAdmin'
export OUTPUT_PLUGIN='wal2json'
```

### 1) Start / initial load + follow

```bash
export START_FRESH=true
export DROP_IF_EXISTS=true
./run-pgcopydb-migration.sh start
```

Notes:
- `START_FRESH=true` resets slot/origin/workdir state and restarts from scratch.
- Keep this running in `tmux` while it catches up.

### 2) Resume after interruption

Preferred:

```bash
./run-pgcopydb-migration.sh resume
```

Equivalent manual mode:

```bash
export START_FRESH=false
export USE_RESUME=true
export RESUME_NOT_CONSISTENT=true
export DROP_IF_EXISTS=false
./run-pgcopydb-migration.sh start
```

Notes:
- Resume reuses existing workdir/replication state and runs `pgcopydb clone --follow --resume --not-consistent`.
- `--not-consistent` is required for resume when the old exported snapshot is no longer valid.
- Do **not** run `cleanup` before resume.

### 3) Check status (separate shell)

```bash
./run-pgcopydb-migration.sh status
```

### 4) Cutover (separate shell)

At cutover time:
1. Put applications/jobs in maintenance mode (stop writes).
2. Run cutover:

```bash
./run-pgcopydb-migration.sh cutover
```

Then wait for the running `start`/`resume` process to drain/apply and exit.

### 5) Cleanup after successful migration

```bash
./run-pgcopydb-migration.sh cleanup
```

Use cleanup only after successful switchover (or when intentionally discarding replication state).

## Operational guidance

- Monitor source WAL retention while catch-up runs.
- Logical replication does not synchronize sequence state automatically; reconcile sequences on target before reopening writes.
- Keep source available for rollback during agreed validation window.

## Key Vault connection string updates

Use `update-keyvault-connection-strings.sh` to inspect and update the two Dialogporten DB secrets in the environment key vault:
- `dialogportenAdoConnectionString`
- `dialogportenPsqlConnectionString`

The script resolves the single key vault in the provided resource group.

### Get current secret values

```bash
./update-keyvault-connection-strings.sh get --resource-group <rg>
```

### Update both secrets

The script prints the exact values it will write and asks for confirmation before overwriting.

```bash
./update-keyvault-connection-strings.sh update \
  --resource-group <rg> \
  --server <server-name> \
  --username <db-user> \
  --password <db-password>
```

Example:

```bash
./update-keyvault-connection-strings.sh get --resource-group dp-be-test-rg
./update-keyvault-connection-strings.sh update \
  --resource-group dp-be-test-rg \
  --server dp-be-test-postgresql-xxxx \
  --username dialogportenPgAdmin \
  --password '***'
```

## Scheduled Container App Jobs

`cae-apps-parallel.sh` does not control Container App Jobs. Use `cae-jobs-parallel.sh` to pause/resume scheduled jobs during cutover.

### Pause scheduled jobs before cutover

```bash
./cae-jobs-parallel.sh pause <resource-group> --parallelism 10
```

Default behavior:
- Stores each job's original cron expression in tag `pgMigrationOriginalCron`.
- Sets a temporary pause cron (`0 0 1 1 *` by default).
- Stops currently running job executions.

### Resume scheduled jobs after cutover

```bash
./cae-jobs-parallel.sh resume <resource-group> --parallelism 10
```

### Inspect job pause/resume status

```bash
./cae-jobs-parallel.sh status <resource-group>
```
