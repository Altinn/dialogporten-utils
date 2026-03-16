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
