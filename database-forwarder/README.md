# database-forwarder — moved to `../db-access/`

This folder has **moved to [`db-access/`](../db-access/)**, which now holds the
tunnel script (`forward.sh`) plus the Entra login helpers (`db-login.sh`,
`pg-token.sh`).

`forward.sh` is kept here **temporarily** as a compatibility copy so existing
aliases/scripts pointing at `database-forwarder/forward.sh` don't break. It is
identical to `db-access/forward.sh`.

**Please update any alias to point at `db-access/forward.sh`.** This folder is
slated for removal after the summer (≈ 2026-08).
