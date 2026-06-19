# database-forwarder — superseded by `../db-access/`

The tooling has moved to [`db-access/`](../db-access/), which holds the newer
tunnel script (`forward.sh`) plus the Entra login helpers (`db-login.sh`,
`pg-token.sh`).

**This folder keeps the previous, stable `forward.sh` on purpose.** It is the
proven script for general jumper / SSH access, kept as a fallback in case the
newer `db-access/forward.sh` (which has had significant recent changes) has any
issues. If the new one misbehaves, use this one.

Once the new version has settled, this folder will be removed (target: after the
summer, ≈ 2026-08). Please start migrating any aliases toward
`db-access/forward.sh`.
