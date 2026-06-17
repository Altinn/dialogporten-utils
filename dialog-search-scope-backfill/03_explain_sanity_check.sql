-- dialog-search-scope-backfill sanity check: confirm a single batch uses the keyset (primary-key)
-- access path BEFORE running the full job.
--
-- What to look for:
--   - an Index Scan on the DialogSearch primary key for the `DialogId > ... ORDER BY DialogId
--     LIMIT n` range (NOT a Seq Scan -- the whole point is to walk the PK cheaply), and
--   - a PK index lookup into "Dialog" for the join.
--
-- Plain EXPLAIN (no ANALYZE) so nothing executes. This mirrors the read side of the batch in
-- maintenance.dialogsearch_scope_backfill_batch; the UPDATE join uses the same access path.
EXPLAIN
WITH batch AS (
    SELECT ds."DialogId"
    FROM search."DialogSearch" ds
    WHERE ds."DialogId" > '00000000-0000-0000-0000-000000000000'::uuid
    ORDER BY ds."DialogId"
    LIMIT 5000
)
SELECT b."DialogId",
       d."ContentUpdatedAt",
       replace(d."ServiceResource", 'urn:altinn:resource:', '')
FROM batch b
JOIN public."Dialog" d ON d."Id" = b."DialogId";
