-- dialog-search-scope-backfill step 00: schema, checkpoint table, batch function.
--
-- KEYSET-BATCHED variant. This is a near-total backfill (~every row in
-- search."DialogSearch" needs it), so the scaffold's candidate-table model is the wrong shape:
-- it would duplicate the whole 1.1B-row keyspace into a transient table inside one giant
-- transaction. Instead we walk the DialogSearch primary key (DialogId) in batches, UPDATE-ing the
-- two new columns in place, and COMMIT each batch (the per-batch commit happens in the calling
-- psql session via autocommit -- see repair.sh). No candidate table, no long-held snapshot.
--
-- Silent repair: only "ContentUpdatedAt" and "ServiceResource" are written. "UpdatedAt" (the
-- reindex/staleness watermark) and "Revision" are deliberately untouched; no outbox row is written.
-- Idempotent: re-running this file is safe.

CREATE SCHEMA IF NOT EXISTS maintenance;

-- Drop objects from the earlier candidate-table version of this job, if step 00 was run before.
DROP PROCEDURE IF EXISTS maintenance.dialogsearchscopebackfill_run_batch(integer, text, integer, integer, integer);
DROP VIEW  IF EXISTS maintenance."DialogSearchScopeBackfill_Progress";
DROP TABLE IF EXISTS maintenance."DialogSearchScopeBackfill_BatchLog";
DROP TABLE IF EXISTS maintenance."DialogSearchScopeBackfill_Candidates";

-- Checkpoint: one row per worker. Holds the keyset cursor (last processed DialogId) so a killed
-- worker resumes exactly where it left off, plus running totals for progress/ETA.
CREATE TABLE IF NOT EXISTS maintenance."DialogSearchScopeBackfill_Checkpoint" (
    "WorkerId"     text        PRIMARY KEY,
    "LastDialogId" uuid        NOT NULL,
    "UpdatedTotal" bigint      NOT NULL DEFAULT 0,
    "ScannedTotal" bigint      NOT NULL DEFAULT 0,
    "Batches"      bigint      NOT NULL DEFAULT 0,
    "StartedAt"    timestamptz NOT NULL DEFAULT now(),
    "UpdatedAt"    timestamptz NOT NULL DEFAULT now()
);

-- One batch: claim the next p_batch_size DialogIds by keyset, backfill the unpopulated ones from
-- the source Dialog, advance + persist the cursor. Returns (updated, scanned, next_cursor).
--   * scanned = 0  => the worker's range (cursor, p_until) is exhausted; the driver stops.
--   * Resumable: cursor comes from the checkpoint; on the worker's first batch it starts at
--     COALESCE(p_start_at, min-uuid).
--   * Parallel: give each worker a distinct p_worker and a DISJOINT [p_start_at, p_until) range.
-- The whole function runs as a single statement => one autocommit transaction per batch.
CREATE OR REPLACE FUNCTION maintenance.dialogsearch_scope_backfill_batch(
    p_worker     text,
    p_batch_size integer,
    p_start_at   uuid DEFAULT NULL,   -- range lower bound (exclusive); used only if no checkpoint yet
    p_until      uuid DEFAULT NULL    -- range upper bound (exclusive); NULL = no bound
) RETURNS TABLE(updated integer, scanned integer, next_cursor uuid)
LANGUAGE plpgsql AS $$
DECLARE
    v_cursor  uuid;
    v_updated integer;
    v_scanned integer;
    v_next    uuid;
BEGIN
    SELECT c."LastDialogId" INTO v_cursor
    FROM maintenance."DialogSearchScopeBackfill_Checkpoint" c
    WHERE c."WorkerId" = p_worker;

    v_cursor := COALESCE(v_cursor, p_start_at, '00000000-0000-0000-0000-000000000000'::uuid);

    WITH batch AS (
        SELECT ds."DialogId"
        FROM search."DialogSearch" ds
        WHERE ds."DialogId" > v_cursor
          AND (p_until IS NULL OR ds."DialogId" < p_until)
        ORDER BY ds."DialogId"
        LIMIT p_batch_size
    ),
    upd AS (
        UPDATE search."DialogSearch" ds
        SET "ContentUpdatedAt" = d."ContentUpdatedAt",
            "ServiceResource"  = replace(d."ServiceResource", 'urn:altinn:resource:', '')
        FROM batch b
        JOIN public."Dialog" d ON d."Id" = b."DialogId"
        WHERE ds."DialogId" = b."DialogId"
          AND (ds."ContentUpdatedAt" IS NULL OR ds."ServiceResource" IS NULL)  -- guard
        RETURNING ds."DialogId"
    )
    SELECT (SELECT count(*) FROM upd)::int,
           (SELECT count(*) FROM batch)::int,
           (SELECT max(b2."DialogId") FROM batch b2)
    INTO v_updated, v_scanned, v_next;

    IF v_scanned > 0 THEN
        INSERT INTO maintenance."DialogSearchScopeBackfill_Checkpoint" AS c
            ("WorkerId","LastDialogId","UpdatedTotal","ScannedTotal","Batches")
        VALUES (p_worker, v_next, v_updated, v_scanned, 1)
        ON CONFLICT ("WorkerId") DO UPDATE
        SET "LastDialogId" = EXCLUDED."LastDialogId",
            "UpdatedTotal" = c."UpdatedTotal" + EXCLUDED."UpdatedTotal",
            "ScannedTotal" = c."ScannedTotal" + EXCLUDED."ScannedTotal",
            "Batches"      = c."Batches" + 1,
            "UpdatedAt"    = now();
    END IF;

    updated := v_updated; scanned := v_scanned; next_cursor := v_next;
    RETURN NEXT;
END;
$$;
