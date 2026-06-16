-- {{JOB_KEBAB}} step 00: schema, candidate table, batch log, progress view, batch procedure.
--
-- Generated from maintenance-job-scaffold/_template. Runbook:
--   ../maintenance-job-scaffold/README.md
-- Editing contract (for humans and agents alike):
--   ../maintenance-job-scaffold/AGENTS.md
--
-- Everything OUTSIDE the ">>> JOB-SPECIFIC" fences is harness -- do not edit.
-- Sole sanctioned exception: adapting the candidate key shape (see the note on
-- the candidate table below).
--
-- Idempotent: re-running this file is safe (IF NOT EXISTS / OR REPLACE everywhere).
-- All objects are prefixed "{{JOB_PASCAL}}_" so they stay clearly separate from
-- anything else already in the maintenance schema.

CREATE SCHEMA IF NOT EXISTS maintenance;

-- Candidate table: one row per entity we plan to repair.
--
-- Key shape: a single uuid "EntityId" (every Dialogporten aggregate is uuid-keyed).
-- If your job needs a composite or non-uuid key, the edit is local and mechanical:
--   1. the PK column(s) below
--   2. the partial index
--   3. every "EntityId" reference in the procedure at the bottom of this file
--   4. the key columns in 01_build_candidates.sql / 02_dry_run_count.sql
CREATE TABLE IF NOT EXISTS maintenance."{{JOB_PASCAL}}_Candidates" (
    "EntityId"    uuid        PRIMARY KEY,
    "EnqueuedAt"  timestamptz NOT NULL DEFAULT now(),
    "ProcessedAt" timestamptz NULL,
    "WorkerId"    text        NULL,
    "Outcome"     text        NULL  -- 'updated' | 'skipped_state_changed'
);

-- Partial index supporting the FOR UPDATE SKIP LOCKED claim path.
-- Only unprocessed rows are in the index; once a row is marked processed it
-- drops out, so the claim query stays cheap as the run progresses.
CREATE INDEX IF NOT EXISTS "IX_{{JOB_PASCAL}}_Candidates_Unprocessed"
    ON maintenance."{{JOB_PASCAL}}_Candidates" ("EntityId")
    WHERE "ProcessedAt" IS NULL;

-- Per-batch log -- progress visibility and post-mortem.
CREATE TABLE IF NOT EXISTS maintenance."{{JOB_PASCAL}}_BatchLog" (
    "BatchId"    bigserial   PRIMARY KEY,
    "WorkerId"   text        NOT NULL,
    "StartedAt"  timestamptz NOT NULL DEFAULT now(),
    "FinishedAt" timestamptz NULL,
    "Claimed"    integer     NOT NULL DEFAULT 0,
    "Updated"    integer     NOT NULL DEFAULT 0,
    "Skipped"    integer     NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS "IX_{{JOB_PASCAL}}_BatchLog_WorkerId_BatchId"
    ON maintenance."{{JOB_PASCAL}}_BatchLog" ("WorkerId", "BatchId" DESC);

-- Progress view for at-a-glance status:
--   psql -c 'SELECT * FROM maintenance."{{JOB_PASCAL}}_Progress";'
CREATE OR REPLACE VIEW maintenance."{{JOB_PASCAL}}_Progress" AS
SELECT
    COUNT(*)                                          AS total,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NOT NULL) AS processed,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NULL)     AS remaining,
    COUNT(*) FILTER (WHERE "Outcome" = 'updated')     AS updated,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NOT NULL
                       AND "Outcome" IS DISTINCT FROM 'updated') AS skipped,
    MIN("ProcessedAt") AS first_processed_at,
    MAX("ProcessedAt") AS last_processed_at
FROM maintenance."{{JOB_PASCAL}}_Candidates";

-- Batch procedure: claims up to p_batch_size unprocessed candidates with
-- FOR UPDATE SKIP LOCKED, applies the job-specific mutation to the matching
-- application rows, and marks the candidates processed -- all in ONE chained
-- CTE statement so the locks acquired by the claim persist through the
-- dependent writes. No risk of another worker stealing a claimed row between
-- statements.
--
-- Silent-repair semantics: mutate ONLY the columns being fixed. "UpdatedAt"
-- and "Revision" (the EF concurrency token) are deliberately left untouched,
-- and no outbox row is inserted -- downstream consumers will not see an event.
CREATE OR REPLACE PROCEDURE maintenance.{{JOB_LOWER}}_run_batch(
    p_batch_size  integer,
    p_worker_id   text,
    OUT p_claimed integer,
    OUT p_updated integer,
    OUT p_skipped integer
) LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id bigint;
BEGIN
    INSERT INTO maintenance."{{JOB_PASCAL}}_BatchLog" ("WorkerId")
    VALUES (p_worker_id)
    RETURNING "BatchId" INTO v_batch_id;

    WITH claimed AS (
        SELECT "EntityId"
        FROM maintenance."{{JOB_PASCAL}}_Candidates"
        WHERE "ProcessedAt" IS NULL
        ORDER BY "EntityId"
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    ),
    -- >>> JOB-SPECIFIC (edit me) >>>
    -- TODO(job): the actual repair.
    --   * Update ONLY the columns being fixed. Do NOT touch "UpdatedAt" or
    --     "Revision", and do NOT insert outbox rows (silent repair -- see
    --     ../maintenance-job-scaffold/README.md if the job needs events).
    --   * KEEP a guard predicate: re-assert the original broken-state
    --     condition here so rows that changed between candidate-build and
    --     processing are skipped, never double-fixed.
    --   * Must RETURN the key of every row actually updated (the "marked"
    --     CTE below joins on it to classify outcomes).
    updated AS (
        UPDATE public."TODO_TargetTable" t
        SET    "TODO_Column" = 'TODO_new_value'
        FROM   claimed c
        WHERE  t."Id" = c."EntityId"
          AND  t."TODO_Column" = 'TODO_broken_value'  -- guard predicate
        RETURNING t."Id"
    ),
    -- <<< JOB-SPECIFIC <<<
    marked AS (
        UPDATE maintenance."{{JOB_PASCAL}}_Candidates" cand
        SET "ProcessedAt" = now(),
            "WorkerId"    = p_worker_id,
            "Outcome"     = CASE WHEN u."Id" IS NOT NULL
                                 THEN 'updated'
                                 ELSE 'skipped_state_changed' END
        FROM claimed c
        LEFT JOIN updated u ON u."Id" = c."EntityId"
        WHERE cand."EntityId" = c."EntityId"
        RETURNING cand."Outcome"
    )
    SELECT
        COUNT(*)::int                                       AS claimed,
        COUNT(*) FILTER (WHERE "Outcome" = 'updated')::int  AS updated,
        COUNT(*) FILTER (WHERE "Outcome" <> 'updated')::int AS skipped
    INTO p_claimed, p_updated, p_skipped
    FROM marked;

    UPDATE maintenance."{{JOB_PASCAL}}_BatchLog"
    SET "FinishedAt" = now(),
        "Claimed"    = p_claimed,
        "Updated"    = p_updated,
        "Skipped"    = p_skipped
    WHERE "BatchId" = v_batch_id;
END;
$$;
