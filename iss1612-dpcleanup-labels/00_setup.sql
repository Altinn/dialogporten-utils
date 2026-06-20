-- iss1612-dpcleanup-labels step 00: schema, candidate table, batch log, progress view, batch procedure.
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
-- All objects are prefixed "Iss1612DpcleanupLabels_" so they stay clearly separate from
-- anything else already in the maintenance schema.

CREATE SCHEMA IF NOT EXISTS maintenance;

-- ===========================================================================
-- JOB-SPECIFIC prep tables (staging + actor-name resolution).
--
-- This job is more than a single-column UPDATE, so in addition to the standard
-- candidate table it needs:
--   * a STAGING table holding the raw CSV (loaded by 00a_load_csv.sql), and
--   * an ACTORNAMES resolution table mapping each actor urn to a name + the
--     ActorName row id to reference (filled by resolve-actornames.sql +
--     00b_ensure_actornames.sql).
-- Both are dropped again by 99_cleanup.sql.
-- ===========================================================================

-- Raw CSV rows: ISS1612_DPCleanup.csv (dialogid, TimeStamp, ActorId, SystemLabel).
CREATE TABLE IF NOT EXISTS maintenance."Iss1612DpcleanupLabels_Staging" (
    "dialog_id"    uuid        NOT NULL,
    "source_ts"    timestamptz NOT NULL,
    "actor_id"     text        NOT NULL,
    "system_label" text        NOT NULL
);

-- One row per distinct actor urn. "actor_name_entity_id" is the public."ActorName"
-- row to reference from the LabelAssignmentLogActor; "source" records how the name
-- was resolved:
--   'existing'   -- reused an ActorName row already present for this actor
--   'brreg'      -- looked up the org name from the Brønnøysund register
--   'unresolved' -- neither found; these candidates are skipped (decision 4)
CREATE TABLE IF NOT EXISTS maintenance."Iss1612DpcleanupLabels_ActorNames" (
    "actor_id"             text PRIMARY KEY,
    "name"                 text NULL,
    "actor_name_entity_id" uuid NULL,
    "source"               text NULL
);

-- Candidate table: one row per (DialogEndUserContext, target SystemLabel) we plan
-- to repair.
--
-- Key shape: this job uses the sanctioned COMPOSITE key ("EntityId" = the
-- DialogEndUserContext."Id", "SystemLabelId" = the target label), because a single
-- context can legitimately receive a folder label (Bin/Archive) and an independent
-- flag (MarkedAsUnopened) in the same dataset. The candidate also carries the
-- payload needed by the mutation: the resolved ActorName row id, the original CSV
-- timestamp, and the namespaced label name for the audit log.
CREATE TABLE IF NOT EXISTS maintenance."Iss1612DpcleanupLabels_Candidates" (
    "EntityId"           uuid        NOT NULL,  -- DialogEndUserContext."Id"
    "SystemLabelId"      integer     NOT NULL,  -- 2=Bin, 3=Archive, 4=MarkedAsUnopened
    "ActorNameEntityId"  uuid        NOT NULL,  -- public."ActorName"."Id" to reference
    "SourceTs"           timestamptz NOT NULL,  -- CSV TimeStamp; used as CreatedAt
    "LabelName"          text        NOT NULL,  -- e.g. 'systemlabel:Archive'
    "EnqueuedAt"         timestamptz NOT NULL DEFAULT now(),
    "ProcessedAt"        timestamptz NULL,
    "WorkerId"           text        NULL,
    "Outcome"            text        NULL,  -- 'updated' | 'skipped_state_changed'
    PRIMARY KEY ("EntityId", "SystemLabelId")
);

-- Partial index supporting the FOR UPDATE SKIP LOCKED claim path.
-- Only unprocessed rows are in the index; once a row is marked processed it
-- drops out, so the claim query stays cheap as the run progresses.
CREATE INDEX IF NOT EXISTS "IX_Iss1612DpcleanupLabels_Candidates_Unprocessed"
    ON maintenance."Iss1612DpcleanupLabels_Candidates" ("EntityId", "SystemLabelId")
    WHERE "ProcessedAt" IS NULL;

-- Per-batch log -- progress visibility and post-mortem.
CREATE TABLE IF NOT EXISTS maintenance."Iss1612DpcleanupLabels_BatchLog" (
    "BatchId"    bigserial   PRIMARY KEY,
    "WorkerId"   text        NOT NULL,
    "StartedAt"  timestamptz NOT NULL DEFAULT now(),
    "FinishedAt" timestamptz NULL,
    "Claimed"    integer     NOT NULL DEFAULT 0,
    "Updated"    integer     NOT NULL DEFAULT 0,
    "Skipped"    integer     NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS "IX_Iss1612DpcleanupLabels_BatchLog_WorkerId_BatchId"
    ON maintenance."Iss1612DpcleanupLabels_BatchLog" ("WorkerId", "BatchId" DESC);

-- Progress view for at-a-glance status:
--   psql -c 'SELECT * FROM maintenance."Iss1612DpcleanupLabels_Progress";'
CREATE OR REPLACE VIEW maintenance."Iss1612DpcleanupLabels_Progress" AS
SELECT
    COUNT(*)                                          AS total,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NOT NULL) AS processed,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NULL)     AS remaining,
    COUNT(*) FILTER (WHERE "Outcome" = 'updated')     AS updated,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NOT NULL
                       AND "Outcome" IS DISTINCT FROM 'updated') AS skipped,
    MIN("ProcessedAt") AS first_processed_at,
    MAX("ProcessedAt") AS last_processed_at
FROM maintenance."Iss1612DpcleanupLabels_Candidates";

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
CREATE OR REPLACE PROCEDURE maintenance.iss1612dpcleanuplabels_run_batch(
    p_batch_size  integer,
    p_worker_id   text,
    OUT p_claimed integer,
    OUT p_updated integer,
    OUT p_skipped integer
) LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id bigint;
BEGIN
    INSERT INTO maintenance."Iss1612DpcleanupLabels_BatchLog" ("WorkerId")
    VALUES (p_worker_id)
    RETURNING "BatchId" INTO v_batch_id;

    WITH claimed AS (
        SELECT "EntityId", "SystemLabelId"
        FROM maintenance."Iss1612DpcleanupLabels_Candidates"
        WHERE "ProcessedAt" IS NULL
        ORDER BY "EntityId", "SystemLabelId"
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    ),
    -- >>> JOB-SPECIFIC (edit me) >>>
    -- The repair: restore the missing end-user system label exactly as the
    -- application would have, as a SILENT repair (no events; DialogEndUserContext
    -- "UpdatedAt"/"Revision" deliberately left untouched). Per claimed candidate:
    --   1. (folder labels only) delete the mutually-exclusive Default(1) label
    --      row -- Default/Bin/Archive are an XOR group, so adding Bin/Archive
    --      removes Default. Removing Default is NOT logged (matches the app).
    --   2. insert the target DialogEndUserContextSystemLabel row.
    --   3. insert the "set <label>" LabelAssignmentLog audit entry.
    --   4. insert the matching Actor (LabelAssignmentLogActor, PartyRepresentative)
    --      referencing the pre-resolved ActorName.
    -- All new ids are UUIDv7 (uuidv7(), native on Postgres 18) to match the app.
    -- All new rows use the CSV timestamp ("SourceTs") as CreatedAt/UpdatedAt.
    --
    -- GUARD (re-asserted here so a row that changed since candidate-build is
    -- skipped, never double-fixed): a folder-label target applies only if the
    -- context has NO non-default folder label yet; any target applies only if
    -- that exact label is not already present.
    eligible AS (
        SELECT c."EntityId", c."SystemLabelId", cand."ActorNameEntityId",
               cand."SourceTs", cand."LabelName"
        FROM claimed c
        JOIN maintenance."Iss1612DpcleanupLabels_Candidates" cand
          ON cand."EntityId"      = c."EntityId"
         AND cand."SystemLabelId" = c."SystemLabelId"
        WHERE NOT EXISTS (  -- folder-label guard: no Bin/Archive present yet
                  SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
                  WHERE l."DialogEndUserContextId" = c."EntityId"
                    AND l."SystemLabelId" IN (2, 3)
                    AND c."SystemLabelId" IN (2, 3)
              )
          AND NOT EXISTS (  -- this exact label not already set
                  SELECT 1 FROM public."DialogEndUserContextSystemLabel" l
                  WHERE l."DialogEndUserContextId" = c."EntityId"
                    AND l."SystemLabelId" = c."SystemLabelId"
              )
    ),
    del_default AS (  -- XOR: drop Default only when adding a folder label
        DELETE FROM public."DialogEndUserContextSystemLabel" l
        USING eligible e
        WHERE l."DialogEndUserContextId" = e."EntityId"
          AND l."SystemLabelId" = 1
          AND e."SystemLabelId" IN (2, 3)
        RETURNING l."DialogEndUserContextId"
    ),
    ins_label AS (
        INSERT INTO public."DialogEndUserContextSystemLabel"
            ("DialogEndUserContextId", "SystemLabelId", "CreatedAt")
        SELECT e."EntityId", e."SystemLabelId", e."SourceTs"
        FROM eligible e
        ON CONFLICT ("DialogEndUserContextId", "SystemLabelId") DO NOTHING
        RETURNING "DialogEndUserContextId", "SystemLabelId"
    ),
    ins_log AS (
        INSERT INTO public."LabelAssignmentLog"
            ("Id", "CreatedAt", "Name", "Action", "ContextId")
        SELECT uuidv7(), e."SourceTs", e."LabelName", 'set', e."EntityId"
        FROM eligible e
        JOIN ins_label il ON il."DialogEndUserContextId" = e."EntityId"
                         AND il."SystemLabelId"          = e."SystemLabelId"
        RETURNING "Id" AS log_id, "ContextId"
    ),
    ins_actor AS (
        INSERT INTO public."Actor"
            ("Id", "ActorTypeId", "CreatedAt", "UpdatedAt", "Discriminator",
             "ActorNameEntityId", "LabelAssignmentLogId")
        SELECT uuidv7(), 1, e."SourceTs", e."SourceTs",
               'LabelAssignmentLogActor', e."ActorNameEntityId", l.log_id
        FROM ins_log l
        JOIN eligible e ON e."EntityId" = l."ContextId"
        RETURNING "Id"
    ),
    -- <<< JOB-SPECIFIC <<<
    marked AS (
        UPDATE maintenance."Iss1612DpcleanupLabels_Candidates" cand
        SET "ProcessedAt" = now(),
            "WorkerId"    = p_worker_id,
            "Outcome"     = CASE WHEN il."DialogEndUserContextId" IS NOT NULL
                                 THEN 'updated'
                                 ELSE 'skipped_state_changed' END
        FROM claimed c
        LEFT JOIN ins_label il ON il."DialogEndUserContextId" = c."EntityId"
                              AND il."SystemLabelId"          = c."SystemLabelId"
        WHERE cand."EntityId"      = c."EntityId"
          AND cand."SystemLabelId" = c."SystemLabelId"
        RETURNING cand."Outcome"
    )
    SELECT
        COUNT(*)::int                                       AS claimed,
        COUNT(*) FILTER (WHERE "Outcome" = 'updated')::int  AS updated,
        COUNT(*) FILTER (WHERE "Outcome" <> 'updated')::int AS skipped
    INTO p_claimed, p_updated, p_skipped
    FROM marked;

    UPDATE maintenance."Iss1612DpcleanupLabels_BatchLog"
    SET "FinishedAt" = now(),
        "Claimed"    = p_claimed,
        "Updated"    = p_updated,
        "Skipped"    = p_skipped
    WHERE "BatchId" = v_batch_id;
END;
$$;
