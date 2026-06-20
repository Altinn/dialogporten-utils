-- iss1712-dpcleanup-actors step 00: schema, candidate table, batch log, progress view, batch procedure.
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
-- All objects are prefixed "Iss1712DpcleanupActors_" so they stay clearly separate from
-- anything else already in the maintenance schema.

CREATE SCHEMA IF NOT EXISTS maintenance;

-- ===========================================================================
-- JOB-SPECIFIC prep tables (staging + actor-name resolution).
--
-- This job repairs the PerformedBy actor of synced Correspondence activities
-- (ActorId/Name defaulted to the recipient instead of the real performer --
-- altinn-correspondence#1716). The fix is driven by a CSV, so in addition to
-- the standard candidate table it needs:
--   * a STAGING table holding the raw CSV (loaded by 00a-load-csv.sh), and
--   * an ACTORNAMES resolution table mapping each (actor urn, name) to the
--     public."ActorName" row id to point the actor at (filled by
--     00b_resolve_actornames.sql).
-- Both are dropped again by 99_cleanup.sql.
-- ===========================================================================

-- Raw CSV rows: ISS1712_DPCleanup.csv / ISS1951_DPCleanup.csv
--   (DialogId, DialogActivityId, Timestamp, ActorId, ActorName, ActivityType).
-- UNLOGGED: this table is rebuildable from the CSV and at ISS-1951 scale (~140M
-- rows) holds ~tens of GB -- skipping WAL for the load is a large win. If the DB
-- restarts uncleanly the table truncates; just reload it (00a). The ALTER guard
-- coerces it UNLOGGED even if a prior run created it logged.
CREATE UNLOGGED TABLE IF NOT EXISTS maintenance."Iss1712DpcleanupActors_Staging" (
    "dialog_id"          uuid        NOT NULL,
    "dialog_activity_id" uuid        NOT NULL,
    "source_ts"          timestamptz NOT NULL,
    "actor_id"           text        NOT NULL,  -- urn:altinn:person:identifier-no:...
    "actor_name"         text        NOT NULL,  -- LAST FIRST MIDDLE (legacy ordering)
    "activity_type"      text        NOT NULL   -- CorrespondenceOpened | CorrespondenceConfirmed
);
ALTER TABLE maintenance."Iss1712DpcleanupActors_Staging" SET UNLOGGED;

-- One row per distinct (actor urn, name) seen in staging. "actor_name_entity_id"
-- is the public."ActorName" row to point the PerformedBy actor at; "source"
-- records how it was resolved (see 00b_resolve_actornames.sql):
--   'existing_exact'     -- reused an ActorName row matching the CSV name as-is
--   'existing_reordered' -- reused a row already in the dialogporten#3171-corrected order
--   'inserted'           -- no row existed for this person; created one (CSV order)
-- NB on #3171: person names are currently stored LAST FIRST MIDDLE and the CSV
-- uses the same order. We reuse any existing row (either ordering) and only
-- insert in CSV order when none exists, so we never add a duplicate that would
-- collide under the pending #3171 in-place reorder.
CREATE TABLE IF NOT EXISTS maintenance."Iss1712DpcleanupActors_ActorNames" (
    "actor_id"             text NOT NULL,
    "name"                 text NOT NULL,
    "actor_name_entity_id" uuid NULL,
    "source"               text NULL,
    PRIMARY KEY ("actor_id", "name")
);

-- Candidate table: one row per public."Actor" (the PerformedBy actor of an
-- in-scope activity) we plan to repoint.
--
-- Key shape: a single uuid "EntityId" = public."Actor"."Id". Each in-scope
-- DialogActivity has exactly one PerformedByActor (unique partial index
-- IX_Actor_ActivityId), so Actor."Id" is a natural unique candidate key. The
-- candidate also carries the payload the mutation needs: the resolved target
-- ActorName row id.
--
-- UNLOGGED for the same scale reason (~146M rows). It is rebuildable from staging
-- via 01; if the DB restarts mid-run it truncates and you re-run 01 then resume.
-- Re-processing is safe: the mutation guard (ActorNameEntityId IS DISTINCT FROM
-- target) makes an already-repointed actor a no-op ('skipped_state_changed').
CREATE UNLOGGED TABLE IF NOT EXISTS maintenance."Iss1712DpcleanupActors_Candidates" (
    "EntityId"                uuid        PRIMARY KEY,  -- public."Actor"."Id" (PerformedBy)
    "TargetActorNameEntityId" uuid        NOT NULL,     -- public."ActorName"."Id" to point at
    "EnqueuedAt"              timestamptz NOT NULL DEFAULT now(),
    "ProcessedAt"            timestamptz NULL,
    "WorkerId"               text        NULL,
    "Outcome"                text        NULL  -- 'updated' | 'skipped_state_changed'
);
ALTER TABLE maintenance."Iss1712DpcleanupActors_Candidates" SET UNLOGGED;

-- Partial index supporting the FOR UPDATE SKIP LOCKED claim path.
-- Only unprocessed rows are in the index; once a row is marked processed it
-- drops out, so the claim query stays cheap as the run progresses.
CREATE INDEX IF NOT EXISTS "IX_Iss1712DpcleanupActors_Candidates_Unprocessed"
    ON maintenance."Iss1712DpcleanupActors_Candidates" ("EntityId")
    WHERE "ProcessedAt" IS NULL;

-- Per-batch log -- progress visibility and post-mortem.
CREATE TABLE IF NOT EXISTS maintenance."Iss1712DpcleanupActors_BatchLog" (
    "BatchId"    bigserial   PRIMARY KEY,
    "WorkerId"   text        NOT NULL,
    "StartedAt"  timestamptz NOT NULL DEFAULT now(),
    "FinishedAt" timestamptz NULL,
    "Claimed"    integer     NOT NULL DEFAULT 0,
    "Updated"    integer     NOT NULL DEFAULT 0,
    "Skipped"    integer     NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS "IX_Iss1712DpcleanupActors_BatchLog_WorkerId_BatchId"
    ON maintenance."Iss1712DpcleanupActors_BatchLog" ("WorkerId", "BatchId" DESC);

-- Progress view for at-a-glance status:
--   psql -c 'SELECT * FROM maintenance."Iss1712DpcleanupActors_Progress";'
CREATE OR REPLACE VIEW maintenance."Iss1712DpcleanupActors_Progress" AS
SELECT
    COUNT(*)                                          AS total,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NOT NULL) AS processed,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NULL)     AS remaining,
    COUNT(*) FILTER (WHERE "Outcome" = 'updated')     AS updated,
    COUNT(*) FILTER (WHERE "ProcessedAt" IS NOT NULL
                       AND "Outcome" IS DISTINCT FROM 'updated') AS skipped,
    MIN("ProcessedAt") AS first_processed_at,
    MAX("ProcessedAt") AS last_processed_at
FROM maintenance."Iss1712DpcleanupActors_Candidates";

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
CREATE OR REPLACE PROCEDURE maintenance.iss1712dpcleanupactors_run_batch(
    p_batch_size  integer,
    p_worker_id   text,
    OUT p_claimed integer,
    OUT p_updated integer,
    OUT p_skipped integer
) LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id bigint;
BEGIN
    INSERT INTO maintenance."Iss1712DpcleanupActors_BatchLog" ("WorkerId")
    VALUES (p_worker_id)
    RETURNING "BatchId" INTO v_batch_id;

    WITH claimed AS (
        SELECT "EntityId"
        FROM maintenance."Iss1712DpcleanupActors_Candidates"
        WHERE "ProcessedAt" IS NULL
        ORDER BY "EntityId"
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    ),
    -- >>> JOB-SPECIFIC (edit me) >>>
    -- The repair: repoint the PerformedBy actor's "ActorNameEntityId" to the
    -- ActorName row carrying the real performer (resolved in 00b). SILENT repair:
    -- only "ActorNameEntityId" is written -- "UpdatedAt" is deliberately left
    -- untouched and no outbox row is inserted. "ActorTypeId" (1) and
    -- "Discriminator" are unchanged.
    --
    -- GUARD (re-asserted so a row that changed since candidate-build is skipped,
    -- never double-fixed): only touch it if it is still a PerformedByActor whose
    -- ActorNameEntityId differs from the target (an already-repointed row is a
    -- no-op and is classified 'skipped_state_changed').
    updated AS (
        UPDATE public."Actor" a
        SET    "ActorNameEntityId" = cand."TargetActorNameEntityId"
        FROM   claimed c
        JOIN   maintenance."Iss1712DpcleanupActors_Candidates" cand
                 ON cand."EntityId" = c."EntityId"
        WHERE  a."Id" = c."EntityId"
          AND  a."Discriminator" = 'DialogActivityPerformedByActor'
          AND  a."ActorNameEntityId" IS DISTINCT FROM cand."TargetActorNameEntityId"
        RETURNING a."Id"
    ),
    -- <<< JOB-SPECIFIC <<<
    marked AS (
        UPDATE maintenance."Iss1712DpcleanupActors_Candidates" cand
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

    UPDATE maintenance."Iss1712DpcleanupActors_BatchLog"
    SET "FinishedAt" = now(),
        "Claimed"    = p_claimed,
        "Updated"    = p_updated,
        "Skipped"    = p_skipped
    WHERE "BatchId" = v_batch_id;
END;
$$;
