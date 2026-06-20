-- iss1712-dpcleanup-actors step 01: build the candidate set.
--
-- Populates maintenance."Iss1712DpcleanupActors_Candidates" with one row per entity that
-- needs repair. Safe to re-run -- ON CONFLICT DO NOTHING absorbs duplicates if the
-- script errors partway and is restarted.
--
-- Run the read-only 02_dry_run_count.sql FIRST to validate the predicate and
-- estimate scope before inserting anything.

-- >>> JOB-SPECIFIC (edit me) >>>
-- Build one candidate per PerformedBy actor to repoint. Prerequisites:
-- 00a-load-csv.sh (staging) and 00b_resolve_actornames.sql (resolution) first.
--
-- A staging row matches a real activity on the agreed key DialogId +
-- DialogActivityId + Timestamp -- specifically DialogActivity."Id" = the CSV
-- DialogActivityId AND "DialogId" = the CSV DialogId AND "CreatedAt" = the CSV
-- Timestamp (verified equal to the ms), plus a defensive "TypeId" cross-check
-- against the CSV ActivityType (16 = CorrespondenceOpened, 17 = Confirmed).
--
-- Duplicate DialogActivityIds: the CSV may list several rows per activity. After
-- the timestamp match we keep an activity ONLY when all matched rows agree on a
-- single (actor urn, name). Activities with conflicting actors are intentionally
-- excluded (reported as ambiguous in 02) for manual follow-up, never guessed.
--
-- The candidate "EntityId" is the PerformedBy Actor."Id"; we skip rows already
-- pointing at the resolved target (idempotent / partially-fixed).
--
-- SCALE: at ISS-1951 volume (~140M staging rows probing billion-row DialogActivity
-- and Actor) a single matched->unambiguous->insert statement would spill huge temp
-- and have to restart from scratch on any failure. So we run it in a DO loop of
-- KEYSET chunks over dialog_activity_id: each iteration finds the id ~CHUNK_ROWS
-- rows ahead via the IX_..._Staging_ActivityId index (an index-only OFFSET scan),
-- then processes the half-open id range (cursor, hi].
--
-- Why keyset and not fixed uuid-prefix buckets: these ids are UUIDv7, so the high
-- bytes are a millisecond timestamp -- every synced id in this era starts 0x01...,
-- so a first-byte split would pile ~everything into one bucket. Keyset boundaries
-- are derived from the data, so chunks stay balanced for ANY distribution.
--
-- Why this never splits an activity: the chunk predicate is on dialog_activity_id
-- itself, and all duplicate rows of one activity share that id, so they are always
-- entirely inside or outside a range -- the per-activity ambiguity check stays
-- correct. Re-running is safe (ON CONFLICT DO NOTHING).
DO $$
DECLARE
    chunk_rows  bigint := 1000000;  -- staging rows per chunk; tune for memory/progress
    cursor_id   uuid   := NULL;     -- exclusive lower bound; NULL = -infinity
    hi          uuid;               -- inclusive upper bound; NULL on the final chunk
    inserted    bigint;
    total       bigint := 0;
    chunk_no    int    := 0;
BEGIN
    LOOP
        -- Boundary: the dialog_activity_id chunk_rows rows past the cursor. NULL
        -- means fewer than chunk_rows remain -> this is the last chunk.
        SELECT s."dialog_activity_id" INTO hi
        FROM maintenance."Iss1712DpcleanupActors_Staging" s
        WHERE cursor_id IS NULL OR s."dialog_activity_id" > cursor_id
        ORDER BY s."dialog_activity_id"
        OFFSET chunk_rows LIMIT 1;

        WITH matched AS (
            SELECT a."Id" AS activity_id, s."actor_id", s."actor_name"
            FROM maintenance."Iss1712DpcleanupActors_Staging" s
            JOIN public."DialogActivity" a
                   ON a."Id"        = s."dialog_activity_id"
                  AND a."DialogId"  = s."dialog_id"
                  AND a."CreatedAt" = s."source_ts"
                  AND a."TypeId"    = CASE s."activity_type"
                                          WHEN 'CorrespondenceOpened'   THEN 16
                                          WHEN 'CorrespondenceConfirmed' THEN 17
                                      END
            WHERE (cursor_id IS NULL OR s."dialog_activity_id" >  cursor_id)
              AND (hi        IS NULL OR s."dialog_activity_id" <= hi)
        ),
        unambiguous AS (  -- exactly one agreed actor for the activity
            SELECT activity_id,
                   min("actor_id")   AS actor_id,
                   min("actor_name") AS actor_name
            FROM matched
            GROUP BY activity_id
            HAVING COUNT(DISTINCT ("actor_id", "actor_name")) = 1
        ),
        ins AS (
            INSERT INTO maintenance."Iss1712DpcleanupActors_Candidates"
                ("EntityId", "TargetActorNameEntityId")
            SELECT act."Id", an."actor_name_entity_id"
            FROM unambiguous u
            JOIN public."Actor" act
                   ON act."ActivityId"    = u.activity_id
                  AND act."Discriminator" = 'DialogActivityPerformedByActor'
            JOIN maintenance."Iss1712DpcleanupActors_ActorNames" an
                   ON an."actor_id" = u.actor_id
                  AND an."name"     = u.actor_name
                  AND an."actor_name_entity_id" IS NOT NULL
            WHERE act."ActorNameEntityId" IS DISTINCT FROM an."actor_name_entity_id"  -- skip already-correct
            ON CONFLICT ("EntityId") DO NOTHING
            RETURNING 1
        )
        SELECT count(*) INTO inserted FROM ins;

        chunk_no := chunk_no + 1;
        total    := total + inserted;
        RAISE NOTICE 'chunk % (up to %): inserted=%, running_total=%',
            chunk_no, COALESCE(hi::text, '(max)'), inserted, total;

        EXIT WHEN hi IS NULL;
        cursor_id := hi;
    END LOOP;
    RAISE NOTICE 'candidate build complete: % inserted across % chunks', total, chunk_no;
END $$;
-- <<< JOB-SPECIFIC <<<

-- Report what was built (generic).
SELECT COUNT(*)                                       AS candidates_total,
       COUNT(*) FILTER (WHERE "ProcessedAt" IS NULL)  AS unprocessed
FROM maintenance."Iss1712DpcleanupActors_Candidates";
