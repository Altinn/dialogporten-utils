#!/usr/bin/env bash

echo "Select environment:"
echo "1) local"
echo "2) azure"

read -p "Enter choice [1-2]: " ENV

case $ENV in
  1)
    DATABASE_URL=localhost
    DATABASE_NAME=Dialogporten
    DATABASE_USER="postgres"
    DATABASE_PORT=15432
    ;;
  2)
    echo "Enter database URL"
    read DATABASE_URL
    DATABASE_NAME=dialogporten
    DATABASE_USER="dialogportenPgAdmin"
    DATABASE_PORT=5432
    ;;
  *)
    echo "Invalid option"
    exit 1
    ;;
esac

BATCH_SIZE=5000

TOTAL_UPDATE_COUNT=0
SECONDS=0

ORGS=()
while IFS= read -r line; do
  ORGS+=("$line")
done < <(psql -h "$DATABASE_URL" -p "$DATABASE_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" -qAt -f "./sql/GetOrgs.sql")

FOLDER_NAME="out"
FILENAME_LOG="$FOLDER_NAME/$(basename "$0")_$(date +%Y%m%d_%H%M%S).txt"

function log {
  echo "$1"
  echo "$1" >> "$FILENAME_LOG"
}

mkdir -p $FOLDER_NAME

for ORG in "${ORGS[@]}"; do
  log ""
  log "=== Processing org: $ORG ==="
  ORG_UPDATE_COUNT=0
  LAST_ID="00000000-0000-0000-0000-000000000000"
  LAST_ContentUpdatedAt=$(date -u +'%Y-%m-%d %H:%M:%S')

  while true; do
    RESULT=$(psql -h $DATABASE_URL -p $DATABASE_PORT -U $DATABASE_USER -d $DATABASE_NAME -f "./sql/UpdateDialogsRecomputeStrategy_Org_ContentUpdatedAt_Id.sql" -q -t -A --pset=footer=off -v ON_ERROR_STOP=1 -v org="$ORG" -v lastId="$LAST_ID" -v lastContentUpdatedAt="$LAST_ContentUpdatedAt" -v batchSize=$BATCH_SIZE)

    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        log "Error: SQL execution failed (exit code $EXIT_CODE)" >&2
        exit $EXIT_CODE
    fi

    IFS='|' read -r UPDATED LAST_ID LAST_ContentUpdatedAt <<< "$RESULT"

    ORG_UPDATE_COUNT=$((ORG_UPDATE_COUNT + UPDATED))
    TOTAL_UPDATE_COUNT=$((TOTAL_UPDATE_COUNT + UPDATED))

    log "[$ORG] Updated: $UPDATED. So far, this org: $ORG_UPDATE_COUNT. So far, total: $TOTAL_UPDATE_COUNT - elapsed ${SECONDS}s. Last ID: $LAST_ID"

    if [ "$UPDATED" -eq 0 ]; then
      log "[$ORG] Done. Total updated $TOTAL_UPDATE_COUNT rows in ${SECONDS}s"
      break
    fi
  done
done

log ""
log "All Done. Total updated $TOTAL_UPDATE_COUNT rows in ${SECONDS}s"
