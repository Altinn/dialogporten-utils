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

DATABASE_NAME=Dialogporten
BATCH_SIZE=5000

TOTAL_UPDATE_COUNT=0
SECONDS=0

ORGS=()
while IFS= read -r line; do
  ORGS+=("$line")
done < <(psql -h "$DATABASE_URL" -p "$DATABASE_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" -qAt -f "./sql/GetOrgs.sql")

FOLDER_NAME="out"
FILENAME_LAST_ID="$FOLDER_NAME/$(basename "$0")_$(date +%Y%m%d_%H%M%S).txt"

mkdir -p $FOLDER_NAME

for ORG in "${ORGS[@]}"; do
  echo ""
  echo "=== Processing org: $ORG ==="
  ORG_UPDATE_COUNT=0
  LAST_ID="00000000-0000-0000-0000-000000000000"

  while true; do
    RESULT=$(psql -h $DATABASE_URL -p $DATABASE_PORT -U $DATABASE_USER -d $DATABASE_NAME -f "./sql/UpdateDialogsRecomputeStrategy_Org_CreatedAt_Id.sql" -q -t -A --pset=footer=off -v ON_ERROR_STOP=1 -v org=$ORG -v lastId=$LAST_ID -v batchSize=$BATCH_SIZE)

    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Error: SQL execution failed (exit code $EXIT_CODE)" >&2
        exit $EXIT_CODE
    fi

    IFS='|' read -r UPDATED LAST_ID <<< "$RESULT"

    if [ -n "$LAST_ID" ]; then
      echo "$LAST_ID" > "$FILENAME_LAST_ID"
    fi

    ORG_UPDATE_COUNT=$((ORG_UPDATE_COUNT + UPDATED))
    TOTAL_UPDATE_COUNT=$((TOTAL_UPDATE_COUNT + UPDATED))

    echo "[$ORG] Updated: $UPDATED. So far, this org: $ORG_UPDATE_COUNT. So far, total: $TOTAL_UPDATE_COUNT - elapsed ${SECONDS}s. Last ID: $LAST_ID"

    if [ "$UPDATED" -eq 0 ]; then
      echo "[$ORG] Done. Total updated $TOTAL_UPDATE_COUNT rows in ${SECONDS}s"
      break
    fi
  done
done

echo ""
echo "All Done. Total updated $TOTAL_UPDATE_COUNT rows in ${SECONDS}s"
