#!/usr/bin/env bash

echo "Select environment:"
echo "1) local"
echo "2) azure"

read -p "Enter choice [1-2]: " ENV

case $ENV in
  1)
    DATABASE_USER="postgres"
    DATABASE_PORT=15432
    ;;
  2)
    DATABASE_USER="dialogportenPgAdmin"
    DATABASE_PORT=5431
    ;;
  *)
    echo "Invalid option"
    exit 1
    ;;
esac

export PGPASSFILE="./.pgpass"

DATABASE_URL=localhost
DATABASE_NAME=Dialogporten
BATCH_SIZE=5000

TOTAL_UPDATE_COUNT=0
SECONDS=0

LAST_ID="00000000-0000-0000-0000-000000000000"

while true; do
  RESULT=$(psql -h $DATABASE_URL -p $DATABASE_PORT -U $DATABASE_USER -d $DATABASE_NAME -f "./sql/UpdateDialogsRecomputeStrategy_Id_Covering.sql" -q -t -A -v ON_ERROR_STOP=1 -v lastId=$LAST_ID -v batchSize=$BATCH_SIZE)

  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
      echo "Error: SQL execution failed (exit code $EXIT_CODE)" >&2
      exit $EXIT_CODE
  fi

  LAST_ID=$(echo "$RESULT" | tail -n 1)
  UPDATED=$(echo "$RESULT" | grep -c .)

  TOTAL_UPDATE_COUNT=$((TOTAL_UPDATE_COUNT + UPDATED))

  echo "Updated $UPDATED rows (total $TOTAL_UPDATE_COUNT) - elapsed ${SECONDS}s. Last ID: $LAST_ID"

  if [ "$UPDATED" -eq 0 ]; then
    echo "Done. Total updated $TOTAL_UPDATE_COUNT rows in ${SECONDS}s"
    break
  fi
done

echo ""
echo "All Done. Total updated $TOTAL_UPDATE_COUNT rows in ${SECONDS}s"
