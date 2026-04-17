#!/usr/bin/env bash

echo "Select environment:"
echo "1) local"
echo "2) azure"

read -p "Enter choice [1-2]: " ENV

case $ENV in
  1)
    DATABASE_NAME=Dialogporten
    DATABASE_USER="postgres"
    DATABASE_PORT=15432
    ;;
  2)
    DATABASE_NAME=dialogporten
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
BATCH_SIZE=5000

TOTAL_UPDATE_COUNT=0
SECONDS=0

ORGS=()
while IFS= read -r line; do
  ORGS+=("$line")
done < <(psql -h "$DATABASE_URL" -p "$DATABASE_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" -qAt -f "./sql/GetOrgs.sql")

for ORG in "${ORGS[@]}"; do
  echo ""
  echo "=== Processing org: $ORG ==="
  ORG_UPDATE_COUNT=0

  while true; do
    RESULT=$(psql -h $DATABASE_URL -p $DATABASE_PORT -U $DATABASE_USER -d $DATABASE_NAME -f "./sql/UpdateDialogsFilterStrategy.sql" -q -t -A --pset=footer=off -v ON_ERROR_STOP=1 -v org=$ORG -v batchSize=$BATCH_SIZE)

    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Error: SQL execution failed (exit code $EXIT_CODE)" >&2
        exit $EXIT_CODE
    fi
    IFS='|' read -r UPDATED <<< "$RESULT"

    ORG_UPDATE_COUNT=$((ORG_UPDATE_COUNT + UPDATED))
    TOTAL_UPDATE_COUNT=$((TOTAL_UPDATE_COUNT + UPDATED))

    echo "[$ORG] Updated $UPDATED rows (total $TOTAL_UPDATE_COUNT) - elapsed ${SECONDS}s"

    if [ "$UPDATED" -eq 0 ]; then
      echo "[$ORG] Done. Total updated $ORG_UPDATE_COUNT rows in ${SECONDS}s"
      break
    fi
  done
done

echo ""
echo "All Done. Total updated $TOTAL_UPDATE_COUNT rows in ${SECONDS}s"
