#!/bin/bash

source ./backfill-get-orgs.sh
echo "Select environment:"
echo "1) local"
echo "2) azure"

read -p "Enter choice [1-2]: " ENV

case $ENV in
  1)
    DATABASE_USER="postgres"
    DATABASE_PORT=5432
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

get_orgs "$DATABASE_URL" "$DATABASE_PORT" "$DATABASE_USER" "$DATABASE_NAME"

for ORG in "${ORGS[@]}"; do
  echo ""
  echo "=== Processing org: $ORG ==="
  ORG_UPDATE_COUNT=0

  while true; do
    UPDATED=$(psql -h $DATABASE_URL -p $DATABASE_PORT -U $DATABASE_USER -d $DATABASE_NAME -qAt -c "
      SET lock_timeout = '2s';
      SET statement_timeout = '60s';

      WITH
        CTE AS (
          SELECT
            \"Id\"
          FROM
            \"Dialog\" d
          WHERE
            \"Org\" = '$ORG'
            AND \"ContentUpdatedAt\" >= '2025-12-01'
            AND \"IsSeenSinceLastContentUpdate\" = false
            AND EXISTS (
                  SELECT 1
                  FROM \"DialogSeenLog\" s
                  WHERE d.\"Id\" = s.\"DialogId\"
                    AND d.\"ContentUpdatedAt\" < s.\"CreatedAt\"
                )
          LIMIT
            $BATCH_SIZE
      )
      UPDATE \"Dialog\" d
      SET \"IsSeenSinceLastContentUpdate\" = true
      FROM cte
      WHERE d.\"Id\" = cte.\"Id\"
      RETURNING 1;
    " | wc -l)

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
