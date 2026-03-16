#!/bin/bash

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

DATABASE_URL=localhost
DATABASE_NAME=Dialogporten
BATCH_SIZE=5000
TOTAL=0
SECONDS=0

export PGPASSFILE="./.pgpass"

while true; do
  UPDATED=$(psql -h $DATABASE_URL -p $DATABASE_PORT -U $DATABASE_USER -d $DATABASE_NAME -qAt -c "
    SET lock_timeout = '2s';
    SET statement_timeout = '30s';

    WITH cte AS (
      SELECT \"Id\"
      FROM \"Dialog\"
      WHERE \"ContentUpdatedAt\" < '2025-12-01'
        AND \"IsSeenSinceLastContentUpdate\" = false
      LIMIT $BATCH_SIZE
    )
    UPDATE \"Dialog\" d
    SET \"IsSeenSinceLastContentUpdate\" = true
    FROM cte
    WHERE d.\"Id\" = cte.\"Id\"
    RETURNING 1;
  " | wc -l)

  TOTAL=$((TOTAL + UPDATED))
  echo "Updated $UPDATED rows (total $TOTAL) - elapsed ${SECONDS}s"

  if [ "$UPDATED" -eq 0 ]; then
    echo "Done. Total updated $TOTAL rows in ${SECONDS}s"
    break
  fi
done
