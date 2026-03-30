function get_orgs() {
  local db_url=$1
  local db_port=$2
  local db_user=$3
  local db_name=$4

  ORGS=()
  while IFS= read -r line; do
    ORGS+=("$line")
  done < <(psql -h "$db_url" -p "$db_port" -U "$db_user" -d "$db_name" -qAt -c "
    WITH RECURSIVE orgs AS (
      SELECT MIN(\"Org\") AS org FROM \"Dialog\"
      UNION ALL
      SELECT (SELECT MIN(\"Org\") FROM \"Dialog\" WHERE \"Org\" > orgs.org)
      FROM orgs
      WHERE orgs.org IS NOT NULL
    )
    SELECT org FROM orgs WHERE org IS NOT NULL
  ")
}
