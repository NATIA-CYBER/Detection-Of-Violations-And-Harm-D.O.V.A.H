#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/db_apply.sh                 # apply via Docker (default)
#   DB_MODE=host scripts/db_apply.sh    # apply using local psql on port 5433

MIG_DIR="db/migrations"

if [[ "${DB_MODE:-docker}" == "host" ]]; then
  # Host mode: requires psql installed locally; DB must be exposed on 5433
  CONN_STR="postgresql://dovah:dovah@localhost:5433/dovah"
  for f in $(ls -1 ${MIG_DIR}/*.sql | sort); do
    echo "Applying $f (host)..."
    psql "$CONN_STR" -v ON_ERROR_STOP=1 -f "$f"
  done
else
  # Docker mode: run psql inside the 'db' service
  for f in $(ls -1 ${MIG_DIR}/*.sql | sort); do
    echo "Applying $f (docker)..."
    docker compose exec -T db psql -U dovah -d dovah -v ON_ERROR_STOP=1 -f - < "$f"
  done
fi

echo "✅ Migrations applied."
