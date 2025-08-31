#!/usr/bin/env bash
set -euo pipefail

if [[ "${DB_MODE:-docker}" == "host" ]]; then
  psql "postgresql://dovah:dovah@localhost:5433/dovah" -c '\dt' -c '\d+ hdfs_logs'
else
  docker compose exec db psql -U dovah -d dovah -c '\dt' -c '\d+ hdfs_logs'
fi
