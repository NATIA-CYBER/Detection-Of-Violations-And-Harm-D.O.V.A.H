#!/usr/bin/env bash
set -euo pipefail

echo "[doctor] Checking Docker..."
docker version >/dev/null 2>&1 || { echo "[doctor] ERROR: Docker not available"; exit 1; }

echo "[doctor] Checking compose services..."
docker compose ps >/dev/null 2>&1 || { echo "[doctor] ERROR: docker compose not working"; exit 1; }

if ! docker compose ps db | grep -q "Up"; then
  echo "[doctor] DB not running; starting..."
  docker compose up -d db
fi

echo "[doctor] Waiting for DB to be healthy..."
tries=20
until docker compose ps db | grep -q "healthy"; do
  sleep 1
  tries=$((tries-1))
  if [ $tries -le 0 ]; then echo "[doctor] ERROR: DB never became healthy"; exit 1; fi
done
echo "[doctor] DB is healthy."

echo "[doctor] psql connectivity test..."
docker compose exec db psql -U dovah -d dovah -c 'select 1' >/dev/null

echo "[doctor] Listing tables..."
docker compose exec db psql -U dovah -d dovah -c '\dt' | tee /tmp/_dt.out
if grep -q " hdfs_logs " /tmp/_dt.out; then
  echo "[doctor] Table exists: public | hdfs_logs"
else
  echo "[doctor] Table missing; applying migration from stdin..."
  docker compose exec -T db psql -U dovah -d dovah -v ON_ERROR_STOP=1 -f - < db/migrations/0001_create_hdfs_logs.sql
  docker compose exec db psql -U dovah -d dovah -c '\dt' | tee /tmp/_dt2.out
  if grep -q " hdfs_logs " /tmp/_dt2.out; then
    echo "[doctor] Table created: public | hdfs_logs"
  else
    echo "[doctor] ERROR: Still missing after migration. Inspect docker logs and SQL file."
    exit 1
  fi
fi

echo "[doctor] Describing table:"
docker compose exec db psql -U dovah -d dovah -c '\d+ hdfs_logs' || true
echo "[doctor] Done."
