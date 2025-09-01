#!/usr/bin/env bash
set -euo pipefail

red()   { printf "\033[31m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
yellow(){ printf "\033[33m%s\033[0m\n" "$*"; }

echo "🔎 Checking Docker..."
docker version >/dev/null 2>&1 || { red "Docker not available"; exit 1; }

echo "🔎 Checking compose services..."
docker compose ps || { red "compose not working"; exit 1; }

if ! docker compose ps db | grep -q "Up"; then
  yellow "DB not running; starting..."
  docker compose up -d db
fi

echo "🔎 Waiting for DB to be healthy..."
tries=20
until docker compose ps db | grep -q "healthy"; do
  sleep 1
  tries=$((tries-1))
  if [ $tries -le 0 ]; then red "DB never became healthy"; exit 1; fi
done
green "DB is healthy."

echo "🔎 psql connectivity test..."
docker compose exec db psql -U dovah -d dovah -c 'select 1' >/dev/null

echo "🔎 Listing tables..."
docker compose exec db psql -U dovah -d dovah -c '\dt' | tee /tmp/_dt.out
if grep -q " hdfs_logs " /tmp/_dt.out; then
  green "✅ Table exists: public | hdfs_logs"
else
  yellow "Table missing; applying migration..."
  docker compose exec -T db psql -U dovah -d dovah -v ON_ERROR_STOP=1 -f - < db/migrations/0001_create_hdfs_logs.sql
  docker compose exec db psql -U dovah -d dovah -c '\dt' | tee /tmp/_dt2.out
  if grep -q " hdfs_logs " /tmp/_dt2.out; then
    green "✅ Table created: public | hdfs_logs"
  else
    red "❌ Still missing after migration. See docker logs and SQL file."
    exit 1
  fi
fi

echo "🔎 Describing table..."
docker compose exec db psql -U dovah -d dovah -c '\d+ hdfs_logs' || true
green "Done."
