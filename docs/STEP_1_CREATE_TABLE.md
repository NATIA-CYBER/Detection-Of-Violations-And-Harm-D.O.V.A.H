# Step 1 — Create the `hdfs_logs` table

**Goal:** Prepare Postgres so `src/ingest/hdfs_loader.py` can write rows.

## Quick start
```bash
make db-migrate
make db-verify
