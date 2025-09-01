# Step 1 — Troubleshooting: `public | hdfs_logs` not showing up

**Symptom:** After `make db-verify`, you don’t see the table in the list (no line like `public | hdfs_logs`).

## Quick fixes (do in order)
```bash
make db-up
make db-migrate
make db-verify
