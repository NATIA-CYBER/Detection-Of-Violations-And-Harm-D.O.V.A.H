# DOVAH — Detection of Violations & Harm

Real-time, drift-aware security analytics:
- Ingest logs → compute windowed features
- Score with unsupervised baselines
- Fuse scores → persist detections with evidence
- Summarize alerts & export signed evidence packs
- Report precision/recall/precision@k/FP/1k and **p95 latency**

---

## 1) Requirements
- Conda (Anaconda/Miniconda/Mamba), Python **3.11**
- PostgreSQL **14+**
- Git, Make (optional)
- (Optional) Docker for a local DB

---

## 2) Environment setup (Conda only)
```bash
conda env create -f environment.yml || conda env update -f environment.yml
conda activate dovah
conda config --set solver libmamba
pre-commit install
```

Optional: reproducible installs via conda-lock
```bash
conda install -n base -c conda-forge conda-lock -y
conda run -n base conda-lock -f environment.yml -p osx-64 -p linux-64 -p win-64
conda-lock install -n dovah conda-lock.yml
conda activate dovah
```

## 3) Database configuration (Alembic owns the schema)
```bash
# Local Postgres (Docker example)
docker run --name dovah-db -e POSTGRES_USER=dovah -e POSTGRES_PASSWORD=dovah \
  -e POSTGRES_DB=dovah -p 5432:5432 -d postgres:16

# App config
cp .env.example .env
# Edit .env to set:
# DATABASE_URL=postgresql://dovah:dovah@localhost:5432/dovah

# Load env vars
set -a; source .env; set +a

# Migrations (one-time, idempotent)
alembic upgrade head
```

## 4) Fetch enrichment (optional)
```bash
python -m src.ingest.epss_fetch --out data/epss/latest.csv
python -m src.ingest.kev_fetch  --out data/kev/latest.json
```

## 5) Ingest logs & compute window features
```bash
# Parse raw HDFS logs → normalized events (adjust input path)
python -m src.ingest.hdfs_loader \
  --input data/hdfs/raw_logs.jsonl \
  --out data/hdfs/parsed_logs_latest.json

# Local streaming stub: 60s windows sliding by 10s → writes to table window_features
python -m src.stream.job --input data/hdfs/parsed_logs_latest.json
```

## 6) Baseline scoring
```bash
# Isolation Forest on window stats (writes scores, or outputs file depending on config)
python -m src.models.anomaly.iforest

# Log-LM / template perplexity scorer (session/window scores)
python -m src.models.log_lm.score
```

## 7) Late fusion → detections
```bash
# Combines baseline scores (weighted), applies threshold,
# persists rows in detections with created_at for latency metrics
python -m src.fusion.late_fusion
```

## 8) Evaluate (prints metrics incl. p95 latency)
```bash
# With labels
python -m src.eval.run_eval \
  --pred data/out/pred.jsonl \
  --labels data/labels/labels.jsonl \
  --k 50

# Without labels (still reports p95 if available)
python -m src.eval.run_eval --pred data/out/pred.jsonl --total-windows 10000
```

Metrics:
- precision, recall, precision@k, FP/1k
- p95_ms (end-to-end latency percentile)

## 9) Summarize & export evidence
```bash
python -m src.genai.summarize_alert --id <DETECTION_ID> --out evidence/summary_<ID>.json
python -m src.integrations.export_json --out evidence/
```

## 10) Quality gates
```bash
ruff .
black --check .
mypy src
pytest -q
```

## 11) Make targets (if you use Make)
```bash
make env
make migrate
make lint
make test
make audit
make lock
```

## 12) Troubleshooting
- DB not reachable / sqlalchemy.url missing — Load .env as shown and ensure Postgres is up (pg_isready).
- Migrations complain about existing tables — Drop/recreate dev DB or let Alembic own schema from scratch.
- conda-lock errors — Run it from the base env or upgrade pydantic (>= 2.7) in the env executing it.
- No detections — Ensure window_features has rows and baselines produced outputs before running fusion.
