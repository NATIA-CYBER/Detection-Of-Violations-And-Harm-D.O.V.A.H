# DOVAH — Detection of Violations & Harm

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -U pip && pip install -e .
make smoke-imports      # import sanity
make smoke-iforest      # tiny no-DB scoring

Real-time, drift-aware security analytics:
- Ingest logs → compute windowed features
- Score with unsupervised baselines
- Fuse scores → persist detections with evidence
- Summarize alerts & export signed evidence packs
- Report precision/recall/precision@k/FP/1k and **p95 latency**

## Quick Start (Docker)

### Prerequisites
- Docker Desktop 4.30+ (or Docker Engine 24+) is running
- ≥ 2 GB free RAM (4 GB recommended) and ~2 GB disk for build
- Git installed (and `make` recommended)
- Internet access for base images
  - Windows: enable **WSL2** backend
  - Apple Silicon: use `--platform linux/amd64` if needed

To verify the system:

```bash
# Build the container
docker build -t dovah .

# Run verification
docker run --rm -v $PWD:/app dovah make verify_day2
```

This will:
- Build a reproducible environment with all dependencies
- Run core PII and schema tests
- Execute analysis with sample data
- Verify PII scrubbing and outputs

### Docker Troubleshooting

- **Tests fail or Out of Memory**
  - Increase Docker Desktop memory (Resources → 4-6 GB)
  - Run with limit: `docker run -m 4g --rm -v $PWD:/app dovah make verify_day2`

- **Slow Build**
  - Enable BuildKit: `DOCKER_BUILDKIT=1 docker build .`
  - Pre-pull base: `docker pull python:3.11.9-slim`

- **Volume Mount Issues**
  - macOS/Windows: Check Docker Desktop → File sharing
  - Linux: Use `-v "$PWD:/app:Z"` for SELinux
  - Permission errors: `--user $(id -u):$(id -g)`

- **Apple Silicon Issues**
  - Build: `docker build --platform linux/amd64 .`
  - Run: `docker run --platform linux/amd64 ...`

- **Clean Rebuild**
  - `docker compose down -v --rmi local && docker compose up --build`
  - Or: `docker system prune -f && docker build .`

- **View Logs**
  - `docker compose logs -f`
  - Or: `docker logs <container-id>`

For development or running specific components, see the sections below.

---

## 1) Requirements
- Conda (Anaconda/Miniconda/Mamba), Python **3.11.9**
- PostgreSQL **16.1**
- Git, Make (optional)
- Docker Desktop 4.30+ or Docker Engine 24+ for containerized builds

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

## 3) Docker Setup

### Quick Verification
```bash
# Build and verify in one go
docker-compose run --rm app make verify_day2
```

### Full Development Setup
```bash
# Start all services
docker-compose up -d

# Run specific components
docker-compose run --rm app python -m src.ingest.hdfs_loader
docker-compose run --rm app python -m src.analysis.run_analysis

# Run tests
docker-compose run --rm app pytest

# Stop all services
docker-compose down
```

### Environment Variables
Key variables (all configured in docker-compose.yml):
- `DOVAH_TENANT_SALT`: Set to 'dev_only_do_not_use' for deterministic testing
- `DOVAH_SESSION_WINDOW`: Session bucket size in seconds (default: 300)
- `DATABASE_URL`: PostgreSQL connection string

## 4) Database configuration (Alembic owns the schema)
```bash
# Local Postgres (Docker example)
docker run --name dovah-db -e POSTGRES_USER=dovah -e POSTGRES_PASSWORD=dovah \
  -e POSTGRES_DB=dovah -p 5432:5432 -d postgres:16

# App config
cp .env.example .env
# Edit .env to set:
# DATABASE_URL=postgresql+psycopg://dovah:dovah@localhost:5432/dovah

# Load env vars (bash/zsh)
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
# Isolation Forest on window stats
python -m src.models.anomaly.iforest

# Log-LM / template perplexity scorer
python -m src.models.log_lm.score
```

## 7) Late fusion → detections
```bash
# Combines baseline scores (weighted), applies threshold,
# persists rows in detections with created_at for latency metrics
python -m src.fusion.late_fusion
```

## Day-4 evaluation files (val/test + fusion)

**Goal:** produce the two files the evaluation needs:

- `data/val/fusion.jsonl`
- `data/test/fusion.jsonl`

They are built from **events** + **labels** via two steps: (1) prep val/test sets; (2) join predictions with labels.

**Prep (from repo root):**
```bash
# Use your REAL data paths for a proper split:
python -m scripts.prep_day4 \
  --events /ABS/PATH/TO/your_events.jsonl \
  --epss   /ABS/PATH/TO/epss_scores.csv \
  --kev    /ABS/PATH/TO/kev_entries.json \
  --val-end  2025-07-31T23:59:59Z \
  --test-start 2025-08-01T00:00:00Z

# If timestamps are messy, use a hash split instead:
# python -m scripts.prep_day4 --events /PATH/events.jsonl --epss /PATH/epss.csv --kev /PATH/kev.json --val-ratio 0.5

##One-time setup/makes script executable
chmod +x scripts/build_fusion.sh 

##Option A — pass paths inline
VAL_IN="path/to/val.jsonl" \
TEST_IN="path/to/test.jsonl" \
VAL_LABELS="data/val/labels.jsonl" \
TEST_LABELS="data/test/labels.jsonl" \
./scripts/build_fusion.sh

##Option B — hard-code once,runs after
./scripts/build_fusion.sh

##Targeted (CI)
make build-fusion \
  VAL_IN=path/to/val.jsonl \
  TEST_IN=path/to/test.jsonl \
  VAL_LABELS=data/val/labels.jsonl \
  TEST_LABELS=data/test/labels.jsonl
# If you hard-coded paths in the script, plain `make build-fusion` works.

#Sanity check
ls -lh data/val/fusion.jsonl data/test/fusion.jsonl
head -n 2 data/val/fusion.jsonl
head -n 2 data/test/fusion.jsonl

### Day-4 data prep (events + labels)
If the required files don’t exist yet, build them from the sample bundle:

```bash
python -m scripts.prep_day4 \
  --events sample_data/hdfs/sample.jsonl \
  --epss   sample_data/security/epss_scores.csv \
  --kev    sample_data/security/kev_entries.json \
  --out-root sample_data/hdfs \
  --val-ratio 0.5 \
  --epss-threshold 0.6 \
  --min-events 500   # demo-only; omit if you have real volume

# day-4 done 
make calibrate MODEL=fusion VAL_PRED=data/val/fusion.jsonl FP1K=5
make artifacts  MODEL=fusion TEST_PRED=data/test/fusion.jsonl
make phase4-accept


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
DB not reachable / sqlalchemy.url missing — Load .env as shown and ensure Postgres is up (pg_isready).

Migrations complain about existing tables — Drop/recreate dev DB or let Alembic own schema from scratch.

conda-lock errors — Run it from the base env or upgrade pydantic (>= 2.7) in the env executing it.

No detections — Ensure window_features has rows and baselines produced outputs before running fusion.
