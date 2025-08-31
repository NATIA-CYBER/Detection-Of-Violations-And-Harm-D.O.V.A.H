# ==== Config ====
-include .env
export
ENV ?= dovah
PY  ?= conda run -n $(ENV) python
SH  ?= conda run -n $(ENV) sh -c

# Phase selector (defaults to phase3; override with PHASE=phase4, etc.)
PHASE ?= phase3

# Streaming demo defaults
DATA    ?= sample_data/hdfs/sample.jsonl
WINDOW  ?= 10
STRIDE  ?= 1
EPS     ?= 100
WARMUP  ?= 10
DUR     ?= 120
SLA_MS  ?= 800

# Day-4 artifacts defaults (override at call-site)
MODEL     ?= fusion
VAL_PRED  ?= data/val/fusion.jsonl
TEST_PRED ?= data/test/fusion.jsonl
FP1K      ?= 5.0

# Fusion builder defaults (override at call-site)
VAL_IN      ?= sample_data/hdfs/val/events.jsonl
TEST_IN     ?= sample_data/hdfs/test/events.jsonl
VAL_LABELS  ?= data/val/labels.jsonl
TEST_LABELS ?= data/test/labels.jsonl

REPORTS := reports/$(PHASE)
METRICS := $(REPORTS)/metrics
LOGS    := $(REPORTS)/logs
IMG     := $(REPORTS)/img

.PHONY: help env deps phase-dirs phase-run phase-accept phase-test phase-all \
        split-data train evaluate \
        streamlit-run clean-phase clean migrate-day3-to-phase3 \
        phase3-run phase3-accept phase3-test phase3-all \
        phase4-run phase4-accept phase4-test phase4-all \
        smoke-imports smoke-iforest smokes \
        calibrate artifacts artifacts-all \
        ingest ingest-files ingest-db data-day4 build-fusion day4 fix-perms print-env

help:
	@echo "Targets:"
	@echo "  env                  - create/update conda env + pip deps"
	@echo "  phase-dirs           - create report dirs for current PHASE ($(PHASE))"
	@echo "  phase-run            - run replay -> features (writes features.jsonl, latency.csv)"
	@echo "  phase-accept         - gate: p95(lat_ms) < $(SLA_MS)ms (exit non-zero on fail)"
	@echo "  phase-test           - run unit tests for streaming features"
	@echo "  phase-all            - run dirs + pipeline + acceptance + unit tests"
	@echo "  streamlit-run        - run the Streamlit UI locally"
	@echo "  smokes               - import + tiny IForest sanity (no DB)"
	@echo "  ingest               - fetch EPSS/KEV to disk (data/security/*)"
	@echo "  ingest-db            - load EPSS/KEV into Postgres (uses DATABASE_URL)"
	@echo "  data-day4            - build val/test events+labels (scripts/prep_day4.py)"
	@echo "  build-fusion         - run harness + join preds+labels -> fusion.jsonl"
	@echo "  calibrate            - choose threshold on validation (writes docs/metrics/thresholds.json)"
	@echo "  artifacts            - PR/ROC PNGs + metrics CSV on test (writes to docs/metrics/)"
	@echo "  day4                 - data-day4 + build-fusion + calibrate + artifacts"

print-env:
	@echo "ENV=$(ENV)"
	@echo "DATABASE_URL=$${DATABASE_URL}"

env:
	conda env update -f environment.yml --prune
	$(PY) -m pip install -e .

deps: env  ## alias

phase-dirs:
	mkdir -p $(METRICS) $(LOGS) $(IMG)

phase-run: phase-dirs
	$(SH) '\
	  python -m src.stream.replay --input-file $(DATA) --eps $(EPS) --warmup-sec $(WARMUP) --run-duration-sec $(DUR) \
	  | python -m src.stream.features --window-size-sec $(WINDOW) --window-stride-sec $(STRIDE) \
	       --latency-log-file $(METRICS)/latency.csv \
	  > $(METRICS)/features.jsonl'

phase-accept: phase-run
	$(PY) tests/calculate_p95.py $(METRICS)/features.jsonl --sla-ms $(SLA_MS) | tee $(REPORTS)/p95.txt

phase-test:
	$(PY) -m pytest -q tests/stream/test_features.py

phase-all: phase-run phase-accept phase-test

# ==== Evaluation Harness (kept) ====
SPLIT_DIR := sample_data/hdfs

split-data:
	$(PY) -m src.eval.split_data --input-file $(DATA) --output-dir $(SPLIT_DIR)

train: split-data
	@echo "Training model on $(SPLIT_DIR)/train.jsonl..."
	# Placeholder for training logic

evaluate: train
	@echo "Evaluating model on $(SPLIT_DIR)/test.jsonl..."
	$(SH) 'APP_ENV=local POSTGRES_URL=$${DATABASE_URL} python -m src.eval.run_evaluation'

# Aliases for specific phases
phase3-run: ;  $(MAKE) PHASE=phase3 phase-run
phase3-accept: ;  $(MAKE) PHASE=phase3 phase-accept
phase3-test: ;  $(MAKE) PHASE=phase3 phase-test
phase3-all: ;  $(MAKE) PHASE=phase3 phase-all

phase4-run: ;  $(MAKE) PHASE=phase4 phase-run
phase4-accept: ;  $(MAKE) PHASE=phase4 phase-accept
phase4-test: ;  $(MAKE) PHASE=phase4 phase-test
phase4-all: ;  $(MAKE) PHASE=phase4 phase-all

streamlit-run:
	$(PY) -m streamlit run streamlit_app.py

migrate-day3-to-phase3:
	@if [ -d reports/day3 ]; then \
	  mkdir -p reports; mv reports/day3 reports/phase3; \
	  echo "moved reports/day3 -> reports/phase3"; \
	else \
	  echo "no reports/day3 directory found; nothing to migrate"; \
	fi

clean-phase:
	rm -f $(METRICS)/features.jsonl $(METRICS)/latency.csv $(REPORTS)/p95.txt 2>/dev/null || true

clean:
	find . -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true

# ---- Smokes (run inside conda env; file-path form works anywhere) ----
smoke-imports:
	$(PY) scripts/smoke_imports.py

smoke-iforest:
	$(PY) scripts/smoke_iforest.py

smokes: smoke-imports smoke-iforest

# ---- EPSS/KEV ingestion ----
# Default 'ingest' writes to disk (no DB). Files land in data/security/.
ingest: ingest-files

ingest-files:
	mkdir -p data/security
	$(PY) -m src.ingest.epss_fetch --out data/security/epss_scores.csv
	$(PY) -m src.ingest.kev_fetch  --out data/security/kev_entries.json
	@echo "Wrote data/security/epss_scores.csv and data/security/kev_entries.json"

# Loads into Postgres (requires DATABASE_URL with psycopg driver, NOT psycopg2)
ingest-db:
	@test -n "$${DATABASE_URL}" || { echo "Missing DATABASE_URL"; exit 2; }
	$(PY) -m src.ingest.epss_fetch --db
	$(PY) -m src.ingest.kev_fetch  --db

# ---- Day-4 data prep (events + labels from your events file) ----
data-day4:
	$(PY) -m scripts.prep_day4 --events $(DATA) --val-ratio 0.5

# ---- Day-4 fusion builder (runs harness + joins with labels) ----
fix-perms:
	chmod +x scripts/build_fusion.sh || true

.PHONY: build-fusion
build-fusion: fix-perms
	VAL_IN="$(VAL_IN)" TEST_IN="$(TEST_IN)" VAL_LABELS="$(VAL_LABELS)" TEST_LABELS="$(TEST_LABELS)" \
	bash scripts/build_fusion.sh

# ---- Day-4 artifacts: threshold calibration + plots/csv ----
calibrate:
	$(PY) scripts/calibrate_thresholds.py --pred $(VAL_PRED) --model $(MODEL) --fp1k-cap $(FP1K)

artifacts:
	$(PY) scripts/generate_metrics_artifacts.py --pred $(TEST_PRED) --model $(MODEL)

artifacts-all: calibrate artifacts

# ---- One-button Day-4 pipeline ----
day4: data-day4 build-fusion artifacts-all
