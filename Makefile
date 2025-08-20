# ==== Config ====
ENV ?= dovah
PY  := conda run -n $(ENV) python
SH  := conda run -n $(ENV) sh -c

# Phase selector (defaults to phase3; override with PHASE=phase4, etc.)
PHASE ?= phase3

DATA    ?= sample_data/hdfs/sample.jsonl
WINDOW  ?= 10
STRIDE  ?= 1
EPS     ?= 100
WARMUP  ?= 10
DUR     ?= 120
SLA_MS  ?= 800

REPORTS := reports/$(PHASE)
METRICS := $(REPORTS)/metrics
LOGS    := $(REPORTS)/logs
IMG     := $(REPORTS)/img

.PHONY: help env deps phase-dirs phase-run phase-accept phase-test phase-all \
        streamlit-run clean-phase clean migrate-day3-to-phase3 \
        phase3-run phase3-accept phase3-test phase3-all \
        phase4-run phase4-accept phase4-test phase4-all

help:
	@echo "Targets:"
	@echo "  env                  - create/update conda env + pip deps"
	@echo "  phase-dirs           - create report dirs for current PHASE ($(PHASE))"
	@echo "  phase-run            - run replay -> features (writes features.jsonl, latency.csv)"
	@echo "  phase-accept         - gate: p95(lat_ms) < $(SLA_MS)ms (exit non-zero on fail)"
	@echo "  phase-test           - run unit tests for streaming features"
	@echo "  phase-all            - run dirs + pipeline + acceptance + unit tests"
	@echo "  streamlit-run        - run the Streamlit UI locally"
	@echo "  migrate-day3-to-phase3 - move reports/day3 -> reports/phase3 (one-time)"
	@echo "  clean-phase          - remove current PHASE artifacts"
	@echo "  clean                - general cleanup"
	@echo ""
	@echo "Shortcuts:"
	@echo "  phase3-*: aliases with PHASE=phase3"
	@echo "  phase4-*: aliases with PHASE=phase4"
	@echo "  Use: make phase-all PHASE=phase4"

env:
	conda env update -f environment.yml --prune
	$(PY) -m pip install -r requirements.txt

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
	$(PY) tests/calculate_p95.py $(METRICS)/features.jsonl --sla-ms $(SLA_MS)

phase-test:
	$(PY) -m pytest -q tests/stream/test_features.py

phase-all: phase-run phase-accept phase-test

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
