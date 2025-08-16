.PHONY: env up down ingest replay run eval export test lint clean eda verify_day2

# Environment Setup
env:
	conda env create -f environment.yml || conda env update -f environment.yml
	conda run -n dovah pre-commit install

# Infrastructure
up:
	docker-compose up -d postgres
	sleep 5  # Wait for PostgreSQL to be ready
	conda run -n dovah alembic upgrade head

down:
	docker-compose down

# Data Pipeline
ingest:
	conda run -n dovah python -m src.ingest.epss_fetch
	conda run -n dovah python -m src.ingest.kev_fetch
	conda run -n dovah python -m src.ingest.hdfs_loader

replay:
	conda run -n dovah python -m src.stream.replay

run:
	conda run -n dovah python -m src.stream.features
	conda run -n dovah python -m src.models.log_lm.score
	conda run -n dovah python -m src.models.anomaly.iforest
	conda run -n dovah python -m src.fusion.late_fusion

# Evaluation & Export
eval:
	conda run -n dovah python -m src.eval.run_eval

export:
	conda run -n dovah python -m src.integrations.export_json

# Quality & Testing
test:
	conda run -n dovah pytest -v --cov=src --cov-report=html

lint:
	conda run -n dovah black src tests
	conda run -n dovah ruff src tests
	conda run -n dovah mypy src tests

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".DS_Store" -delete

# Development helpers
format: lint

run_demo: env up ingest replay run eval export

# Analysis
eda:
	mkdir -p reports/eda
	python -m src.analysis.run_analysis --input tests/data/hdfs/sample.jsonl --out reports/eda
	python -m src.eval.run_eval --baseline tests/data/hdfs/sample.jsonl --current tests/data/hdfs/sample.jsonl

# Day 2 Verification
verify_day2:
	pytest -q tests/test_pii.py tests/test_schema.py
	python -m src.analysis.run_analysis --input tests/data/hdfs/sample.jsonl --out reports/eda --epss data/intel/epss_latest.csv --kev data/intel/cisa_kev.csv
	@[ $$(grep -E -ri "(<REDACTED:email>|<REDACTED:ipv[46]>)" reports/eda | wc -l) -gt 0 ] || (echo "PII check failed"; exit 1)
	@[ -f reports/eda/template_stats.json ] && [ -f reports/eda/distribution_stats.json ] && [ -f reports/eda/spike_stats.json ] || (echo "Missing analysis outputs"; exit 1)
	@echo "✅ Day-2 verification passed"
