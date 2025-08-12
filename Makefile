.PHONY: env up down ingest replay run eval export test lint clean

# Environment Setup
env:
	poetry install
	poetry run pre-commit install

# Infrastructure
up:
	docker-compose up -d postgres
	sleep 5  # Wait for PostgreSQL to be ready
	poetry run alembic upgrade head

down:
	docker-compose down

# Data Pipeline
ingest:
	poetry run python -m src.ingest.epss_fetch
	poetry run python -m src.ingest.kev_fetch
	poetry run python -m src.ingest.hdfs_loader

replay:
	poetry run python -m src.stream.replay

run:
	poetry run python -m src.stream.features
	poetry run python -m src.models.log_lm.score
	poetry run python -m src.models.anomaly.iforest
	poetry run python -m src.fusion.late_fusion

# Evaluation & Export
eval:
	poetry run python -m src.eval.metrics

export:
	poetry run python -m src.integrations.export_json

# Quality & Testing
test:
	poetry run pytest -v --cov=src --cov-report=html

lint:
	poetry run black src tests
	poetry run ruff src tests
	poetry run mypy src tests

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
