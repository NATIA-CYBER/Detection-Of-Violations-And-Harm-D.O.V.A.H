# DOVAH (Detection of Violation and Harm)

A cloud-agnostic streaming security ML platform for real-time detection of violations and harm with sub-second latency and explainable alerts.

## Core Features

- Real-time + drift-aware by design
- Explainable-by-default (SHAP + ATT&CK)
- EU-first governance (GDPR/NIS2/DORA compliant)
- Cloud-agnostic architecture
- Metrics-driven acceptance

## Technical Stack

- Python 3.11
- Poetry for dependency management
- Apache Flink for stream processing
- PostgreSQL with pgvector
- AWS (default) with clean adapters for alternatives

## Quick Start

```bash
# Install dependencies
make env

# Start services
make up

# Run data ingestion
make ingest

# Start streaming simulation
make replay

# Run detection pipeline
make run

# Run evaluation
make eval

# Export evidence
make export
```

## Development Setup

1. Install Poetry:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Install dependencies:
```bash
poetry install
```

3. Set up pre-commit hooks:
```bash
poetry run pre-commit install
```

4. Copy environment template:
```bash
cp .env.example .env
```

## Project Structure

```
dovah/
├── src/
│   ├── ingest/      # Data ingestion (EPSS, KEV, HDFS)
│   ├── stream/      # Stream processing and features
│   ├── models/      # ML models (Log-LM, IForest)
│   ├── fusion/      # Late fusion implementation
│   ├── xai/         # Explainability components
│   ├── eval/        # Evaluation metrics
│   ├── genai/       # GenAI components
│   └── integrations/# External integrations
├── notebooks/       # EDA and analysis notebooks
├── sql/            # Database schemas and migrations
├── docs/           # Documentation
└── adr/            # Architecture Decision Records
```

## Performance SLOs

- P95 end-to-end latency ≤ 2s
- Explanation coverage ≥ 95%
- Precision, recall, precision@k metrics
- FP/1k rate monitoring
- Drift TTL tracking

## Security & Compliance

- HMAC-SHA256 pseudonymization
- Per-tenant salt
- EU data residency
- Evidence pack generation
- RBAC implementation

## Contributing

1. Create feature branch
2. Make changes
3. Run tests: `make test`
4. Run linting: `make lint`
5. Submit PR

## License

[License details to be added]
