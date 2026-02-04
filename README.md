# Multi-Tenant Airflow Architecture

A scalable Airflow-based architecture supporting multi-tenancy with hybrid scheduling, CDC, and reusable connectors.

## Features

- **Multi-tenancy**: Thousands of tenants, many DAG runs, few DAG definitions
- **Workflow-based DAGs**: Each DAG represents a use case (e.g., s3_to_mongo)
- **Hybrid scheduling**: Airflow-native daily schedules + external control plane for weekly/monthly/on-demand
- **CDC-driven orchestration**: Debezium-triggered runs
- **Reusable connectors**: S3, Azure, MongoDB, MySQL connectors shared across workflows
- **Operational safety**: DST handling, backfill control, worker-slot efficiency

## Architecture Overview

```
Business DB (MySQL) --> CDC (Debezium) --> Kafka --> Control Plane Service
                                                            |
                                                            v
                                                    Airflow REST API
                                                            |
                                                            v
                                            Airflow Scheduler --> Workers
```

## Project Structure

```
.
├── control_plane/          # FastAPI control plane service
│   ├── app/
│   │   ├── api/           # REST API endpoints
│   │   ├── models/        # SQLAlchemy models
│   │   ├── schemas/       # Pydantic schemas
│   │   ├── services/      # Business logic
│   │   └── core/          # Configuration
│   └── tests/
├── connectors/             # Reusable data source connectors
│   ├── s3/
│   ├── azure_blob/
│   ├── mongo/
│   ├── mysql/
│   └── tests/
├── airflow/                # Airflow components
│   ├── dags/              # DAG definitions
│   ├── plugins/           # Custom operators and hooks
│   └── tests/
└── docker/                 # Docker configuration
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+

### Local Development

1. Start the local stack:
```bash
docker-compose up -d
```

2. Access services:
- Airflow UI: http://localhost:8080
- Control Plane API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

### Running Tests

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run all tests
pytest

# Run specific test suites
pytest control_plane/tests/
pytest connectors/tests/
pytest airflow/tests/
```

## Key Components

### Control Plane Service

FastAPI-based service that manages:
- Workflow registry
- Schedule management
- DST normalization
- Backfill policies

### Connectors

Reusable modules wrapping data source APIs:
- S3 Connector (boto3)
- Azure Blob Connector
- MongoDB Connector
- MySQL Connector

### Airflow DAGs

- **Daily Scheduled DAGs**: Pre-created DAGs for each hour (00-23)
- **On-Demand DAG**: Triggered via API for CDC, manual replays, and backfills

## Configuration

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for detailed configuration options.

## Testing Strategy

- **Unit tests**: Operators, hooks, and business logic
- **Connector tests**: Mock SDK clients, contract tests
- **DAG validation**: Static import-only tests
- **Integration tests**: End-to-end CDC flow

## License

MIT
