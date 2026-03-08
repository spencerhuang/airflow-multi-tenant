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
├── packages/               # Shared pip-installable packages
│   ├── shared_models/     # SQLAlchemy Core table definitions (single source of truth)
│   └── shared_utils/      # Shared utilities (TimezoneConverter, etc.)
├── control_plane/          # FastAPI control plane service
│   ├── app/
│   │   ├── api/           # REST API endpoints
│   │   ├── models/        # SQLAlchemy ORM models (use __table__ from shared_models)
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
[SETUP_COMPLETED.md](SETUP_COMPLETE.md)
[MYSQL_CDC_SETUP.md](MYSQL_CDC_SETUP.md)

### Prerequisites

- Docker and Docker Compose
- Python 3.11+

### Local Development

1. Install shared packages:
```bash
pip install -e packages/shared_models -e packages/shared_utils
```

2. Start the local stack:
```bash
docker-compose up -d
```

3. Access services:
- Airflow UI: http://localhost:8080
- Control Plane API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

![Docker local](docs/Screenshot2026-02-04at10.56.10AM.png)

### Running Tests

[TESTING.md](TESTING.md)
[E2E_TEST_GUIDE.md](E2E_TEST_GUIDE.md)

![Test in local](docs/Screenshot2026-02-04at8.47.22AM.png)

![Test in local e2e](docs/Screenshot2026-02-04at10.51.29AM.png)

![Test in local e2e](docs/Screenshot2026-02-04at10.50.41AM.png)

## Key Components

[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)

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

- **Daily Scheduled DAGs**: Pre-created DAGs for each hour (00-23) per workflow
- **On-Demand DAG**: Triggered via API for CDC, manual replays, and backfills

## Testing Strategy

- **Unit tests**: Operators, hooks, and business logic
- **Connector tests**: Mock SDK clients, contract tests
- **DAG validation**: Static import-only tests
- **Integration tests**: End-to-end CDC flow

## TODO

- Implementation of 24 static dags per workflow, only completed 1 static dag for demostration
- k8s deployment not verified
- Busy-Time Mitigation in section 9.3 was not implemented
- Future start_date planner dag for all tenants and all workflows, basically a daily chron job to see which integrations will need weekly or monthly dag_run for tomorrow.
- Future start_date dag (for weekly and monthly) per workflow. Will use DateTimeSensor with mode="reschedule"



## License

MIT
