[![Unit Tests without Containers](https://github.com/spencerhuang/airflow-multi-tenant/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/spencerhuang/airflow-multi-tenant/actions/workflows/unit-tests.yml)

# Multi-Tenant and Event-Driven Airflow System

A scalable Airflow-based system supporting multi-tenancy and event-driven architecture with hybrid scheduling, CDC, and reusable connectors.

## Features

- **Multi-tenancy**: Hundreds of tenants/customers, many DAG runs
- **CDC-driven orchestration**: Debezium-triggered runs
- **Workflow-based DAGs**: Each group of DAGs represents a use case (e.g., s3_to_mongo)
- **Hybrid scheduling**: Airflow-native daily schedules + dispatcher-based weekly/monthly
- **Reusable connectors**: S3, Azure, MongoDB, MySQL connectors shared across workflows
- **Hotspot detection**: There's a limit to max_active_runs, this tries to anticipate potential ceilings for scheduled workflows/dags
- **Operational safety**: DST handling, backfill control, worker-slot efficiency

## Product/Business statement

What is it?

The business use case defines a workflow: a workflow can be a transfer of data from any one system to another. In my use case, it’s the PDFs in AWS S3 to MongoDB. The workflow can be full-load on demand or scheduled daily load. On the surface, this sounds like something you can find templates from n8n’s community. However, once you factor in traceability and scalability, n8n feels more like an internal tool, as in I would not want to be the person standing in front of customers explaining why their scheduled workflow/DAG did not run.

Who is it for?
Data Engineers, Product Owner/Manager, Data Scientists, ML Engineers, AI Engineers, Business Analysts

Why is it relevant?
Regardless you're doing EDA or fine-tuning LLM, you need data to start your ingestion/training pipeline. This project will bootstrap your data needs not just for the near-term, but robust enough to expand in the long run.

## Architecture Overview

```
Control Plane Service
 |
 v
Business DB (MySQL) --> CDC (Debezium) --> Kafka --> Kafka Consumer Service
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

- **Scheduled DAGs**: [Dispatcher pattern](docs/DISPATCHER_PATTERN.md) — scheduled DAGs query the control plane DB for due integrations and trigger the ondemand DAG for each one. Each integration gets an isolated DAG run with full conf and IntegrationRun tracking. Daily has to be picked on the hour. Weekly and Monthly do not get to pick the hour.
- **On-Demand DAG**: Triggered via API for CDC, manual replays, and backfills

## Testing Strategy

- **Unit tests**: Operators, hooks, and business logic
- **Connector tests**: Mock SDK clients, contract tests
- **DAG validation**: Static import-only tests
- **Integration tests**: End-to-end CDC flow

## TODO

- Create remaining hourly dispatcher DAGs (daily_00 through daily_23) per workflow — only daily_02 and 03 exist as a working example
- k8s setup/deployment are not verified
- Busy-Time Mitigation in section 9.3 was not implemented
- To scale, kafka_consumer_service in control_plane needs to be its own micro-service, this would also allow fail-over. Once it is its own micro-service, it'll have its own health-check.


## License

MIT
