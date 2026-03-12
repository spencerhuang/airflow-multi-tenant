# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-12

### Added
- Multi-tenant Airflow 3.0 architecture with hybrid scheduling (daily, weekly, monthly, on-demand)
- Control Plane FastAPI service for workspace, integration, and schedule management
- CDC-driven orchestration via Debezium, Kafka, and Kafka consumer service
- Reusable connectors: S3, Azure Blob, MongoDB, MySQL
- Shared packages: `shared_models` (SQLAlchemy table definitions), `shared_utils` (timezone converter, utilities)
- Dispatcher DAG pattern for schedule-based triggering
- On-demand DAG triggering via Airflow REST API
- Alembic database migrations for control plane schema
- Docker Compose local development environment (MySQL, PostgreSQL, MongoDB, MinIO, Kafka, Debezium)
- Kubernetes deployment manifests
- GitHub Actions CI with matrix unit tests (connectors, control_plane, airflow)
- DST-aware schedule normalization
- Workspace-scoped authentication and credential resolution

### Infrastructure
- Airflow 3.0.6 with FAB auth manager, API server, scheduler, and DAG processor
- Debezium 2.5 CDC connector with auto-registration
- Kafka UI and Debezium UI for monitoring
- Makefile with development, test, and Docker commands

[0.1.0]: https://github.com/spencerhuang/airflow-multi-tenant/releases/tag/v0.1.0
