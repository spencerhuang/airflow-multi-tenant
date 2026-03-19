# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-03-19

### Added
- Distributed tracing — Custom `TraceparentInterceptor` for Kafka Connect propagates W3C `traceparent` headers from Debezium CDC events through to Airflow DAG runs, enabling end-to-end trace correlation (382320e).
- `TraceContext` shared utility — Centralised trace ID generation and `traceparent` parsing in `shared_utils`, replacing ad-hoc implementations across services.
- `dag_trigger` shared utility — Extracted duplicated DAG triggering logic (conf building, auth resolution, DAG ID routing, Airflow REST API calls) from control plane, Kafka consumer, and dispatch operators into `shared_utils.dag_trigger`.
- `parse_mongo_uri()` shared utility — Robust MongoDB URI parser using `urllib.parse` that handles `mongodb+srv://`, URL-encoded credentials, replica sets, and query parameters, replacing brittle string-splitting.
- `parse_s3_uri()` shared utility — S3 URI parser for `s3://bucket/prefix` format.

### Changed
- Kafka consumer service refactored to use shared `dag_trigger` utilities, removing ~130 lines of duplicated code.
- `ValidateS3ToMongoTask` and `ExecuteS3ToMongoTask` now use `parse_mongo_uri()` + `MongoAuth.from_dict()` instead of manual URI splitting.
- Kafka Connect container uses a custom Docker image with the tracing interceptor JAR and increased memory allocation.

### Fixed
- MongoDB URI parsing no longer breaks on passwords containing `@`, `:`, or `%` characters.
- CI test patches updated to target `shared_utils.dag_trigger` after request-handling code was centralised.

## [0.1.1] - 2026-03-16

### Added
- Microservice extraction — The Kafka consumer was split out from the control plane into its own service (7681df6), enabling independent deployment and failure isolation.

- Build system migration — Switched to uv as a drop-in pip interface for faster, reproducible dependency installation (7b325e8).


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
[0.1.1]: https://github.com/spencerhuang/airflow-multi-tenant/releases/tag/v0.1.1
[0.1.2]: https://github.com/spencerhuang/airflow-multi-tenant/releases/tag/v0.1.2
