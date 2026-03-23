# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2026-03-23

### Added
- **Audit service** — New standalone microservice that consumes audit events from Kafka and writes to per-customer MySQL schemas with automatic schema provisioning (06905f6).
- `audit_producer` shared utility — Publishes structured audit events (integration CRUD, DAG lifecycle) to Kafka `audit.events` topic with PII masking.
- `audit_listener` Airflow plugin — Listens to DAG/task lifecycle events (on_dag_run_running, on_task_instance_success/failed) and emits audit events via `audit_producer`.
- `schema_manager` for audit service — Auto-provisions per-customer `audit_<customer_guid>` schemas on first event.
- Audit query API (`GET /api/v1/audit/{customer_guid}`) with filtering by event type, integration ID, and date range.
- `Dockerfile.audit-service` and audit service added to Docker Compose.
- Kafka topic auto-creation script (`kafka-create-topics.sh`).
- `AuditLog` shared model added to `shared_models`.
- Unit tests for audit masking and schema manager.

### Changed
- **Airflow upgraded from 3.0.6 to 3.1.8** — Unifies all services on SQLAlchemy 2.0; eliminates the `<2.0` constraint that was pinning the Airflow requirements file (29ecc08).
- SQLAlchemy unified to 2.0.48 across all requirement files (main, control-plane, kafka-consumer, test, audit-service).
- FastAPI pinned to 0.117.0 across all services for Airflow 3.1 compatibility.
- Custom Airflow Docker image (`Dockerfile.airflow`) to bundle shared packages and plugins.
- Control plane `integrations` API enriched with audit event publishing on create/update/delete.
- Control plane middleware now extracts `customer_guid` for audit context.
- Shared packages (`shared_models`, `shared_utils`) bumped to 0.1.5.
- Documentation reorganised — renamed `SAFETY_AUDIT.md` → `OPERATIONAL_SAFETY_CHECK.md`, `COMPLIANCE_AUDIT.md` → `SPEC_COMPLIANCE_CHECK.md` (2eee3bb).
- Kafka consumer service publishes audit events on CDC processing.

### Fixed
- Lock files regenerated for Airflow 3.1.8 dependency tree.

## [0.1.4] - 2026-03-22

### Security
- Fernet-based encryption for customer credentials — New `secret_provider` module encrypts sensitive auth data at rest using `cryptography.fernet`, replacing plaintext credential passing (4dd98cc).
- Customer auth credentials moved to Redis transient vault — Credentials are now stored in Redis scoped by `dag_run_id` with automatic TTL expiration, eliminating long-lived secrets in Airflow DAG conf (4dd98cc).

### Added
- Dynamic Task Mapping for scheduled DAGs — Replaced static per-schedule DAG files (`s3_to_mongo_daily_02`, `s3_to_mongo_weekly`, `s3_to_mongo_monthly`) with a single `s3_to_mongo_controller` DAG that uses Airflow's `expand()` to fan out tasks dynamically per integration (c7532d6).
- `redis_client` shared utility — Connection pooling, health checks, automatic reconnection, and Sentinel support for Redis-backed credential storage (c406cdb).
- `secret_provider` shared utility — Encrypt/decrypt credentials with Fernet keys, store and retrieve from Redis with configurable TTL.
- `generate_fernet_key.py` helper script for key generation.
- TLS toggle configuration for both Redis (`REDIS_TLS_ENABLED`) and Airflow (`AIRFLOW_TLS_VERIFY`) connections.
- Docker entrypoint secrets script (`entrypoint-secrets.sh`) for secure key injection.
- Redis service added to Docker Compose with persistence and optional TLS.
- Kubernetes deployments — Airflow DAG processor, scheduler, worker deployments; Redis Sentinel StatefulSet; control plane PodDisruptionBudgets (c406cdb).
- Unit tests for `redis_client`, `secret_provider`, dynamic dispatch, and back-fill policy modules.

### Changed
- Dispatcher pattern refactored — Weekly/monthly back-fill policy now correctly scopes dispatches; hourly filtering applied only to daily schedules (25f18d4).
- `s3_to_mongo_operators` refactored to retrieve credentials from Redis vault instead of DAG conf.
- Control plane and Kafka consumer configs updated with Redis and Fernet settings.
- All dependency versions pinned to exact versions for reproducible builds.
- Shared packages (`shared_models`, `shared_utils`) bumped to 0.1.4.
- E2E tests refactored — Replaced `pytest.skip` with `pytest.fail` for real pipeline failures; polling replaces `sleep(15)` in CDC trigger test (ba54cc4).
- Integration tests (`TestDebeziumCDC`) marked with `@pytest.mark.integration` and excluded from default test runs.

### Removed
- Static schedule DAG files — `s3_to_mongo_daily_02.py`, `s3_to_mongo_daily_03.py`, `s3_to_mongo_weekly.py`, `s3_to_mongo_monthly.py` replaced by dynamic controller.

### Fixed
- Naive datetime in DAG `start_date` and E2E tests replaced with timezone-aware UTC datetimes (5aac8b6).
- Weekly/monthly dispatchers no longer accidentally filter by hour (25f18d4).
- Missing `redis` dependency in test requirements that broke CI (36876bd, 676bbb4).
- K8s configmap for control plane missing Redis/Fernet env vars (14a8a5e).

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
[0.1.3]: https://github.com/spencerhuang/airflow-multi-tenant/releases/tag/v0.1.3
[0.1.4]: https://github.com/spencerhuang/airflow-multi-tenant/releases/tag/v0.1.4
[0.1.5]: https://github.com/spencerhuang/airflow-multi-tenant/releases/tag/v0.1.5
