[![Unit Tests without Containers](https://github.com/spencerhuang/airflow-multi-tenant/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/spencerhuang/airflow-multi-tenant/actions/workflows/unit-tests.yml)

# Multi-Tenant and Event-Driven Airflow System

A scalable Airflow-based system supporting multi-tenancy and event-driven architecture with hybrid scheduling, CDC, and reusable connectors.

## Features

- **Multi-tenancy**: Hundreds of tenants/customers, many DAG runs
- **CDC-driven orchestration**: Debezium-triggered runs
- **Workflow-based DAGs**: Each group of DAGs represents a use case (e.g., s3_to_mongo)
- **Hybrid scheduling**: Single Controller DAG with Dynamic Task Mapping dispatches all schedule types (daily/weekly/monthly)
- **Reusable connectors**: S3, Azure, MongoDB, MySQL connectors shared across workflows
- **Hotspot detection**: There's a limit to max_active_runs, this tries to anticipate potential ceilings for scheduled workflows/dags
- **Operational safety**: DST handling, backfill control, worker-slot efficiency, distributed tracing without collector/persistence
- **Audit trail**: Per-customer audit logging via Kafka with schema-per-tenant isolation, sensitive data masking, and GDPR/SOC 2 alignment — see [Audit Trail Design](docs/AUDIT-TRAIL-DESIGN.md)

## Product/Business statement

What is it?

At its core, every business use case becomes a workflow, moving data from one system to another so it can create value. In my case, that means taking PDFs stored in AWS S3 and transforming them into structured data in MongoDB, either through a one time full load or a daily scheduled pipeline. On the surface, this looks like something you could quickly assemble using community templates from tools like n8n. But once the workflow becomes business critical, the real challenges emerge: traceability, observability, and reliable execution at scale. At that point, tools like n8n start to feel more like internal productivity utilities rather than production infrastructure, especially if you are the one who has to explain to customers why yesterday’s scheduled workflow did not run.

Who is it for?

Data Engineers, Product Owner/Manager, Data Scientists, ML Engineers, AI Engineers, Business Analysts in Academic or Small Medium Business.

Why is it relevant?

Regardless you're doing EDA or fine-tuning LLM, you need data to start your ingestion/training pipeline. This project will bootstrap your data needs not just for the near-term, but robust enough to expand in the long run.

How does it compare?

No existing solution provides a turnkey, open-source, self-hosted platform that combines Airflow-compatible orchestration, Kafka-native event-driven architecture, and true single-instance multi-tenancy.

| Capability | This Project | Kestra | Dagster | Astronomer Astro | AWS MWAA |
|---|---|---|---|---|---|
| Single-instance multi-tenancy | ✅ (SMB) | Enterprise only | ✅ | ❌ (separate deploys) | ❌ |
| Kafka-native event-driven | ✅ | ✅ | ❌ | ❌ | ❌ |
| Airflow-compatible | ✅ | ❌ (YAML) | ❌ | ✅ | ✅ |
| Open-source & self-hosted | ✅ | ✅ | ✅ | ❌ | ❌ |

Managed Airflow providers (Astronomer, MWAA, Cloud Composer) solve multi-tenancy by spinning up separate environments per tenant — expensive and operationally heavy. Kestra is the closest architecturally but uses YAML workflows instead of the Airflow ecosystem. Dagster has good multi-tenancy but no native Kafka event backbone.

## Architecture Overview

```
Control Plane Service (REST API, port 8000)
 |                                    \
 v                                     \---> audit.events topic
Business DB (MySQL) --> CDC (Debezium) --> Kafka
                                            |
                          ┌─────────────────┼──────────────────────┐
                          v                 v                      v
                   Audit Service    Airflow Triggerer         audit.events
                   (port 8002)      (AssetWatcher +            topic
                          |         KafkaMessageQueueTrigger)
                          v                 |
                   MySQL (schema-       AssetEvent
                   per-customer)            |
                                            v
                                   cdc_integration_processor DAG
                                            |
                                            v
                                   s3_to_mongo_ondemand DAG
                                            |
                                            v
                                     Airflow Workers
```

The Airflow **triggerer** process runs a persistent `KafkaMessageQueueTrigger` via the `AssetWatcher` (AIP-82). When a matching CDC message arrives, the triggerer fires an `AssetEvent` that the scheduler converts into a `cdc_integration_processor` DAG run. This eliminates the standalone Kafka consumer microservice — CDC event processing is now fully native to Airflow.

## Project Structure

```
.
├── packages/               # Shared pip-installable packages
│   ├── shared_models/     # SQLAlchemy Core table definitions (single source of truth)
│   └── shared_utils/      # Shared utilities (TimezoneConverter, dag_trigger, dlq_utils, etc.)
├── control_plane/          # FastAPI control plane service (REST API)
│   ├── app/
│   │   ├── api/           # REST API endpoints (integrations, dlq, diagnostics, health)
│   │   ├── models/        # SQLAlchemy ORM models (use __table__ from shared_models)
│   │   ├── schemas/       # Pydantic schemas
│   │   ├── services/      # Business logic
│   │   └── core/          # Configuration
│   └── tests/
├── audit_service/          # Audit trail service (Kafka consumer → per-customer MySQL schemas)
│   ├── app/
│   │   ├── api/           # Query endpoints (/audit/{customer_guid}/events)
│   │   ├── services/      # AuditConsumer, AuditSchemaManager
│   │   └── core/          # Configuration
│   └── tests/
├── connectors/             # Reusable data source connectors
│   ├── s3/
│   ├── azure_blob/
│   ├── mongo/
│   ├── mysql/
│   └── tests/
├── airflow/                # Airflow components
│   ├── dags/              # DAG definitions (cdc_event_listener, cdc_integration_processor, etc.)
│   ├── plugins/           # Custom operators, hooks, and callbacks (cdc_apply_function)
│   └── tests/
└── docker/                 # Docker configuration
```

## Quick Start
[SETUP_COMPLETE.md](SETUP_COMPLETE.md)

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (fast Python package manager)

### Local Development

1. Install uv (if you haven't already):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Create a virtual environment and install dependencies (not utilizing uv workspace for reasons):
```bash
uv venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements-dev.txt
```

3. Start the local stack:
```bash
docker-compose up -d
```

4. Access services:
- Airflow UI: http://localhost:8080
- Control Plane API: http://localhost:8000
- API Documentation: http://localhost:8000/docs
- CDC Diagnostics: http://localhost:8000/api/v1/diagnostics/cdc
- Audit Service Health: http://localhost:8002/health

![Docker local](docs/Screenshot2026-02-04at10.56.10AM.png)

### Running Tests

[TESTING.md](TESTING.md)
[E2E_TEST_GUIDE.md](E2E_TEST_GUIDE.md)

![Test in local](docs/Screenshot2026-02-04at8.47.22AM.png)

![Test in local e2e](docs/Screenshot2026-02-04at10.51.29AM.png)

![Test in local e2e](docs/Screenshot2026-02-04at10.50.41AM.png)

## Distributed Tracing

W3C traceparent headers flow end-to-end across the entire pipeline for log correlation:

```
Debezium CDC ──→ Kafka (traceparent header) ──→ AssetWatcher apply_function ──→ AssetEvent payload ──→ Processor DAG ──→ Ondemand DAG conf ──→ Task logs
```

A lightweight Java interceptor (`TraceparentInterceptor`) injects a `traceparent` header into every Kafka message at the Kafka Connect producer level — zero OpenTelemetry SDK dependencies. The `cdc_apply_function` (running in the Airflow triggerer) extracts the traceparent from Kafka message headers and includes it in the normalized payload. The `cdc_integration_processor` DAG reads it from the triggering AssetEvent and passes it through to the ondemand DAG via `dag_run.conf`. Airflow tasks extract it via `TraceIdMixin` and prefix every log line with `[trace_id=...]`.

Traceparent headers are visible in Kafka UI and propagated through Airflow XCom:

![Distributed tracing — Kafka UI traceparent header and Airflow XCom](docs/Screenshot2026-03-17.png)

**Disclaimer:** Trace IDs live only in Kafka headers, structured logs, and Airflow XCom — they are not persisted to the database (e.g., `integration_runs`).

See [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for the full design rationale and file-by-file breakdown.

## Key Components

[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)

### Control Plane Service

FastAPI-based stateless REST API (port 8000) that manages:
- Workflow registry
- Schedule management
- DST normalization
- Backfill policies

### CDC Event Processing (AssetWatcher + KafkaMessageQueueTrigger)

Native Airflow event-driven CDC processing that replaced the standalone Kafka consumer service:
- **AssetWatcher** with `KafkaMessageQueueTrigger` runs in the Airflow triggerer process
- Continuously polls `cdc.integration.events` topic for Debezium CDC events
- `cdc_apply_function` validates/transforms each message in the triggerer (lightweight, no DB queries)
- Matching messages create AssetEvents that trigger `cdc_integration_processor` DAG runs
- Retry-before-DLQ: poison pills are retried (offset not committed) before routing to dead letter queue
- Debezium operations supported: `c` (create), `r` (snapshot), `u` (update), `d` (delete) — only creates trigger DAG processing

### CDC Pipeline Diagnostics

The control plane provides diagnostic endpoints for the CDC pipeline at `/api/v1/diagnostics/cdc`:
- **`GET /diagnostics/cdc`** — Shows Kafka consumer group offsets and Airflow's internal AssetEvent queue side by side, with a `diverged` flag when the two systems are out of sync
- **`POST /diagnostics/cdc/cleanup`** — Clears stale AssetEvents from Airflow's metastore and optionally resets Kafka consumer group offsets

**Why this exists:** Airflow's AssetEvent queue (in the Airflow postgres metastore) is separate from Kafka consumer offsets. Resetting Kafka offsets does NOT clear pending AssetEvents — they will still trigger processor DAG runs. This divergence is invisible without these diagnostic endpoints. See [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for the full explanation.

**Kafka offset reset requires stopping the triggerer first** (consumer group must be inactive):
```bash
# Stop the triggerer
docker-compose stop airflow-triggerer

# Wait ~45s for the Kafka session to expire, then call cleanup
curl -X POST http://localhost:8000/api/v1/diagnostics/cdc/cleanup \
  -H "Content-Type: application/json" \
  -d '{"clear_asset_events": true, "reset_kafka_offsets": true}'

# Restart the triggerer
docker-compose start airflow-triggerer
```

Clearing AssetEvents alone does NOT require stopping the triggerer — it writes directly to the Airflow postgres metastore.

### Audit Service

Standalone FastAPI microservice (port 8002) that provides a per-customer audit trail:
- Consumes `audit.events` from Kafka (produced by Control Plane and Airflow)
- Schema-per-customer isolation (`audit_{customer_guid}`) with template-based provisioning
- Sensitive data masking (passwords, tokens, keys redacted before persistence)
- Query API: `GET /audit/{customer_guid}/events` with filtering by event type, date range, and actor
- GDPR Article 17(3)(e) compliant retention; SOC 2 CC6.1/CC7.1/CC8.1 aligned
- See [Audit Trail Design](docs/audit-trail-design.md) for the full design document

### Connectors

Reusable modules wrapping data source APIs:
- S3 Connector (boto3)
- Azure Blob Connector
- MongoDB Connector
- MySQL Connector

### Airflow DAGs

- **Controller DAG**: [Controller DAG pattern](docs/DISPATCHER_PATTERN.md) — a single `s3_to_mongo_controller` DAG runs every hour, queries the control plane DB for all due integrations (`utc_next_run <= now`), and dispatches each one via `TriggerDagRunOperator` with Dynamic Task Mapping (DTM). Replaces 26+ static dispatcher files with one DAG that handles daily, weekly, and monthly schedules uniformly.
- **CDC Event Listener**: `cdc_event_listener` defines the `integration_cdc_events` Asset with an AssetWatcher that polls Kafka via `KafkaMessageQueueTrigger`
- **CDC Integration Processor**: `cdc_integration_processor` — triggered by AssetEvents from the watcher. Reads the CDC payload, queries the integration from the control plane DB, builds conf, and triggers the appropriate ondemand DAG. Includes DLQ handling via `handle_dlq` task (trigger_rule=ONE_FAILED)
- **On-Demand DAG**: `s3_to_mongo_ondemand` — triggered by the controller (scheduled), CDC processor (event-driven), or control plane API (manual). All trigger paths reuse the same pipeline: Prepare → Validate → Execute → Cleanup.

## Testing Strategy

- **Unit tests**: Operators, hooks, and business logic
- **Connector tests**: Mock SDK clients, contract tests
- **DAG validation**: Static import-only tests
- **Integration tests**: End-to-end CDC flow

## TODO

- k8s setup/deployment are not verified
- Busy-Time Mitigation in section 9.3 was not implemented

## License

MIT
