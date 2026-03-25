# Implementation Summary

## What Was Delivered

### 0. Design Decision: Native Airflow KafkaMessageQueueTrigger ✅

#### Original Assessment (v0.1.1)

Airflow 3.1.7 introduced a stable Kafka Message Queue Trigger with Kafka Provider v1.12.0. We initially chose a standalone Kafka consumer microservice due to concerns about offset management, delivery guarantees, and DLQ support.

#### Revised Decision (v0.1.6): Migrate to AssetWatcher + KafkaMessageQueueTrigger

After operating the standalone consumer, we revisited the tradeoffs. The standalone service added significant operational overhead (separate Dockerfile, CI/CD, container, health monitoring) for a workload that is fundamentally **orchestration signaling** — "wake up and start work" — not high-volume stream processing.

| Concern | Original Assessment | Actual Experience |
| :--- | :--- | :--- |
| **Offset Management** | ⚠️ Basic auto-commit | ✅ Manual commit via `enable.auto.commit: false` + raise-to-prevent-commit pattern |
| **DLQ** | ❌ No native support | ✅ Implemented in `apply_function` with file-based retry tracking |
| **Retry Policy** | ❌ Minimal | ✅ Retry-before-DLQ: raise exception → trigger crashes → offset not committed → message redelivered |
| **Throughput** | ⚠️ Moderate (triggerer asyncio) | ✅ Adequate — CDC events are metadata-only, not high-volume data |
| **Operational Effort** | ✅ Low (within Airflow) | ✅ Eliminated separate service, Dockerfile, CI/CD, and health monitoring |

**Verdict**: For CDC orchestration signaling (low-volume metadata events that trigger DAG runs), the native Airflow approach eliminates an entire microservice while providing equivalent reliability through the retry-before-DLQ pattern.

### 1. AssetWatcher + KafkaMessageQueueTrigger (CDC Event Ingestion) ✅

Replaced the standalone `kafka_consumer/` microservice with Airflow's native event-driven scheduling:

**Asset Definition** — [airflow/dags/cdc_event_listener.py](airflow/dags/cdc_event_listener.py):
```python
integration_cdc_asset = Asset(
    name="integration_cdc_events",
    uri="kafka://kafka/cdc.integration.events",
    watchers=[AssetWatcher(
        name="cdc_integration_watcher",
        trigger=KafkaMessageQueueTrigger(
            topics=["cdc.integration.events"],
            kafka_config_id="kafka_default",
            apply_function="callbacks.cdc_apply_function.cdc_apply_function",
            poll_timeout=1,
            poll_interval=1,
        ),
    )],
)
```

**Apply Function** — [airflow/plugins/callbacks/cdc_apply_function.py](airflow/plugins/callbacks/cdc_apply_function.py):
- Runs in the triggerer process (sync, wrapped by `sync_to_async`)
- Parses Debezium CDC messages (`__op` field) and legacy events (`event_type` field)
- Extracts W3C `traceparent` from Kafka message headers
- Returns normalized payload for `integration.created` events, `None` for all others
- **Retry-before-DLQ**: file-based retry tracking (`/tmp/cdc_apply_retries.json`) — raises `_PoisonPill` to prevent offset commit on parse failures, routes to DLQ after max retries exhausted

**Offset commit behavior** (`AwaitMessageTrigger.run()`):
- `apply_function` returns truthy → offset committed, TriggerEvent yielded
- `apply_function` returns `None` → offset committed, message skipped
- `apply_function` raises exception → trigger crashes, offset NOT committed → Airflow restarts trigger → consumer re-reads from last committed offset (message redelivered)

### 2. CDC Integration Processor DAG ✅

**File**: [airflow/dags/cdc_integration_processor.py](airflow/dags/cdc_integration_processor.py)

Event-driven DAG triggered by the `integration_cdc_events` Asset:

```python
with DAG(
    dag_id="cdc_integration_processor",
    schedule=[integration_cdc_asset],  # Triggered by AssetEvents
    ...
) as dag:
    result = process_and_trigger()   # @task (retries=3, exp backoff)
    handle_dlq_task = handle_dlq()   # @task (trigger_rule=ONE_FAILED)
    result >> handle_dlq_task
```

**`process_and_trigger`**: Reads event payload from `context["triggering_asset_events"]`, queries the integration from the control plane DB, builds DAG run conf, resolves auth credentials, triggers the ondemand DAG via Airflow REST API, records IntegrationRun, and emits audit event.

**`handle_dlq`**: Fires when `process_and_trigger` fails after 3 retries. Persists the failed message to the `dead_letter_messages` table and Kafka DLQ topic.

### 3. DLQ Strategy (3 Layers) ✅

| Layer | Where | Handles |
|-------|-------|---------|
| **Apply function retries** | Triggerer process | Parse/validation failures — raises to prevent offset commit, redelivers message |
| **Task retries** | `process_and_trigger` task | Transient errors (DB down, API timeout) — 3 retries with exponential backoff |
| **DLQ persistence** | `handle_dlq` task | Permanent failures — writes to `dead_letter_messages` table + Kafka DLQ topic + audit trail |

### 4. CDC Pipeline Diagnostics & Cleanup ✅

**File**: [control_plane/app/api/diagnostics.py](control_plane/app/api/diagnostics.py)

#### The Problem: Airflow AssetEvent / Kafka Offset Divergence

Airflow's `AssetWatcher` creates `AssetEvent` rows in the Airflow postgres metastore. These are **separate from Kafka consumer offsets**. Resetting Kafka offsets does NOT clear pending AssetEvents — they will still trigger `cdc_integration_processor` DAG runs.

This divergence is invisible without diagnostics. Example scenario:
1. Triggerer reads 5 CDC messages from Kafka → creates 5 AssetEvents in postgres
2. Operator resets Kafka consumer group offset to latest (e.g., via `kafka-consumer-groups --reset-offsets`)
3. Kafka shows lag=0, but Airflow still has 5 pending AssetEvents → 5 unwanted DAG runs

#### Diagnostic Endpoint: `GET /api/v1/diagnostics/cdc`

Shows Kafka consumer group offsets and Airflow's AssetEvent queue side by side:

```json
{
  "kafka": {
    "consumer_group": "cdc-consumer-airflow",
    "partitions": [{"partition": 0, "committed_offset": 11, "end_offset": 11, "lag": 0}],
    "total_lag": 0
  },
  "airflow_asset_events": {
    "pending_count": 5,
    "queued_dag_runs": 0,
    "events": [
      {"event_id": 10, "integration_id": 21, "event_type": "integration.created", "timestamp": "..."}
    ]
  },
  "diverged": true,
  "divergence_detail": "5 AssetEvent(s) pending in Airflow metastore but Kafka consumer is caught up (lag=0)..."
}
```

#### Cleanup Endpoint: `POST /api/v1/diagnostics/cdc/cleanup`

Clears stale state from both systems:

```json
// Request
{"clear_asset_events": true, "reset_kafka_offsets": true}

// Response
{"asset_events_cleared": 5, "dag_run_queue_cleared": 0, "kafka_offsets_reset": true, "new_offsets": {"partition_0": 11}}
```

**AssetEvent cleanup** writes directly to the Airflow postgres metastore (deletes from `asset_dag_run_queue`, `dagrun_asset_event`, `asset_event` tables). Does NOT require stopping the triggerer.

**Kafka offset reset** requires the consumer group to be inactive. The endpoint checks for active members and returns a clear error if the triggerer is still running:

```json
{"kafka_offsets_reset": false, "kafka_error": "Consumer group 'cdc-consumer-airflow' has 1 active member(s). Stop the airflow-triggerer before resetting offsets."}
```

#### How to Reset Kafka Offsets

The `KafkaMessageQueueTrigger` runs a `confluent-kafka` consumer in the Airflow triggerer process. Kafka requires the consumer group to have no active members before offsets can be reset.

```bash
# 1. Stop the triggerer (this stops the Kafka consumer)
docker-compose stop airflow-triggerer

# 2. Wait for the Kafka session to expire (~45 seconds)
#    The confluent-kafka consumer has a default session.timeout.ms of 45s.
#    Until the session expires, Kafka still considers the consumer active.

# 3. Reset offsets via the control plane API
curl -X POST http://localhost:8000/api/v1/diagnostics/cdc/cleanup \
  -H "Content-Type: application/json" \
  -d '{"clear_asset_events": true, "reset_kafka_offsets": true}'

# 4. Restart the triggerer
docker-compose start airflow-triggerer
```

Alternatively, clear only AssetEvents (no triggerer stop required):
```bash
curl -X POST http://localhost:8000/api/v1/diagnostics/cdc/cleanup \
  -H "Content-Type: application/json" \
  -d '{"clear_asset_events": true, "reset_kafka_offsets": false}'
```

### 5. Updated E2E Tests ✅

**File**: [control_plane/tests/test_s3_to_mongo_e2e.py](control_plane/tests/test_s3_to_mongo_e2e.py)

The E2E test validates the complete native Airflow CDC pipeline:

```
Test → Create integration via API → MySQL INSERT
  → Debezium detects change → CDC event to Kafka
  → AssetWatcher detects event → AssetEvent created
  → cdc_integration_processor DAG triggered
  → s3_to_mongo_ondemand DAG triggered
  → Data transferred from S3 to MongoDB
```

**CDC pipeline cleanup** runs in the test fixture `finally` block (regardless of pass or fail) via shared helpers in [control_plane/tests/e2e_helpers.py](control_plane/tests/e2e_helpers.py):
1. Stop the triggerer
2. Reset Kafka consumer group offset to latest
3. Delete processor DAG runs via Airflow API
4. Clear stale AssetEvents from Airflow metastore (via `docker exec` psql)
5. Clear triggerer retry state file
6. Restart the triggerer

## How It Works

### Complete Data Flow

```
1. Integration Created
   User → Control Plane API → MySQL

2. CDC Event Published
   Debezium → Kafka Topic (cdc.integration.events)
   (with traceparent header from TraceparentInterceptor)

3. AssetWatcher Processes Event (in Airflow Triggerer)
   KafkaMessageQueueTrigger polls message
   ↓
   cdc_apply_function(message) called
   ↓
   Parses CDC payload, extracts traceparent
   ↓
   Returns normalized dict → TriggerEvent → AssetEvent

4. Scheduler Triggers Processor DAG
   AssetEvent → cdc_integration_processor DAG run
   ↓
   process_and_trigger task:
     - Reads payload from triggering_asset_events
     - Queries integration from control plane DB
     - Builds conf, resolves auth credentials
     - Triggers s3_to_mongo_ondemand via Airflow REST API
     - Records IntegrationRun, emits audit event

5. Airflow Executes Workflow
   s3_to_mongo_ondemand DAG: Prepare → Validate → Execute → Cleanup

6. Data Transferred
   MinIO/S3 → MongoDB
```

### CDC Event Processing Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  AIRFLOW TRIGGERER PROCESS (asyncio)                        │
│                                                             │
│  KafkaMessageQueueTrigger                                   │
│    - Polls cdc.integration.events continuously              │
│    - apply_function validates/transforms each message       │
│    - On match: yields TriggerEvent → AssetEvent created     │
│                                                             │
│  AssetWatcher(name="cdc_watcher", trigger=kafka_trigger)    │
│    - Wraps the trigger                                      │
│    - Fires asset events to the scheduler                    │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼ AssetEvent (payload = normalized message data)
┌─────────────────────────────────────────────────────────────┐
│  cdc_integration_processor DAG                              │
│  schedule=[integration_cdc_asset]                           │
│                                                             │
│  [process_and_trigger]  @task (retries=3, exp backoff)      │
│    - Read event from context["triggering_asset_events"]     │
│    - Extract traceparent, log with trace_id                 │
│    - Query DB, build conf, resolve auth                     │
│    - Trigger s3_to_mongo_ondemand via Airflow REST API      │
│    - Record IntegrationRun, emit audit event                │
│         │                                                   │
│        / \                                                  │
│  [handle_dlq]            @task (trigger_rule=ONE_FAILED)    │
│    - Persist to dead_letter_messages table + Kafka DLQ      │
│    - Emit audit "message.dead_lettered"                     │
└─────────────────────────────────────────────────────────────┘
```

## Running the System

### 1. Start Services

```bash
# Start complete stack
docker-compose up -d

# Check services are running
docker-compose ps
```

### 2. Verify Triggerer Started

```bash
# Check triggerer logs for AssetWatcher activity
docker-compose logs airflow-triggerer | grep -E "cdc|apply_function|AssetWatcher"

# Check CDC pipeline diagnostics
curl -s http://localhost:8000/api/v1/diagnostics/cdc | python3 -m json.tool
```

### 3. Monitor Activity

**Kafka UI**: http://localhost:8081
- Navigate to Consumer Groups
- Find: `cdc-consumer-airflow`
- View lag and consumption rate

**Triggerer Logs**:
```bash
docker-compose logs -f airflow-triggerer | grep "cdc"
```

**CDC Diagnostics API**:
```bash
# Check Kafka offsets and AssetEvent queue
curl -s http://localhost:8000/api/v1/diagnostics/cdc | python3 -m json.tool
```

### 4. Run Tests

```bash
# E2E test (CDC event-driven)
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s

# E2E test (cron scheduled)
pytest control_plane/tests/test_cron_scheduled_e2e.py -v -s
```

## Configuration

**Docker Compose** ([docker-compose.yml](docker-compose.yml)):
```yaml
# Kafka connection for AssetWatcher trigger
AIRFLOW_CONN_KAFKA_DEFAULT: '{"conn_type": "kafka", "extra": {
  "bootstrap.servers": "kafka:29092",
  "group.id": "cdc-consumer-airflow",
  "auto.offset.reset": "earliest",
  "enable.auto.commit": "false"
}}'

# Control plane needs Airflow metastore access for CDC diagnostics
AIRFLOW_METADB_URL: 'postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow'
```

**Consumer Settings** (via `AIRFLOW_CONN_KAFKA_DEFAULT`):
- Group ID: `cdc-consumer-airflow`
- Auto Offset Reset: `earliest`
- Auto Commit: `false` (manual commit via trigger)
- Poll Interval: 1 second
- Poll Timeout: 1 second

## Testing

### Run All Tests

```bash
# E2E test (CDC event-driven)
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s

# E2E test (cron scheduled)
pytest control_plane/tests/test_cron_scheduled_e2e.py -v -s
```

### Expected Results

**E2E Test (CDC)**: 6/6 passing
- Upload to MinIO ✓
- Verify MinIO data ✓
- Create integration ✓ (triggers Debezium CDC)
- Wait for CDC pipeline ✓ (AssetWatcher → Processor → Ondemand)
- Verify MongoDB data ✓
- Summary ✓

## Files Created/Modified

### CDC Event Processing (New — replaces kafka_consumer/ microservice)

1. **airflow/dags/cdc_event_listener.py** — Asset + AssetWatcher definition
2. **airflow/dags/cdc_integration_processor.py** — Event-driven processor DAG
3. **airflow/plugins/callbacks/__init__.py** — Callbacks package init
4. **airflow/plugins/callbacks/cdc_apply_function.py** — Apply function with retry-before-DLQ
5. **packages/shared_utils/shared_utils/dlq_utils.py** — Shared DLQ persistence utilities

### CDC Diagnostics (New)

1. **control_plane/app/api/diagnostics.py** — Kafka offset + AssetEvent diagnostic and cleanup endpoints
2. **control_plane/tests/e2e_helpers.py** — Shared CDC pipeline cleanup for e2e tests

### Audit Service (New)

1. **audit_service/app/main.py** — FastAPI app with Kafka consumer lifecycle
2. **audit_service/app/core/config.py** — Configuration (DATABASE_URL, Kafka settings)
3. **audit_service/app/services/audit_consumer.py** — Kafka consumer with sensitive data masking
4. **audit_service/app/services/schema_manager.py** — Schema-per-customer provisioning and cache
5. **audit_service/app/api/audit.py** — Query API (`GET /audit/{customer_guid}/events`)
6. **audit_service/tests/test_schema_manager.py** — Schema manager unit tests
7. **audit_service/tests/test_audit_masking.py** — Sensitive data masking tests
8. **packages/shared_utils/shared_utils/audit_producer.py** — Threaded Kafka audit producer
9. **packages/shared_utils/tests/test_audit_producer.py** — Audit producer unit tests
10. **docker/Dockerfile.audit-service** — Container image
11. **requirements-audit-service.txt** — Dependencies
12. **docker/mysql-init.sql** — Added `audit_svc` user and `audit_template` schema
13. **docs/audit-trail-design.md** — Full design document (GDPR/SOC 2 compliance)

### Control Plane (Modified)

1. **control_plane/app/main.py** — Pure stateless REST API
2. **control_plane/app/core/config.py** — Added `AIRFLOW_METADB_URL` for CDC diagnostics
3. **control_plane/app/api/__init__.py** — Registered diagnostics router
4. **control_plane/app/models/dead_letter_message.py** — ORM model for DLQ table

### Infrastructure (Modified)

1. **docker-compose.yml** — Added `airflow-triggerer` service, `AIRFLOW_CONN_KAFKA_DEFAULT`, `AIRFLOW_METADB_URL`; removed `kafka-consumer` service
2. **docker/Dockerfile.airflow** — Added `librdkafka-dev` for `confluent-kafka`
3. **requirements.txt** — Added `apache-airflow-providers-apache-kafka>=1.11.3`
4. **requirements-control-plane.txt** — Added `psycopg2-binary` for Airflow metastore access

### Documentation (Modified)

1. **README.md** — Updated architecture diagram, project structure, key components, CDC diagnostics
2. **IMPLEMENTATION_SUMMARY.md** (this file) — Replaced Kafka consumer sections with AssetWatcher approach

## Audit Trail Service ✅

**Design Document**: [docs/AUDIT-TRAIL-DESIGN.md](docs/AUDIT-TRAIL-DESIGN.md)

A standalone FastAPI microservice (port 8002) that provides a GDPR-compliant, per-customer audit trail across the Control Plane and Airflow services.

### Architecture

```
Control Plane  ─┐
Airflow DAGs   ─┼──→ Kafka (audit.events) ──→ Audit Consumer ──→ MySQL (schema-per-customer)
CDC Processor  ─┘                                                    audit_{customer_guid}
```

All audit producers use a shared `AuditProducer` (threaded, fire-and-forget, never blocks the caller) from `shared_utils`. The Audit Service consumes events, auto-provisions a schema for each customer on first event, and persists the audit record.

### Key Components

| Component | File | Purpose |
|---|---|---|
| AuditProducer | `packages/shared_utils/shared_utils/audit_producer.py` | Threaded Kafka producer; fire-and-forget with bounded queue |
| AuditConsumer | `audit_service/app/services/audit_consumer.py` | Kafka consumer; masks sensitive data, writes to per-customer schema |
| AuditSchemaManager | `audit_service/app/services/schema_manager.py` | Schema-per-customer provisioning via `CREATE TABLE LIKE audit_template.audit_events` |
| Query API | `audit_service/app/api/audit.py` | `GET /audit/{customer_guid}/events` with filtering |
| Sensitive data masking | `audit_service/app/services/audit_consumer.py` | Redacts passwords, tokens, keys, credentials before persistence |

### Storage Isolation

Each customer gets an isolated MySQL schema (`audit_{customer_guid}`) cloned from `audit_template`. The `audit_svc` MySQL user has `SELECT, INSERT, UPDATE, DELETE, CREATE, DROP ON *.*` to support dynamic schema provisioning. GDPR Article 17 erasure is a single `DROP SCHEMA`.

### Test Coverage

- **20 unit tests** in `audit_service/tests/` (schema manager, sensitive data masking)
- **10 unit tests** in `packages/shared_utils/tests/` (audit producer, NoOp fallback, queue behavior)

---

## Key Benefits

### 1. Event-Driven Architecture ✅
- Decoupled services
- Real-time processing
- Scalable design

### 2. Production Ready ✅
- Comprehensive error handling
- Graceful startup/shutdown
- Background thread execution

### 3. Well Tested ✅
- Unit tests
- Integration tests
- End-to-end tests

### 4. Fully Documented ✅
- Architecture diagrams
- Configuration guides
- Troubleshooting procedures

### 5. Observable ✅
- Detailed logging
- Kafka UI monitoring
- Metrics ready

## Quick Start Commands

```bash
# Start everything
docker-compose up -d

# Verify triggerer and AssetWatcher are running
docker-compose logs airflow-triggerer | grep -E "cdc|apply_function"

# Check CDC pipeline diagnostics (Kafka offsets + AssetEvent queue)
curl -s http://localhost:8000/api/v1/diagnostics/cdc | python3 -m json.tool

# Run tests
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s

# Monitor triggerer activity
docker-compose logs -f airflow-triggerer | grep "cdc"

# Check Kafka consumer group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cdc-consumer-airflow

# Clean up stale CDC state (AssetEvents only — no triggerer stop needed)
curl -X POST http://localhost:8000/api/v1/diagnostics/cdc/cleanup \
  -H "Content-Type: application/json" \
  -d '{"clear_asset_events": true, "reset_kafka_offsets": false}'
```

## Error Tracking: XCom-Based Pipeline Error Capture

### How It Works

Airflow pipeline tasks (Prepare, Validate, Execute) capture errors and push them to XCom using the shared utility `push_task_errors()` from `shared_utils.task_error_tracking`. The CleanUp task (which runs with `trigger_rule=ALL_DONE`, i.e. always) collects all errors and persists them to the `integration_run_errors` table.

```
Prepare/Validate/Execute tasks
  ↓ (on error)
  push_task_errors(ti, task_id, errors)
    → XCom key: "task_errors_{task_id}"
    → Structured: [{task_id, error_code, message}, ...]
  ↓
CleanUp task (always runs)
  ↓
  pull_all_task_errors(ti, ["prepare", "validate", "execute"])
    → Collects all XCom error records
  ↓
  Fallback: check task instance states for tasks with no XCom errors
    → Generates generic error from state (e.g. "Task validate ended in state: failed")
    → Deduplicates: skips tasks already covered by XCom errors
  ↓
  INSERT INTO integration_run_errors (run_id, error_code, message, task_id, timestamp)
  UPDATE integration_runs SET ended=now, is_success=(no errors)
```

### XCom Space Limits

XCom has limited storage, so error messages are truncated and capped:

| Constant | Value | Purpose |
|---|---|---|
| `MAX_ERROR_MESSAGE_LENGTH` | 2,000 chars | Truncates individual error messages |
| `MAX_ERRORS_PER_TASK` | 20 errors | Caps number of errors per task |

These constants are defined in `packages/shared_utils/shared_utils/task_error_tracking.py`.

### Error Types by Task

| Task | Error Code | When |
|---|---|---|
| Prepare | `PREPARE_ERROR` | Missing config (s3_bucket, mongo_collection), DB failures |
| Validate | `VALIDATION_ERROR` | S3 bucket inaccessible, MongoDB connection failed |
| Execute | `DATA_ERROR` | Per-file JSON parse errors, write failures (non-fatal, continues) |
| Execute | `EXECUTE_ERROR` | Fatal exception during execution |

### IntegrationRun Lifecycle

1. **PrepareTask** calls `create_integration_run()` (from `shared_utils`) **before** any validation — guarantees the row exists even if Prepare fails
2. **CleanUpTask** updates the same row with `ended`, `is_success`, and inserts error records

### Key Files

- `packages/shared_utils/shared_utils/task_error_tracking.py` — `push_task_errors()`, `pull_all_task_errors()`
- `packages/shared_utils/shared_utils/integration_run.py` — `create_integration_run()`
- `airflow/plugins/operators/s3_to_mongo_operators.py` — Prepare/Validate/Execute/CleanUp implementations

---

## SQLAlchemy Version Strategy

### The Problem

The project has two distinct SQLAlchemy consumers with different runtime environments:

| Component | SQLAlchemy Version | API Style | Connection |
|---|---|---|---|
| **Control Plane** (FastAPI) | 2.0.25 | Async ORM (`AsyncSession`, `select()`) | `asyncpg` / `aiomysql` |
| **Airflow Operators** | 2.0.25 (via Airflow 3.0) | Sync Core (`engine.connect()`, `engine.begin()`) | `pymysql` |

### Solution: `shared_models` with Core Tables

The `packages/shared_models` package defines tables using **SQLAlchemy Core** (`Table`, `Column`, `MetaData`) — the lowest-common-denominator API that works identically across SQLAlchemy 1.4 and 2.0, sync and async.

```
shared_models/tables.py  (Core tables — works everywhere)
  ↑                        ↑
  │                        │
Control Plane ORM         Airflow Operators
(maps via __table__)      (uses Core directly)
(SQLAlchemy 2.0 async)    (SQLAlchemy 2.0 sync)
```

### Dependency Constraint

`shared_models` declares a broad dependency to support both:
```toml
# packages/shared_models/pyproject.toml
dependencies = ["sqlalchemy>=1.4,<3"]
```

### Why Core Tables, Not ORM Models

- ORM models (`DeclarativeBase`, `relationship()`) require the ORM layer and session management, which differs between async (Control Plane) and sync (Airflow)
- Core tables (`Table()`, `Column()`) are pure schema definitions with zero runtime assumptions
- The Control Plane wraps Core tables with ORM models that add `relationship()` for convenience; Airflow operators use the Core tables directly with `conn.execute(table.insert().values(...))`

---

## Database Schema Management (Alembic Migrations)

### Architecture

The project uses a two-layer schema architecture with Alembic for versioned migrations:

```
packages/shared_models/tables.py    ← Single source of truth (SQLAlchemy Core)
        ↓                                    ↓
control_plane/app/models/*.py       Airflow operators
(ORM: __table__ + relationships)    (Core: direct table access)
        ↓
control_plane/alembic/              ← Migration scripts
```

- **`shared_models/tables.py`**: Defines all tables using SQLAlchemy Core (`Table`, `Column`, `MetaData`). This is the canonical schema.
- **`control_plane/app/models/*.py`**: ORM models that map to shared tables via `__table__` and add only `relationship()` declarations.
- **`control_plane/app/core/database.py`**: Creates `Base = declarative_base(metadata=shared_metadata)`, linking ORM models to the shared `MetaData`.
- **`control_plane/alembic/env.py`**: Sets `target_metadata = Base.metadata` so Alembic autogenerate diffs against `shared_models`.

### Migration Workflow

#### 1. Modify Schema

Edit `packages/shared_models/shared_models/tables.py` — add/remove/modify columns or tables.

If adding a new table or changing relationships, also update the corresponding ORM model in `control_plane/app/models/`.

#### 2. Generate Migration

```bash
cd control_plane
alembic revision --autogenerate -m "description of change"
```

This compares the live DB against `tables.py` and generates `upgrade()` / `downgrade()` in a new versioned file under `alembic/versions/`.

#### 3. Review the Generated Migration

Always review the generated file — autogenerate can miss or misinterpret certain changes (e.g., column renames detected as drop+add).

#### 4. Apply Migration

```bash
# Apply all pending migrations
cd control_plane
alembic upgrade head

# Apply one migration forward
alembic upgrade +1
```

#### 5. Revert Migration

```bash
# Roll back one migration
cd control_plane
alembic downgrade -1

# Roll back to a specific revision
alembic downgrade <revision_id>

# Roll back everything
alembic downgrade base
```

#### 6. Deploy to a Fresh Database

```bash
cd control_plane
alembic upgrade head
```

This runs the entire migration chain from `base` → `head`, creating all tables from scratch.

### Current Migration History

| Revision | Description | Date |
|---|---|---|
| `90e6a4598441` | Initial migration (all tables) | 2026-02-05 |
| `b3f1a2c7d890` | Add dead letter messages table | 2026-02-05 |

### Useful Commands

```bash
# Show current revision in DB
alembic current

# Show migration history
alembic history --verbose

# Show pending migrations
alembic heads

# Show SQL without executing (offline mode)
alembic upgrade head --sql
```

### Key Rules

1. **Schema changes go in `shared_models/tables.py`** — never define columns in ORM models
2. **ORM models only add `relationship()`** — mapped via `__table__`
3. **Always generate migrations** — don't manually create/alter tables in production
4. **Review autogenerated scripts** — especially for renames, default values, and server defaults
5. **Commit migration files to git** — they are the deployable, revertible history of your schema
6. **Update KEDA raw SQL after schema changes** — the KEDA ScaledObject in `k8s/deployments/airflow-worker-deployment.yaml` contains raw SQL queries against `integrations` (MySQL) and `task_instance` (PostgreSQL). These are not covered by Alembic. After renaming/removing columns referenced in KEDA triggers, update the queries manually. Run: `grep -r 'integrations\|task_instance' k8s/deployments/ --include='*.yaml'`

---

## Package Management: pip → uv (Drop-in Replacement)

### What Changed

We replaced `pip` and `pip-tools` (`pip-compile`) with [uv](https://docs.astral.sh/uv/) as a drop-in replacement. All `pip install` commands became `uv pip install`, and `pip-compile` became `uv pip compile`.

| File | Change |
|------|--------|
| `Makefile` | `pip install` → `uv pip install`, `pip-compile` → `uv pip compile` |
| `docker/Dockerfile.control-plane` | Added uv binary via multi-stage copy, `pip install` → `uv pip install --system` |
| `docker/Dockerfile.airflow` | Same as above |
| `.github/workflows/unit-tests.yml` | Replaced `actions/setup-python` with `astral-sh/setup-uv@v5`, `uv venv`, and `uv run` |
| `run_all_tests.sh` | `pip install` → `uv pip install` |
| `requirements-dev.txt` | Removed `pip-tools` (uv replaces it) |
| `.gitignore` | Added `uv.lock` |
| `.python-version` | Created with `3.11` for uv auto-detection |

All `requirements*.txt` files remain unchanged — uv is fully compatible with pip's format.

### Why Drop-in, Not uv Workspaces

uv offers a native workspace mode (`[tool.uv.workspace]`) that manages dependencies via `uv.lock` and `uv sync`. We intentionally chose **not** to use it because of the **SQLAlchemy version split**:

- **Airflow context** requires `sqlalchemy>=1.4,<2.0` (Airflow 3.0.6 core constraint)
- **Control Plane** uses `sqlalchemy==2.0.48`

uv workspaces resolve a **single version per package** across all workspace members. There is no way to have one member use SQLAlchemy 1.x and another use 2.x within the same workspace lockfile.

The 7 separate `requirements*.txt` files exist because this is a multi-service monorepo where each service has distinct dependency needs:

| File | Purpose | SQLAlchemy |
|------|---------|------------|
| `requirements.txt` | Full Airflow environment | `<2.0` |
| `requirements-control-plane.txt` | FastAPI control plane | `==2.0.46` |
| `requirements-test.txt` | CI unit tests (no Airflow) | `==2.0.46` |
| `requirements-dev.txt` | Local development (includes Airflow) | `<2.0` |
| `requirements-lock.txt` | Locked Airflow deps with hashes | `<2.0` |
| `requirements-control-plane-lock.txt` | Locked control plane deps with hashes | `==2.0.46` |

This separation is load-bearing — collapsing it into a single `pyproject.toml` would force one SQLAlchemy version and break either Airflow or the control plane.

### What the Drop-in Approach Gives Us

- **Speed**: uv resolves and installs 10-50x faster than pip
- **Lock file generation**: `uv pip compile` replaces `pip-compile` with the same output format
- **Zero risk**: All existing requirements files work unchanged
- **Docker builds**: Faster image builds with the same hash-verified installs
- **CI**: Faster pipeline runs via the `astral-sh/setup-uv@v5` GitHub Action

### When to Revisit

A full uv workspace migration becomes practical when:

1. **Airflow supports SQLAlchemy 2.0** — eliminates the version split, allowing a single lockfile
2. **Services are split into separate repos** — each can have its own `uv.lock`

### Common Commands

```bash
# Install dev dependencies
uv pip install -r requirements-dev.txt

# Install test dependencies only
uv pip install -r requirements-test.txt

# Generate locked requirements with hashes
uv pip compile --generate-hashes --output-file requirements-lock.txt requirements.txt

# Upgrade all locked dependencies
uv pip compile --generate-hashes --upgrade --output-file requirements-lock.txt requirements.txt

# Or use Makefile shortcuts
make install       # uv pip install -r requirements-dev.txt
make lock          # Generate both lock files
make lock-upgrade  # Upgrade and regenerate lock files
```

---

## Distributed Tracing: W3C Traceparent via Custom Kafka Connect Image

### The Problem

When a CDC event flows through **Debezium → Kafka → Consumer → Airflow**, there is no way to correlate log lines across these services. A single integration trigger can produce logs in four different processes, and without a shared identifier, debugging requires manual timestamp correlation — slow, error-prone, and impossible at scale.

### Why W3C Traceparent (Not OpenTelemetry SDK)

The standard approach would be to add the OpenTelemetry Java SDK to Kafka Connect. We rejected this because:

1. **Heavyweight dependency** — The OTEL SDK pulls in `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-*`, and their transitive dependencies, significantly increasing the Kafka Connect image size and startup time.
2. **Exporter infrastructure required** — OTEL expects a collector (Jaeger, Zipkin, OTLP) to receive spans. We don't need span visualization yet — we need **log correlation**.
3. **Version conflicts** — Debezium bundles its own dependency tree; adding OTEL jars risks classpath conflicts with Kafka Connect's existing libraries.
4. **Overkill for the use case** — We only need a unique `trace_id` attached to each CDC message. A full tracing SDK with span hierarchies, sampling, and export pipelines is unnecessary overhead.

### The Solution: Custom `TraceparentInterceptor` + Custom Docker Image

Kafka Connect supports **producer interceptors** — classes that modify every `ProducerRecord` before it hits the broker. We wrote a zero-dependency Java interceptor that:

1. Generates a W3C-compliant `traceparent` header (`00-<trace_id>-<span_id>-01`)
2. Injects it into the Kafka message headers on every `onSend()`
3. Requires no OTEL SDK, no collector, no external dependencies beyond `kafka-clients` (already on the classpath)

**Why a custom Docker image?** The interceptor is a `.jar` that must be on Kafka Connect's classpath at `/kafka/libs/`. The stock `debezium/connect` image doesn't include it, so we build a derived image:

```
docker/Dockerfile.kafka-connect (multi-stage)
├── Stage 1: eclipse-temurin:21-jdk — compiles TraceparentInterceptor.java → .jar
└── Stage 2: debezium/connect:3.0.0.Final — copies the .jar into /kafka/libs/
```

This keeps the production image minimal (only adds ~3KB) while the JDK build tools stay in the discarded builder stage.

### How Trace Context Flows End-to-End

```
┌──────────────────────┐
│  MySQL CDC Change     │
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│  Debezium Connector   │
│  (Kafka Connect)      │
│                       │
│  TraceparentInterceptor.onSend()
│  → Generates traceparent header
│  → Injects into Kafka message headers
└──────────┬───────────┘
           ↓  (Kafka message with traceparent header)
┌──────────────────────┐
│  Airflow Triggerer    │
│  (cdc_apply_function) │
│                       │
│  _extract_traceparent(message.headers())
│  → Parses traceparent from Kafka headers
│  → Includes in normalized payload dict
│  → Payload becomes AssetEvent.extra
└──────────┬───────────┘
           ↓  (AssetEvent → cdc_integration_processor → conf.traceparent)
┌──────────────────────┐
│  Airflow DAG Tasks    │
│                       │
│  TraceIdMixin._get_trace_context()
│  → Reads traceparent from dag_run.conf
│  → Falls back to XCom (pushed by PrepareTask)
│  → Prefixes all log lines with [trace_id=...]
└──────────────────────┘
```

### Files Involved

| File | Role |
|---|---|
| `docker/kafka-interceptor/src/io/debezium/tracing/TraceparentInterceptor.java` | Java interceptor — generates and injects W3C traceparent headers |
| `docker/Dockerfile.kafka-connect` | Multi-stage build: compile interceptor → derive from debezium/connect |
| `docker-compose.yml` | `kafka-connect` service: `build:` replaces `image:`, adds `CONNECT_PRODUCER_INTERCEPTOR_CLASSES` env var |
| `packages/shared_utils/shared_utils/trace_context.py` | Python `TraceContext` class — parses/generates W3C traceparent strings |
| `packages/shared_utils/shared_utils/__init__.py` | Exports `TraceContext` from `shared_utils` |
| `airflow/plugins/callbacks/cdc_apply_function.py` | Extracts traceparent from Kafka message headers, includes in normalized payload |
| `airflow/dags/cdc_integration_processor.py` | Reads traceparent from AssetEvent payload, threads through DAG conf |
| `airflow/plugins/operators/base_operators.py` | `TraceIdMixin` — extracts traceparent from dag_run conf or XCom for all task operators |
| `airflow/plugins/operators/s3_to_mongo_operators.py` | All tasks (Prepare/Validate/Execute/CleanUp) prefix logs with `[trace_id=...]` |
| `airflow/plugins/operators/dispatch_operators.py` | `find_and_prepare_due_integrations()` generates a fresh traceparent for scheduler-triggered DAGs |

### Key Design Decisions

1. **Per-message trace_id (not per-connector)** — Each CDC event gets its own trace_id, so you can grep a single integration trigger across all services.

2. **Fallback to `TraceContext.new()`** — If the traceparent header is missing (e.g., manual Airflow trigger, legacy messages), a fresh context is generated rather than failing. Tracing is always-on, never blocking.

3. **Two propagation paths into Airflow** — `dag_run.conf.traceparent` (set by `cdc_integration_processor` from the AssetEvent payload) is the primary path. `XCom` (pushed by PrepareTask) is the fallback for downstream tasks that can't access dag_run conf directly.

4. **Scheduler-triggered DAGs get tracing too** — `find_and_prepare_due_integrations()` calls `TraceContext.new().traceparent` so scheduled runs (not triggered by CDC) still have a trace_id for log correlation.

5. **`CONNECT_PRODUCER_INTERCEPTOR_CLASSES` env var** — Kafka Connect applies the interceptor to all connectors in the cluster without modifying individual connector configs. One setting, all CDC topics get traceparent headers.

---

## Security Overhaul: Unified Secret Management & Transient Credential Vault

### The Problem

The project had several security gaps:

1. **Hardcoded credentials** in `docker-compose.yml` (MySQL, Postgres, MongoDB, MinIO passwords as literal strings)
2. **Empty Fernet key** — Airflow Variables and Connections stored unencrypted in the metadata DB
3. **Customer credentials leaked through XCom** — sensitive S3/Mongo auth data persisted in the Airflow metadata database (`xcom` table), accessible to anyone with DB read access
4. **No unified secret management** — each service resolved secrets independently with inconsistent patterns

### Design Principles

- **Infrastructure vs. customer credentials**: Infrastructure secrets (MySQL, Postgres, Redis, Airflow passwords) are loaded once at startup via `InfraSecrets`. Customer credentials (S3/Mongo creds from the `auths` table) are ephemeral, stored transiently in Redis per DAG run.
- **Environment-agnostic resolution**: The same code runs on Docker (dev) and Kubernetes (prod) without changes. A three-tier resolution order — file, env var, default — abstracts both Docker secrets and K8s mounted volumes.
- **Adaptive TLS**: Redis client auto-detects the environment by checking for a CA certificate file. Production gets mTLS; development gets plain TCP with no extra setup.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Secret Resolution Flow                      │
│                                                                 │
│  K8s Secret volume ──┐                                          │
│  Docker secret file ─┤──> /run/secrets/{key} ──┐               │
│                      │                          ├──> read_secret│
│  Environment var ────┘──> os.environ[KEY] ─────┘     (unified) │
│                                                       │         │
│                              Default value ───────────┘         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              Customer Credential Flow (per DAG run)             │
│                                                                 │
│  PrepareTask ──store_credentials(run_id, creds)──> Redis SETEX  │
│       │                                           (TTL=1800s)   │
│       v                                                         │
│  ValidateTask ──fetch_credentials(run_id)────────> Redis GET    │
│       │                                                         │
│       v                                                         │
│  ExecuteTask ──fetch_credentials(run_id)─────────> Redis GET    │
│       │                                                         │
│       v                                                         │
│  CleanUpTask ──delete_credentials(run_id)────────> Redis DELETE │
│                                                                 │
│  Key format: airflow:run_{dag_run_id}:creds                     │
│  If TTL expires before cleanup: AirflowFailException            │
└─────────────────────────────────────────────────────────────────┘
```

### Component 1: Unified Secret Provider

**File**: `packages/shared_utils/shared_utils/secret_provider.py`

`read_secret(key, default)` resolves secrets in order:
1. Filesystem: `/run/secrets/{key.lower()}`
2. Environment variable: `os.environ[key.upper()]`
3. Default value

`InfraSecrets` is a frozen dataclass loaded once via `get_infra_secrets()` (singleton):

| Field | Default (dev) | Source |
|---|---|---|
| `mysql_password` | `control_plane` | Control plane DB |
| `postgres_password` | `airflow` | Airflow metadata DB |
| `redis_password` | `changeme_redis` | Redis transient vault |
| `airflow_fernet_key` | `""` | Variable/Connection encryption |
| `airflow_webserver_secret` | `airflow-secret-key-...` | Session cookies |
| `airflow_password` | `airflow` | API authentication |
| `kafka_password` | `None` | Future SASL support |

Customer credentials (S3, MongoDB, MinIO) are **not** in `InfraSecrets` — they come from the business database `auths` table and flow through the Redis transient vault.

### Component 2: TLS-Aware Redis Client

**File**: `packages/shared_utils/shared_utils/redis_client.py`

Singleton client with adaptive TLS:
- Checks `os.path.isfile("/etc/ssl/redis/ca.crt")`
- If present: `ssl=True`, `ssl_ca_certs`, `ssl_certfile`, `ssl_keyfile`
- If absent: plain TCP (development)

Credential vault helpers:
- `store_credentials(dag_run_id, creds, ttl=1800)` — atomic `SETEX`
- `fetch_credentials(dag_run_id)` — `GET` or raise `AirflowFailException`
- `delete_credentials(dag_run_id)` — explicit `DELETE`

The `redis` package is optional — import is wrapped in `try/except ImportError` in `__init__.py` so services that don't need Redis aren't affected.

### Component 3: Fernet Key from Docker Secret

**Files**: `scripts/generate_fernet_key.py`, `docker/entrypoint-secrets.sh`

- `generate_fernet_key.py` generates a Fernet key into `docker/secrets/fernet_key` (idempotent — won't overwrite existing)
- `entrypoint-secrets.sh` reads Docker secret files into environment variables before exec-ing the Airflow entrypoint:
  ```bash
  export AIRFLOW__CORE__FERNET_KEY=$(cat /run/secrets/fernet_key)
  export REDIS_PASSWORD=$(cat /run/secrets/redis_password)
  exec /entrypoint "$@"
  ```
- Docker Compose mounts secrets via the top-level `secrets:` block

### Component 4: Operator Changes (XCom to Redis)

**File**: `airflow/plugins/operators/s3_to_mongo_operators.py`

| Task | Before | After |
|---|---|---|
| PrepareTask | `ti.xcom_push(key="credentials", value=creds)` | `store_credentials(dag_run.run_id, creds)` |
| ValidateTask | `ti.xcom_pull(task_ids="prepare", key="credentials")` | `fetch_credentials(dag_run_id)` |
| ExecuteTask | `ti.xcom_pull(task_ids="prepare", key="credentials")` | `fetch_credentials(dag_run_id)` |
| CleanUpTask | `_clear_sensitive_xcom()` (DELETE from xcom table) | `delete_credentials(dag_run_id)` |

Non-sensitive configuration (bucket names, collection names, integration IDs) still flows through XCom. Only sensitive auth material moves to Redis.

### Component 5: Service Configuration Integration

The control plane uses `@model_validator(mode="after")` in its Pydantic `Settings` class to override sensitive fields via the unified secret provider:

**`control_plane/app/core/config.py`**:
```python
@model_validator(mode="after")
def _resolve_secrets(self) -> "Settings":
    secret_key = read_secret("SECRET_KEY")
    if secret_key:
        object.__setattr__(self, "SECRET_KEY", secret_key)
    airflow_pw = read_secret("AIRFLOW_PASSWORD")
    if airflow_pw:
        object.__setattr__(self, "AIRFLOW_PASSWORD", airflow_pw)
    return self
```

**`packages/shared_utils/shared_utils/db.py`**: `_build_default_db_url()` uses `get_infra_secrets().mysql_password` when constructing the control plane DB URL from components (fallback when `CONTROL_PLANE_DB_URL` env var is not set).

### Component 6: Docker Compose Hardening

**File**: `docker-compose.yml`

- All hardcoded passwords replaced with `${VAR:-default}` pattern
- Redis service added (`redis:7-alpine`, password-protected, health-checked)
- Docker secrets block for `fernet_key` and `redis_password`
- Custom entrypoint on all Airflow services
- `redis>=5.0.0` added to `_PIP_ADDITIONAL_REQUIREMENTS`

### Component 7: Kubernetes Template

**File**: `k8s/secrets/infra-secrets.yaml`

Opaque Secret with placeholder `stringData` for all infrastructure secrets. Intended to be mounted at `/run/secrets/` via a volume mount. Not used in testing — Docker-only workflow for now.

### Test Coverage

**Shared Utils Tests**:

| Test file | Coverage |
|---|---|
| `packages/shared_utils/tests/test_secret_provider.py` | File/env/default resolution, case-insensitive lookup, empty file skipping, `InfraSecrets.load()`, singleton, frozen dataclass |
| `packages/shared_utils/tests/test_redis_client.py` | Plain TCP vs TLS detection, singleton, custom host/port, store/fetch/delete credentials, TTL, missing key exception |

**Operator Tests** (`airflow/tests/test_s3_to_mongo_operators.py`):
- PrepareTask: asserts `store_credentials` called with `dag_run.run_id`
- Validate/Execute: asserts `fetch_credentials` returns expected dict
- CleanUp: asserts `delete_credentials` called with `dag_run_id`

### Files Created/Modified

| File | Status | Purpose |
|---|---|---|
| `packages/shared_utils/shared_utils/secret_provider.py` | New | Unified secret resolution + InfraSecrets |
| `packages/shared_utils/shared_utils/redis_client.py` | New | TLS-aware Redis + credential vault |
| `scripts/generate_fernet_key.py` | New | Fernet key generation |
| `docker/entrypoint-secrets.sh` | New | Docker secret to env var bridge |
| `docker/secrets/.gitkeep` | New | Gitignored secrets directory |
| `k8s/secrets/infra-secrets.yaml` | New | K8s secrets template |
| `packages/shared_utils/tests/test_secret_provider.py` | New | Secret provider tests |
| `packages/shared_utils/tests/test_redis_client.py` | New | Redis client tests |
| `docker-compose.yml` | Modified | Redis service, secrets, password variables |
| `packages/shared_utils/shared_utils/__init__.py` | Modified | New exports (secret_provider, redis_client) |
| `packages/shared_utils/shared_utils/db.py` | Modified | Secret-aware DB URL construction |
| `control_plane/app/core/config.py` | Modified | Pydantic secret resolution |
| `airflow/plugins/operators/s3_to_mongo_operators.py` | Modified | XCom to Redis for credentials |
| `airflow/tests/test_s3_to_mongo_operators.py` | Modified | Redis mock assertions |
| `.gitignore` | Modified | `docker/secrets/*` |
| `.env.example` | Modified | All new env vars documented |

---

## Controller DAG: Single Hourly Dispatcher with Dynamic Task Mapping

### The Problem

The previous architecture used static dispatcher DAGs — one per schedule hour (`s3_to_mongo_daily_02`, `s3_to_mongo_daily_03`, etc.) plus separate weekly and monthly DAGs. This required 26+ DAG files to cover all hours and schedule types, with each file duplicating the same query-and-trigger logic.

### The Solution

A single **Controller DAG** (`s3_to_mongo_controller`) runs every hour (`0 * * * *`) and uses Airflow's Dynamic Task Mapping (DTM) to dispatch all due integrations:

```
Phase A: @task find_due_integrations()
  → Queries: WHERE utc_next_run <= now AND usr_sch_status = 'active'
  → Builds conf dict per integration (same as control plane API / CDC processor)
  → Advances utc_next_run to next cron occurrence
  → Returns list[dict] with {conf, trigger_run_id}

Phase B: TriggerDagRunOperator.partial(...).expand_kwargs(due)
  → Fires one s3_to_mongo_ondemand DAG run per due integration
  → wait_for_completion=False — controller finishes in seconds
```

### Key Design Decisions

1. **`utc_next_run <= now` over cron-hour matching**: The `utc_next_run` column (maintained by croniter) serves as a universal "is due" check. Daily, weekly, and monthly schedules are handled by a single query — no per-schedule-type branching.

2. **`utc_next_run` advanced in Phase A (before trigger)**: Phase B (`TriggerDagRunOperator`) is fire-and-forget with no callback. Advancing in Phase A before triggering means a Phase B failure could theoretically skip a run, but `catchup=False` + `utc_next_run <= now` means past-due integrations are naturally picked up on the next hourly run.

3. **Per-schedule backfill policy**: Daily integrations advance `utc_next_run` from `now` (skip missed runs — replaying a week of stale dailies would flood the system). Weekly and monthly advance from the current `utc_next_run` (backfill one occurrence per controller cycle until caught up). This means after 3 weeks of downtime, a weekly integration dispatches 3 runs across 3 consecutive hourly cycles.

3. **Native `TriggerDagRunOperator` over REST API**: Eliminates the need for `AIRFLOW_INTERNAL_API_URL` / username / password inside the dispatcher. More reliable (Airflow internal mechanism vs HTTP).

4. **`determine_dag_id()` simplified**: All schedule types now resolve to `{workflow_name}_ondemand`. The per-hour DAG IDs (`_daily_02`, `_daily_14`, etc.) no longer exist.

### Files Changed

| File | Action |
|------|--------|
| `airflow/dags/s3_to_mongo_controller.py` | **Created** — single Controller DAG |
| `airflow/plugins/operators/dispatch_operators.py` | **Refactored** — replaced `DispatchScheduledIntegrationsTask` class with `find_and_prepare_due_integrations()` function |
| `packages/shared_utils/shared_utils/dag_trigger.py` | **Updated** — `determine_dag_id()` always returns `_ondemand` |
| `airflow/dags/s3_to_mongo_daily_02.py` | **Deleted** |
| `airflow/dags/s3_to_mongo_daily_03.py` | **Deleted** |
| `airflow/dags/s3_to_mongo_weekly.py` | **Deleted** |
| `airflow/dags/s3_to_mongo_monthly.py` | **Deleted** |

### Three Trigger Paths (Unified)

All paths target `s3_to_mongo_ondemand` and build identical conf dicts:

| Trigger Source | When | Sample `dag_run_id` |
|---|---|---|
| Control plane API | On-demand / manual | `ws-abc_s3_to_mongo_ondemand_manual_20260310_183116_3` |
| CDC processor DAG | Debezium CDC event | `ws-abc_s3_to_mongo_ondemand_cdc_20260310_183335_2` |
| Controller DAG | Hourly cron | `ws-abc_s3_to_mongo_ondemand_scheduled_20260310_204234_0` |

### Test Coverage

**Unit Tests** (`airflow/tests/test_s3_to_mongo_operators.py` — `TestFindAndPrepareDueIntegrations`):
- Due integrations returned with correct confs
- Future `utc_next_run` integrations excluded (empty result)
- Error isolation (one fails, others continue)
- Missing `CONTROL_PLANE_DB_URL` returns empty list
- `trigger_run_id` format validated
- `utc_next_run` advanced after dispatch
- Advance failure does not break dispatch

**E2E Test** (`control_plane/tests/test_cron_scheduled_e2e.py`):
Full pipeline: Seed MySQL → Upload to MinIO → Deploy test controller DAG → Wait for scheduler → Verify MongoDB data → Verify IntegrationRun record. See [E2E_TEST_GUIDE.md](E2E_TEST_GUIDE.md) for details.

---

## Summary

**CDC Event Processing**: Native Airflow `AssetWatcher` + `KafkaMessageQueueTrigger` — replaces the standalone Kafka consumer microservice. CDC events flow through the triggerer process into `cdc_integration_processor` DAG runs with retry-before-DLQ, distributed tracing, and audit trail. Diagnostic endpoints expose Kafka/AssetEvent divergence.

**Controller DAG**: Single hourly dispatcher using DTM — replaces 26+ static DAG files with one Controller DAG that queries `utc_next_run <= now` and fires `TriggerDagRunOperator.expand_kwargs()` for all due integrations.

**Security Overhaul**: Unified secret provider (file/env/default), Fernet encryption via Docker secrets, Redis transient credential vault (30-min TTL, atomic SETEX, adaptive TLS), customer credentials removed from XCom/metadata DB, all hardcoded passwords replaced with env var substitution.
