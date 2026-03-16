# Implementation Summary: Kafka Consumer Service

## What Was Delivered

### 0. Design Decision  ✅

Airflow 3.1.7 now offers a stable Kafka Message Queue Trigger with stable Kafka Provider v1.12.0 starting Feb 2026. However,
### ⚖️ Technical Verdict: Airflow 3 Kafka Trigger vs. Custom Consumer Service

| Capability | Airflow 3 Kafka Trigger | Custom Consumer Microservice |
| :--- | :--- | :--- |
| **Primary Role** | **Orchestration Signaling**: "Wake up and start work." | **Stream Processing**: High-volume data movement/transformation. |
| **Offset Management** | ⚠️ **Basic**: Relies on Auto-commit; lacks granular consumer-group state management. | ✅ **Robust**: Supports manual commits and precise offset tracking. |
| **Delivery Guarantee** | **At-Most-Once / At-Least-Once**: Risk of message loss or double-triggering during crashes. | ✅ **Exactly-Once**: Possible via Kafka Transactional API and idempotent producers. |
| **Throughput** | ⚠️ **Moderate**: Limited by the `asyncio` event loop capacity of the Triggerer process. | ✅ **Ultra-High**: Horizontally scalable to millions of events/sec. |
| **Error Handling (DLQ)** | ❌ **Manual**: No native DLQ; must be custom-coded in `apply_function`. | ✅ **Native**: Standard patterns available via libraries (e.g., Spring/Confluent). |
| **Retry Policy** | ❌ **Minimal**: No native backoff/re-queueing at the trigger level. | ✅ **Sophisticated**: Built-in exponential backoff and retry-topic routing. |
| **Operational Effort** | ✅ **Low**: Managed within Airflow; no extra infra/health-checks needed. | ❌ **High**: Requires separate CI/CD, K8s manifests, and monitoring. |


### 1. Production Kafka Consumer Service ✅

**File**: [kafka_consumer/app/services/kafka_consumer_service.py](kafka_consumer/app/services/kafka_consumer_service.py)

A standalone FastAPI microservice (port 8001) that:
- Subscribes to Kafka CDC events (`cdc.integration.events`)
- Runs as an independent service, decoupled from the Control Plane
- Automatically triggers Airflow DAGs when integrations are created
- Handles errors gracefully with DLQ support
- Supports custom event handlers
- Provides health endpoints (`/health`, `/health/ready`, `/health/detailed`)
- Enables independent scaling and failover

**Key Implementation**:
```python
class KafkaConsumerService:
    def start() -> None:
        """Start consumer in background thread"""

    def stop() -> None:
        """Gracefully stop consumer"""

    def _process_message(message: dict) -> None:
        """Process CDC events"""

    def _trigger_integration_workflow(data: dict) -> None:
        """Trigger Airflow DAG for integration"""
```

### 2. Standalone Service Lifecycle ✅

**File**: [kafka_consumer/app/main.py](kafka_consumer/app/main.py)

Standalone FastAPI app with modern `lifespan` context manager:
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_kafka_consumer()
    yield
    shutdown_kafka_consumer()
```

The control plane ([control_plane/app/main.py](control_plane/app/main.py)) no longer manages the Kafka consumer — it is a pure stateless REST API.

### 3. Comprehensive Test Suite ✅

**File**: [kafka_consumer/tests/test_kafka_consumer_service.py](kafka_consumer/tests/test_kafka_consumer_service.py)

Complete test coverage with **18 test cases**:

**Unit Tests** (9 tests):
- `test_consumer_initialization` - Verify service initialization
- `test_consumer_start_stop` - Test lifecycle management
- `test_process_message_integration_created` - Test event processing
- `test_process_message_integration_updated` - Test update events
- `test_process_message_integration_deleted` - Test delete events
- `test_process_message_run_events` - Test run lifecycle events
- `test_process_message_unknown_event` - Test unknown event handling
- `test_process_message_error_handling` - Test error recovery
- `test_consumer_with_custom_handler` - Test custom handlers

**Integration Tests** (2 tests):
- `test_consumer_receives_published_message` - Test with real Kafka
- `test_multiple_messages_in_sequence` - Test batch processing

**Global Service Tests** (3 tests):
- `test_initialize_and_shutdown` - Test singleton management
- `test_initialize_twice_warning` - Test double-init handling
- `test_shutdown_when_not_initialized` - Test shutdown safety

**Health & Observability Tests** (4 tests):
- `test_health_observability_attributes` - Test metrics tracking attributes
- `test_messages_processed_counter` - Test processing counter increments
- `test_messages_failed_counter` - Test failure counter increments
- `test_is_connected_flag` - Test connection state tracking

**Run Tests**:
```bash
pytest kafka_consumer/tests/test_kafka_consumer_service.py -v -s
```

### 4. Updated E2E Test (Event-Driven) ✅

**File**: [control_plane/tests/test_s3_to_mongo_e2e.py](control_plane/tests/test_s3_to_mongo_e2e.py)

**Changed from**: Direct Airflow API calls
**Changed to**: Kafka event-driven triggering

**Old Flow**:
```
Test → Airflow API → DAG Execution
```

**New Flow**:
```
Test → Kafka Event → Consumer Service → Airflow API → DAG Execution
```

**Key Changes**:
- Added `kafka_producer` fixture
- Renamed `test_04_trigger_workflow` → `test_04_trigger_workflow_via_kafka`
- Publishes `integration.created` event to Kafka
- Waits for consumer to process event and trigger DAG
- Validates complete event-driven pipeline

**Test Steps**:
1. Upload test data to MinIO
2. Verify data in MinIO
3. Create integration via API
4. **Publish Kafka event** ← NEW
5. **Consumer triggers DAG** ← NEW
6. Verify data in MongoDB

**Run Test**:
```bash
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s
```

### 5. Complete Documentation ✅

Created three comprehensive documentation files:

**[KAFKA_CONSUMER.md](KAFKA_CONSUMER.md)** (470 lines):
- Architecture overview
- Implementation details
- Configuration guide
- Testing procedures
- Error handling
- Troubleshooting
- Production considerations
- Monitoring and observability

**[KAFKA_EVENT_DRIVEN_IMPLEMENTATION.md](KAFKA_EVENT_DRIVEN_IMPLEMENTATION.md)** (550+ lines):
- Complete implementation summary
- Architecture diagrams
- Data flow visualization
- Configuration details
- Running instructions
- Monitoring commands
- Future enhancements

**[E2E_TEST_GUIDE.md](E2E_TEST_GUIDE.md)** (updated):
- Updated with Kafka consumer section
- New event-driven test flow diagram
- Added kafka-python to prerequisites

## How It Works

### Complete Data Flow

```
1. Integration Created
   User → Control Plane API → MySQL

2. CDC Event Published
   Debezium → Kafka Topic (cdc.integration.events)

3. Consumer Processes Event
   Kafka Consumer Service (background thread)
   ↓
   Reads event from Kafka
   ↓
   Validates event data
   ↓
   Calls IntegrationService.trigger_integration()
   ↓
   Makes Airflow REST API call
   ↓
   DAG triggered!

4. Airflow Executes Workflow
   DAG: Prepare → Validate → Execute → Cleanup

5. Data Transferred
   MinIO/S3 → MongoDB
```

### Consumer Service Architecture

```
┌─────────────────────────────────────────────┐
│  Kafka Consumer Microservice (port 8001)    │
│                                             │
│  lifespan(app):                             │
│    → initialize_kafka_consumer()            │
│       → KafkaConsumerService.start()        │
│          → Background thread launched       │
│          → Subscribes to Kafka topic        │
│    yield                                    │
│    → shutdown_kafka_consumer()              │
│       → Stop polling                        │
│       → Commit offsets                      │
│       → Close connections                   │
│                                             │
│  while running:                            │
│    messages = poll(kafka)                   │
│    for msg in messages:                    │
│      process_message(msg)                   │
│        ↓                                    │
│      if event == "integration.created":     │
│        trigger_integration_workflow()       │
│          ↓                                  │
│        Airflow API call                     │
│          ↓                                  │
│        DAG triggered                        │
│                                             │
│  Health Endpoints:                          │
│    GET /health          (liveness)          │
│    GET /health/ready    (readiness)         │
│    GET /health/detailed (diagnostics)       │
└─────────────────────────────────────────────┘
```

## Running the System

### 1. Start Services

```bash
# Start complete stack
docker-compose up -d

# Check services are running
docker-compose ps
```

### 2. Verify Consumer Started

```bash
# Check Kafka Consumer logs
docker-compose logs kafka-consumer | grep "Kafka consumer"

# Expected:
# INFO: Starting Kafka consumer for topic: cdc.integration.events
# INFO: Kafka consumer connected to kafka:29092
# INFO: Kafka consumer initialized successfully

# Check health endpoints
curl http://localhost:8001/health
curl http://localhost:8001/health/ready
curl http://localhost:8001/health/detailed
```

### 3. Monitor Activity

**Kafka UI**: http://localhost:8081
- Navigate to Consumer Groups
- Find: `cdc-consumer`
- View lag and consumption rate

**Kafka Consumer Logs**:
```bash
docker-compose logs -f kafka-consumer | grep "Processing CDC event"
```

### 4. Run Tests

```bash
# Kafka consumer service tests
pytest kafka_consumer/tests/test_kafka_consumer_service.py -v -s

# E2E test (event-driven)
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s
```

## Configuration

**Docker Compose** ([docker-compose.yml](docker-compose.yml)):
```yaml
kafka-consumer:
  environment:
    DATABASE_URL: mysql+pymysql://control_plane:control_plane@mysql:3306/control_plane
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    AIRFLOW_API_URL: http://airflow-webserver:8080/api/v2
```

**Application Config** ([kafka_consumer/app/core/config.py](kafka_consumer/app/core/config.py)):
```python
KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
KAFKA_TOPIC_CDC: str = "cdc.integration.events"
KAFKA_CONSUMER_GROUP: str = "cdc-consumer"
```

**Consumer Settings**:
- Group ID: `cdc-consumer` (configurable via `KAFKA_CONSUMER_GROUP`)
- Auto Offset Reset: `earliest`
- Auto Commit: Enabled
- Max Records per Poll: 10
- Consumer Timeout: 1 second

## Testing

### Run All Tests

```bash
# Kafka consumer service tests
pytest kafka_consumer/tests/test_kafka_consumer_service.py -v -s

# E2E test (event-driven)
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s

# All CDC tests
pytest control_plane/tests/test_cdc_kafka.py -v -s
```

### Expected Results

**Kafka Consumer Tests**: 18/18 passing
- 9 unit tests
- 2 integration tests
- 3 global service tests
- 4 health/observability tests

**E2E Test**: 6/6 passing
- Upload to MinIO ✓
- Verify MinIO data ✓
- Create integration ✓
- Trigger via Kafka ✓ (NEW)
- Verify MongoDB data ✓
- Summary ✓

## Files Created/Modified

### Kafka Consumer Microservice (New)

1. **kafka_consumer/app/main.py** — FastAPI app with lifespan context manager
2. **kafka_consumer/app/core/config.py** — Slim Settings (Kafka, DB, Airflow, logging)
3. **kafka_consumer/app/core/logging.py** — JSON logging configuration
4. **kafka_consumer/app/services/kafka_consumer_service.py** — Consumer service with health observability
5. **kafka_consumer/app/api/health.py** — Liveness, readiness, and detailed health endpoints
6. **kafka_consumer/tests/test_kafka_consumer_service.py** — 18 test cases
7. **kafka_consumer/tests/conftest.py** — Test configuration
8. **docker/Dockerfile.kafka-consumer** — Container image for the consumer service
9. **requirements-kafka-consumer.txt** — Minimal dependencies

### Control Plane (Modified)

1. **control_plane/app/main.py** — Removed Kafka consumer lifecycle (now pure REST API)
2. **control_plane/app/core/config.py** — Removed Kafka settings
3. **control_plane/app/services/kafka_consumer_service.py** — Deleted (moved to kafka_consumer/)
4. **control_plane/tests/test_kafka_consumer_service.py** — Deleted (moved to kafka_consumer/)

### Infrastructure (Modified)

1. **docker-compose.yml** — Added kafka-consumer service, removed Kafka dependency from control-plane

### Documentation (Modified)

1. **README.md** — Updated architecture, project structure, service access
2. **IMPLEMENTATION_SUMMARY.md** (this file) — Updated to reflect standalone architecture

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

# Verify consumer is running
docker-compose logs kafka-consumer | grep "Kafka consumer"

# Check consumer health
curl http://localhost:8001/health/detailed

# Run tests
pytest kafka_consumer/tests/test_kafka_consumer_service.py -v
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s

# Monitor consumer activity
docker-compose logs -f kafka-consumer | grep "Processing CDC event"

# Check Kafka consumer group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cdc-consumer
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

---

## Summary

✅ **Extracted**: Kafka consumer into standalone FastAPI microservice (port 8001)
✅ **Decoupled**: Control plane is now a pure stateless REST API (no Kafka dependency)
✅ **Tested**: 18 comprehensive test cases for the consumer service
✅ **Observable**: Liveness, readiness, and detailed health endpoints
✅ **Containerized**: Dedicated Dockerfile and docker-compose service
✅ **Documented**: Architecture, configuration, and operational guides updated

The Kafka consumer now runs as an independent microservice that:
- Scales independently from the control plane
- Supports failover via Kafka consumer group rebalancing
- Provides health endpoints for container orchestration
- Uses modern FastAPI lifespan for clean startup/shutdown
- Connects directly to MySQL via pymysql (no aiomysql dependency)
- Uses `shared_models` Core tables directly (no ORM dependency)
