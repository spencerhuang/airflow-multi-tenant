# Audit Trail Design Document

**Status**: Draft v5
**Date**: 2026-03-22
**Scope**: GDPR compliance + SOC 2 alignment
**Tenant key**: `customer_guid`
**Architecture**: Kafka-based event streaming with dedicated audit consumer service
**Storage isolation**: Schema-per-customer with template-based provisioning (~100 customers in 2 years)

---

## 1. Problem Statement

The platform has three services that perform auditable actions:

| Service | Auditable actions | Current visibility |
|---|---|---|
| Control Plane (API) | CRUD on integrations, credentials, manual triggers | Structured logs only |
| Kafka Consumer | CDC-triggered DAG runs | Metrics counters only |
| Airflow DAGs | Scheduled dispatch, credential access, data transfer, run outcomes | `integration_runs` table (no actor/context) |

For scheduled operations, **only Airflow is doing anything** — there is no user API call to trace. If a nightly integration fails, corrupts data, or accesses credentials, the only record is a row in `integration_runs` with a boolean `is_success`. We cannot answer:

- "Which integrations accessed credentials for customer X last week?"
- "How many records were transferred from S3 bucket Y to MongoDB collection Z?"
- "What was the exact sequence of events when integration 42 failed on Tuesday?"
- "Who or what triggered this DAG run — the scheduler, CDC, or a manual API call?"

---

## 2. Compliance Targets

### GDPR (primary)

| Article | Requirement | How we satisfy it |
|---|---|---|
| 5(2) | Accountability — demonstrate lawful processing | Audit trail proves what processing occurred, when, by what actor |
| 15 | Right of access — show what processing occurred on a subject's data | Query audit log by `customer_guid` to reconstruct data processing history |
| 17(3)(e) | Audit logs exempt from right-to-erasure when needed for legal compliance | Retain security audit logs; anonymize PII after retention window |
| 33/34 | Breach notification — know what happened | Full timeline reconstruction across all three services via traceparent correlation |

### SOC 2 (secondary, closing toward)

| Criteria | Requirement | How we satisfy it |
|---|---|---|
| CC6.1 | Log logical access events | Record credential access in Airflow operators + API calls |
| CC7.1 | Detect anomalies through monitoring | Queryable audit store enables alerting on unusual patterns |
| CC8.1 | Log configuration changes with before/after | Capture state diff on every control plane mutation |

### What we are NOT targeting

- HIPAA (no PHI in scope)
- ISO 27001 (heavier process overhead, not needed now)

---

## 3. What Gets Audited

### 3.1 Control Plane Events (actor = user or API client)

| Event Type | Action | Trigger | Producer location |
|---|---|---|---|
| `integration.created` | create | POST /integrations | `IntegrationService.create_integration()` |
| `integration.updated` | update | PUT /integrations/{id} | `IntegrationService.update_integration()` |
| `integration.deleted` | delete | DELETE /integrations/{id} | `IntegrationService.delete_integration()` |
| `integration.triggered` | execute | POST /integrations/trigger | `IntegrationService.trigger_dag_run()` |
| `auth.created` | create | Credential provisioned | Future auth API |
| `auth.updated` | update | Credential rotated | Future auth API |
| `auth.deleted` | delete | Credential removed | Future auth API |
| `customer.created` | create | Customer onboarded | Future customer API |
| `customer.updated` | update | Customer quota changed | Future customer API |
| `audit.queried` | read | Audit API accessed | Audit service query endpoint |

### 3.2 Kafka Consumer Events (actor = service:kafka-consumer)

| Event Type | Action | Trigger | Producer location |
|---|---|---|---|
| `integration.cdc_triggered` | execute | CDC event triggers DAG | `KafkaConsumerService._trigger_integration_workflow()` |
| `message.dead_lettered` | error | Max retries exceeded | `KafkaConsumerService._send_to_dlq()` |

### 3.3 Airflow Execution Events (actor = service:airflow-scheduler or service:airflow-controller)

Captured by **Airflow Listener plugin** — zero changes to operator code.

| Event Type | Action | Listener hook | Data source |
|---|---|---|---|
| `integration.dispatched` | execute | `on_task_instance_success` (controller `find_due_integrations` task) | `dag_run.conf` |
| `integration.run.started` | execute | `on_dag_run_running` (ondemand DAG) | `dag_run.conf` |
| `auth.accessed` | read | `on_task_instance_success` (prepare task) | `dag_run.conf` (auth_id) |
| `data.destination.written` | write | `on_task_instance_success` (execute task) | XCom return value (files_processed, records_written) |
| `integration.run.completed` | execute | `on_dag_run_success` (ondemand DAG) | `dag_run.conf` + XCom stats from execute task |
| `integration.run.failed` | execute | `on_dag_run_failed` (ondemand DAG) | `dag_run.conf` + XCom errors from cleanup task |
| `auth.cleared` | delete | `on_task_instance_success` (cleanup task) | `dag_run.conf` |

### 3.4 What we do NOT audit (volume control)

- Read-only list/get calls on integrations (low security value, high volume)
- Airflow internal task state transitions (queued → running → success — already tracked by Airflow metadata DB)
- Health check endpoints
- XCom reads between tasks within the same DAG run

---

## 4. Audit Event Schema

### Storage model: schema-per-customer

Each customer gets their own MySQL schema, cloned from a template during onboarding:

```
MySQL Instance
├── control_plane              (existing — operational data)
├── audit_template             (blueprint — never written to)
│   └── audit_events           (canonical table definition)
├── audit_{customer_guid_1}    (cloned from template)
│   └── audit_events
├── audit_{customer_guid_2}    (cloned from template)
│   └── audit_events
└── ... (~100 schemas at scale)
```

### Table definition (per-customer schema)

```
audit_events   (lives in schema: audit_{customer_guid})
─────────────────────────────────────────────────────────────────
event_id        CHAR(36)      PK, UUIDv7 (time-ordered)
timestamp       DATETIME(6)   UTC, microsecond precision, NOT NULL
event_type      VARCHAR(100)  Dot-notation hierarchy, NOT NULL, indexed
actor_id        VARCHAR(100)  User ID / service account / "system", NOT NULL
actor_type      VARCHAR(20)   "user" | "service" | "system", NOT NULL
actor_ip        VARCHAR(45)   Source IP (supports IPv6), nullable
resource_type   VARCHAR(50)   "integration" | "auth" | "data" | ...
resource_id     VARCHAR(100)  ID of the affected resource
action          VARCHAR(20)   "create" | "read" | "update" | "delete" | "execute" | "write" | "error"
outcome         VARCHAR(20)   "success" | "failure" | "denied"
before_state    JSON          Snapshot before mutation (sensitive values masked), nullable
after_state     JSON          Snapshot after mutation (sensitive values masked), nullable
trace_id        VARCHAR(100)  W3C trace_id from traceparent, indexed
request_id      VARCHAR(100)  HTTP request ID or dag_run_id
metadata        JSON          Event-specific context
─────────────────────────────────────────────────────────────────
```

**No `customer_guid` column** — the schema IS the tenant boundary. No row-level filtering needed, no risk of cross-tenant leaks.

### Schema template

The `audit_template` schema is the single source of truth for the table definition:

```sql
-- Created once during platform setup (init.sql)
CREATE SCHEMA IF NOT EXISTS audit_template;

CREATE TABLE audit_template.audit_events (
    event_id        CHAR(36)        PRIMARY KEY,
    timestamp       DATETIME(6)     NOT NULL,
    event_type      VARCHAR(100)    NOT NULL,
    actor_id        VARCHAR(100)    NOT NULL,
    actor_type      VARCHAR(20)     NOT NULL,
    actor_ip        VARCHAR(45)     NULL,
    resource_type   VARCHAR(50)     NULL,
    resource_id     VARCHAR(100)    NULL,
    action          VARCHAR(20)     NOT NULL,
    outcome         VARCHAR(20)     NOT NULL,
    before_state    JSON            NULL,
    after_state     JSON            NULL,
    trace_id        VARCHAR(100)    NULL,
    request_id      VARCHAR(100)    NULL,
    metadata        JSON            NULL,

    INDEX idx_event_type (event_type),
    INDEX idx_trace_id (trace_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_resource (resource_type, resource_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  PARTITION BY RANGE (UNIX_TIMESTAMP(timestamp)) (
    PARTITION p_current VALUES LESS THAN MAXVALUE
);
```

### Template-based provisioning (onboarding)

```python
# audit_service/app/services/schema_manager.py

class AuditSchemaManager:
    """Manages per-customer audit schemas cloned from template."""

    TEMPLATE_SCHEMA = "audit_template"

    async def provision_customer(self, customer_guid: str) -> None:
        """Clone audit_template into a new per-customer schema.
        Called during customer onboarding.
        """
        schema = self._schema_name(customer_guid)
        await self.db.execute(text(f"CREATE SCHEMA IF NOT EXISTS `{schema}`"))
        await self.db.execute(text(
            f"CREATE TABLE `{schema}`.audit_events LIKE "
            f"`{self.TEMPLATE_SCHEMA}`.audit_events"
        ))
        # Grant audit service account INSERT + SELECT only
        await self.db.execute(text(
            f"GRANT INSERT, SELECT ON `{schema}`.* TO 'audit_svc'@'%'"
        ))
        await self.db.commit()

    async def deprovision_customer(self, customer_guid: str) -> None:
        """Drop a customer's audit schema.
        Called during customer offboarding. GDPR erasure is this simple.
        """
        schema = self._schema_name(customer_guid)
        # Export to S3 first if retention requires it (see section 10)
        await self.db.execute(text(f"DROP SCHEMA IF EXISTS `{schema}`"))
        await self.db.commit()

    async def migrate_all(self) -> None:
        """Apply template changes to all existing customer schemas.
        Run after updating audit_template.
        """
        schemas = await self._list_customer_schemas()
        for schema in schemas:
            await self._apply_template_diff(schema)

    def _schema_name(self, customer_guid: str) -> str:
        return f"audit_{customer_guid}"

    async def _list_customer_schemas(self) -> list[str]:
        result = await self.db.execute(text(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 'audit\\_%' "
            "AND schema_name != 'audit_template'"
        ))
        return [row[0] for row in result]

    async def _apply_template_diff(self, schema: str) -> None:
        """Compare schema against template and apply ALTER statements.
        Uses INFORMATION_SCHEMA.COLUMNS to detect differences.
        """
        # Get template columns
        template_cols = await self._get_columns(self.TEMPLATE_SCHEMA)
        customer_cols = await self._get_columns(schema)

        # Add missing columns
        for col_name, col_def in template_cols.items():
            if col_name not in customer_cols:
                await self.db.execute(text(
                    f"ALTER TABLE `{schema}`.audit_events "
                    f"ADD COLUMN {col_def}"
                ))

        # Add missing indexes (compare INFORMATION_SCHEMA.STATISTICS)
        # ... similar pattern
        await self.db.commit()
```

### Why schema template helps

| Without template | With template |
|---|---|
| DDL hardcoded in onboarding code | `CREATE TABLE ... LIKE audit_template.audit_events` — one line |
| Migration: update hardcoded DDL + write ALTER for existing | Update template → `migrate_all()` diffs and applies |
| Schema drift between old/new customers | Template is single source of truth; `migrate_all()` enforces consistency |
| Testing: test against production-like schema? | Test against `audit_template` — it's always canonical |

### Design notes

1. **`trace_id` is the primary cross-service correlator.** The traceparent already propagates from Debezium → Kafka → Airflow → tasks. For control plane API calls, a trace_id is generated at the middleware layer. Query by trace_id to see everything that happened for a single operation.

2. **`request_id` is secondary context** — HTTP request ID for API calls, `dag_run_id` for Airflow events.

3. **`actor_ip` is nullable** — Airflow tasks don't have a meaningful source IP.

4. **`before_state` / `after_state` are nullable** — Execution events like `data.destination.written` have no before/after state; they carry stats in `metadata` instead.

### Metadata examples by event type

```json
// integration.dispatched
{"workspace_id": "ws-123", "trigger_run_id": "ctrl__42__20260322T020000", "schedule_type": "daily"}

// auth.accessed
{"workspace_id": "ws-123", "auth_id": 5, "dag_run_id": "s3_to_mongo_ondemand__20260322T020000", "stored_in": "redis", "ttl_seconds": 1800}

// data.source.accessed
{"workspace_id": "ws-123", "s3_bucket": "acme-data", "s3_prefix": "daily/", "files_listed": 12}

// data.destination.written
{"workspace_id": "ws-123", "mongo_database": "acme_db", "mongo_collection": "events", "records_written": 1847, "files_processed": 12}

// integration.run.failed
{"workspace_id": "ws-123", "dag_run_id": "...", "errors": [{"task_id": "validate", "error_code": "S3_CONNECTION_FAILED", "message": "..."}]}
```

---

## 5. Architecture

```
 PRODUCERS (each service)                BROKER                    CONSUMER
 ────────────────────────                ──────                    ────────

┌──────────────────────────────────┐
│ Control Plane / Kafka Consumer   │
│ / Airflow Listener Plugin        │
│                                  │
│  Business logic (main thread)    │
│    │                             │
│    │ emit() — returns instantly  │
│    ▼                             │
│  ┌────────────────────────────┐  │
│  │ AuditProducer              │  │
│  │                            │  │
│  │  emit() {                  │  │
│  │    build dict              │  │
│  │    queue.put_nowait(dict)  │  │      ┌──────────────────────────┐
│  │    return (no exceptions)  │  │      │  Kafka topic:            │
│  │  }                         │  │      │  audit.events            │
│  │                            │  │      │                          │
│  │  Background daemon thread: │  │      │  Partitioned by          │
│  │  ┌──────────────────────┐  │  │      │  customer_guid           │
│  │  │ _drain_loop()        │──│──│─────▶│  (ordering per customer) │
│  │  │ while True:          │  │  │      └────────────┬─────────────┘
│  │  │   event = queue.get()│  │  │                   │
│  │  │   serialize to JSON  │  │  │                   │
│  │  │   kafka.send()       │  │  │                   │
│  │  │   on error: log warn │  │  │                   │
│  │  └──────────────────────┘  │  │                   │
│  └────────────────────────────┘  │                   │
└──────────────────────────────────┘                   │
                                                       │ consume
                                                       ▼
                                          ┌──────────────────────────┐
                                          │  Audit Service           │
                                          │  (FastAPI :8002)         │
                                          │                          │
                                          │  ┌────────────────────┐  │
                                          │  │ KafkaConsumer      │  │
                                          │  │ - deserialize      │  │
                                          │  │ - mask sensitive   │  │
                                          │  │ - validate schema  │  │
                                          │  │ - write to MySQL   │  │
                                          │  └────────────────────┘  │
                                          │                          │
                                          │  ┌────────────────────┐  │
                                          │  │ Query API          │  │
                                          │  │ GET /api/v1/audit  │  │
                                          │  │ - cursor-based     │  │
                                          │  │ - scoped by        │  │
                                          │  │   customer_guid    │  │
                                          │  └────────────────────┘  │
                                          └────────────┬─────────────┘
                                                       │
                                                       ▼
                                          ┌──────────────────────────┐
                                          │  MySQL: audit_events     │
                                          │  - append-only           │
                                          │  - INSERT + SELECT only  │
                                          │  - monthly partitions    │
                                          └──────────────────────────┘
```

### Component responsibilities

| Component | Responsibility |
|---|---|
| **AuditProducer** (shared library in `packages/shared_utils/`) | ThreadedStrategy producer (bg thread + queue). Used by all services. In Airflow, the Listener plugin owns the producer instance — it runs inside the worker process (long-lived), so the bg thread works. `emit()` never raises, never breaks business logic. |
| **Kafka topic `audit.events`** | Durable buffer. Partitioned by `customer_guid` for per-customer ordering. Retention: 7 days (safety net if consumer is down). |
| **Audit Service** (new FastAPI :8002) | Single consumer: deserializes, masks sensitive fields, validates, writes to MySQL. Also serves the query API. Owns all write + read logic for audit data. |
| **MySQL `audit_events`** | Append-only storage. Separate grants from control plane service account. Monthly partitioned. |

### Why Kafka (not same-transaction or HTTP)

| Concern | Same-transaction (v1 design) | Kafka (v2 design) |
|---|---|---|
| Airflow as producer | Cannot participate in control plane DB transactions | Produces to Kafka natively — no DB session needed |
| Failure isolation | Audit failure could block mutations | Full thread isolation — `emit()` is a queue put on main thread; serialization, Kafka send, and retries happen on a separate daemon thread. No exception can propagate to business logic. |
| Masking consistency | Each service must call masking lib | Single consumer applies all masking |
| Delivery guarantee | Exactly-once (control plane only) | At-least-once (all producers). Consumer deduplicates by `event_id`. |
| Operational cost | Zero new services | +1 service, +1 topic |

The deciding factor: **scheduled DAG runs have no API call to hook into**. The controller DAG queries MySQL directly, triggers ondemand DAGs, operators access credentials and move data — all without touching the control plane API. Kafka is the only clean way to capture these events.

### Delivery guarantee: at-least-once with idempotent consumer

Producers retry up to 3x on send failure. The consumer uses `event_id` (UUIDv7, generated by the producer) as a natural dedup key. On write, the consumer does:

```sql
INSERT INTO audit_events (event_id, ...) VALUES (...)
ON DUPLICATE KEY UPDATE event_id = event_id;  -- no-op on duplicate
```

This gives effectively-once semantics in the audit store.

---

## 6. AuditProducer (Shared Library) — Two-Strategy Design

Lives in `packages/shared_utils/shared_utils/audit_producer.py`. All three services import it.

### Goal

**Business logic must never be affected by audit.** Not by Kafka being down, not by serialization errors, not by malformed data, not by anything. The `emit()` call must be as safe as a no-op from the caller's perspective — in every runtime context.

### Why two strategies

The control plane and kafka consumer are **long-running FastAPI processes** — a background thread with an in-memory queue is ideal. But Airflow tasks have a fundamentally different execution model that breaks the background thread pattern:

| Problem | Why it happens in Airflow | Impact |
|---|---|---|
| **Fork safety** | LocalExecutor and CeleryExecutor fork worker processes. Daemon threads don't survive `os.fork()`. | Background thread created before fork is dead in the child. Events enqueued but never sent. |
| **Process lifecycle** | Tasks are short-lived (seconds to minutes). Process exits when task completes. | Daemon thread killed before it drains the queue. Events silently lost. |
| **Import-time failure** | Module-level `AuditProducer()` runs at DAG parse time. If `__init__` fails (env var missing, etc.), the module fails to import. | DAG can't parse → task can't run. **Audit broke the business flow before it even started.** |
| **KubernetesExecutor** | Each task is a separate pod. Cold start every time. | Background thread + Kafka connection overhead per task for potentially 1-2 audit events. Wasteful. |

### Solution: same interface, two backends

```python
# Factory function — callers don't know or care which strategy is used
def get_audit_producer(**kwargs) -> AuditProducer:
    """Returns the appropriate producer for the runtime context."""
    ...
```

```
┌─────────────────────────────────────────────────────────────┐
│                    AuditProducer (interface)                 │
│                                                             │
│  emit(event_type, actor_id, customer_guid, ...)  → None    │
│  close()                                         → None    │
│  stats                                           → dict    │
│                                                             │
├──────────────────────────┬──────────────────────────────────┤
│  ThreadedStrategy        │  DirectStrategy                  │
│  (FastAPI services)      │  (Airflow tasks)                 │
│                          │                                  │
│  emit() → queue.put()    │  emit() → kafka.send()          │
│  bg thread → kafka.send  │  (sync, 2s timeout, try/except) │
│                          │                                  │
│  Used by:                │  Used by:                        │
│  - Control plane :8000   │  - Controller DAG operators      │
│  - Kafka consumer :8001  │  - Ondemand DAG operators        │
└──────────────────────────┴──────────────────────────────────┘
```

---

### 6.1 Common interface

```python
class AuditProducer:
    """Audit event producer. Never raises, never blocks business logic."""

    def emit(
        self,
        event_type: str,         # "integration.created", "data.destination.written"
        actor_id: str,           # "user:john@acme.com", "service:airflow-scheduler"
        actor_type: str,         # "user" | "service" | "system"
        customer_guid: str,      # Tenant key — used as Kafka partition key
        resource_type: str,      # "integration", "auth", "data"
        resource_id: str,        # ID of affected resource
        action: str,             # "create" | "read" | "update" | "delete" | "execute" | "write"
        outcome: str,            # "success" | "failure" | "denied"
        trace_id: str = "",      # W3C trace_id for cross-service correlation
        request_id: str = "",    # HTTP request ID or dag_run_id
        actor_ip: str = "",      # Source IP (API calls only)
        before_state: dict = None,
        after_state: dict = None,
        metadata: dict = None,
    ) -> None:
        """Emit an audit event. Never raises. Never blocks."""
        ...

    def close(self) -> None:
        """Cleanup. Best-effort, with timeout."""
        ...

    @property
    def stats(self) -> dict:
        """Read-only stats for health checks."""
        ...
```

Both strategies generate `event_id` (UUIDv7) and `timestamp` at the moment `emit()` is called — before any queuing or sending — so the event always reflects the true time of the action.

---

### 6.2 ThreadedStrategy (for FastAPI services)

Used by: **control plane (:8000)** and **kafka consumer (:8001)**.

These are long-running processes where a daemon thread works well.

```
Main thread                          Background daemon thread
───────────                          ───────────────────────

emit(event_type, ...)                _drain_loop():
  │                                    │
  ├─ try:                              ├─ while True:
  │    build plain dict from args      │    event = queue.get()  # blocks
  │    queue.put_nowait(dict)          │    try:
  │  except Exception:                 │      serialize to JSON
  │    log.warning(...)  # swallow     │      kafka_producer.send(
  │    return                          │        topic, key=customer_guid,
  │                                    │        value=json_bytes)
  ├─ return  # instant                 │    except Exception:
                                       │      log.warning(...)
                                       │      drop_count += 1
```

**Key properties:**
- `put_nowait()` — never blocks, even if queue is full (caught, logged)
- Entire `emit()` body wrapped in `try/except Exception` — nothing escapes
- Only builds a plain dict on main thread — no serialization, no Kafka calls
- Lazy KafkaProducer init on background thread — services boot even if Kafka is unreachable
- Bounded queue (10,000 events, ~10MB) — prevents memory leak
- Daemon thread — auto-killed on process exit

**Graceful shutdown (FastAPI `shutdown` event):**

```python
def close(self, timeout: float = 5.0) -> None:
    self._queue.put(_SHUTDOWN_SENTINEL)
    self._thread.join(timeout=timeout)
```

**Initialization:**

```python
# control_plane/app/main.py — at app startup
audit_producer = get_audit_producer(
    strategy="threaded",
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
)

# kafka_consumer/app/main.py — same pattern
audit_producer = get_audit_producer(
    strategy="threaded",
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
)
```

---

### 6.3 Airflow Listener Plugin (replaces DirectStrategy)

Used by: **all Airflow DAGs** — controller and ondemand.

Instead of instrumenting each operator with `emit()` calls, we use an **Airflow Listener plugin** that hooks into task/DAG lifecycle events globally. This completely separates audit from business logic.

#### Why listeners, not operator-level hooks

| Concern | Operator-level emit() | Listener plugin |
|---|---|---|
| Code coupling | Audit code mixed into every operator | Zero operator changes — audit is a separate plugin |
| Scope | Must instrument each operator individually | Global — applies to all DAGs automatically |
| New workflow types | Developer must remember to add audit | Automatically covered by the listener |
| Fork safety | DirectStrategy needed (no bg thread) | Listener runs inside the **worker process**, which is long-lived — ThreadedStrategy works |
| Import-time risk | Module-level lazy init needed | Plugin loaded by Airflow's plugin manager, isolated from DAG parsing |

#### Key insight: listeners run in the worker process

The Airflow worker (LocalExecutor, CeleryExecutor) is a **long-running process** that executes many tasks over its lifetime. The listener plugin is loaded once when the worker starts. This means:

- A background daemon thread survives across task executions (no fork issue)
- A single KafkaProducer connection is reused for all tasks in the worker
- The ThreadedStrategy (queue + bg thread) works perfectly here

For KubernetesExecutor, each pod is a fresh process, but it runs the full Airflow worker lifecycle (plugin load → task execute → shutdown), so the threaded approach still works — the daemon thread lives for the duration of the pod.

#### Listener architecture

```
Airflow Worker Process (long-running)
──────────────────────────────────────

┌──────────────────────────────────────────────────────────┐
│  Plugin: AuditListenerPlugin                             │
│  Loaded once at worker startup                           │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ AuditProducer (ThreadedStrategy)                   │  │
│  │ - background daemon thread                         │  │
│  │ - bounded queue (10K events)                       │  │
│  │ - lazy KafkaProducer                               │  │
│  │ - reused across all task executions                │  │
│  └──────────────────────┬─────────────────────────────┘  │
│                         │                                │
│  Listener hooks:        │ emit() → queue.put_nowait()    │
│  ┌──────────────────────┴─────────────────────────────┐  │
│  │ on_dag_run_running(dag_run)                        │  │
│  │   → if ondemand DAG: emit integration.run.started  │  │
│  │                                                    │  │
│  │ on_task_instance_success(task_instance)             │  │
│  │   → if prepare: emit auth.accessed                 │  │
│  │   → if execute: emit data.destination.written      │  │
│  │     (pull stats from XCom return value)            │  │
│  │   → if cleanup: emit auth.cleared                  │  │
│  │   → if controller dispatch: emit dispatched        │  │
│  │                                                    │  │
│  │ on_task_instance_failed(task_instance)              │  │
│  │   → emit task.failed with error context            │  │
│  │                                                    │  │
│  │ on_dag_run_success(dag_run)                        │  │
│  │   → emit integration.run.completed                 │  │
│  │                                                    │  │
│  │ on_dag_run_failed(dag_run)                         │  │
│  │   → emit integration.run.failed                    │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

#### Implementation

```python
# airflow/plugins/listeners/audit_listener.py

import logging
from airflow.listeners import hookimpl

logger = logging.getLogger(__name__)

# ── Lazy singleton — created on first listener invocation ──────────
_audit_producer = None

def _get_producer():
    global _audit_producer
    if _audit_producer is None:
        try:
            from shared_utils.audit_producer import get_audit_producer
            import os
            _audit_producer = get_audit_producer(
                strategy="threaded",
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
            )
        except Exception:
            logger.warning("AuditProducer init failed, using no-op", exc_info=True)
            _audit_producer = _NoOpProducer()
    return _audit_producer


def _extract_conf(dag_run) -> dict:
    """Safely extract audit-relevant fields from dag_run.conf."""
    try:
        conf = (dag_run.conf if dag_run else None) or {}
        return {
            "customer_guid": conf.get("customer_guid", ""),
            "integration_id": conf.get("integration_id"),
            "integration_type": conf.get("integration_type", ""),
            "auth_id": conf.get("auth_id"),
            "workspace_id": conf.get("tenant_id", ""),
            "traceparent": conf.get("traceparent", ""),
        }
    except Exception:
        return {}


def _trace_id_from(conf: dict) -> str:
    """Extract trace_id from traceparent string."""
    try:
        parts = conf.get("traceparent", "").split("-")
        return parts[1] if len(parts) >= 3 else ""
    except Exception:
        return ""


# ── DAG Run Listeners ──────────────────────────────────────────────

@hookimpl
def on_dag_run_running(dag_run, msg):
    """Emitted when a DAG run transitions to RUNNING state."""
    try:
        if not dag_run.dag_id.endswith("_ondemand"):
            return  # only audit ondemand DAGs, not the controller
        conf = _extract_conf(dag_run)
        if not conf.get("customer_guid"):
            return  # not a tenant integration run
        _get_producer().emit(
            event_type="integration.run.started",
            actor_id="service:airflow-scheduler",
            actor_type="service",
            customer_guid=conf["customer_guid"],
            resource_type="integration",
            resource_id=str(conf.get("integration_id", "")),
            action="execute",
            outcome="success",
            trace_id=_trace_id_from(conf),
            request_id=dag_run.run_id,
            metadata={"workspace_id": conf["workspace_id"],
                       "integration_type": conf["integration_type"]},
        )
    except Exception:
        logger.warning("Audit listener error (dag_run_running)", exc_info=True)


@hookimpl
def on_dag_run_success(dag_run, msg):
    """Emitted when a DAG run completes successfully."""
    try:
        if not dag_run.dag_id.endswith("_ondemand"):
            return
        conf = _extract_conf(dag_run)
        if not conf.get("customer_guid"):
            return
        _get_producer().emit(
            event_type="integration.run.completed",
            actor_id="service:airflow-scheduler",
            actor_type="service",
            customer_guid=conf["customer_guid"],
            resource_type="integration",
            resource_id=str(conf.get("integration_id", "")),
            action="execute",
            outcome="success",
            trace_id=_trace_id_from(conf),
            request_id=dag_run.run_id,
            metadata={"workspace_id": conf["workspace_id"]},
        )
    except Exception:
        logger.warning("Audit listener error (dag_run_success)", exc_info=True)


@hookimpl
def on_dag_run_failed(dag_run, msg):
    """Emitted when a DAG run fails."""
    try:
        if not dag_run.dag_id.endswith("_ondemand"):
            return
        conf = _extract_conf(dag_run)
        if not conf.get("customer_guid"):
            return
        _get_producer().emit(
            event_type="integration.run.failed",
            actor_id="service:airflow-scheduler",
            actor_type="service",
            customer_guid=conf["customer_guid"],
            resource_type="integration",
            resource_id=str(conf.get("integration_id", "")),
            action="execute",
            outcome="failure",
            trace_id=_trace_id_from(conf),
            request_id=dag_run.run_id,
            metadata={"workspace_id": conf["workspace_id"],
                       "failure_reason": str(msg) if msg else None},
        )
    except Exception:
        logger.warning("Audit listener error (dag_run_failed)", exc_info=True)


# ── Task Instance Listeners ───────────────────────────────────────

@hookimpl
def on_task_instance_success(previous_state, task_instance, session):
    """Emitted when a task instance succeeds. Dispatches by task_id."""
    try:
        dag_run = task_instance.dag_run
        conf = _extract_conf(dag_run)
        if not conf.get("customer_guid"):
            return
        task_id = task_instance.task_id
        trace_id = _trace_id_from(conf)
        producer = _get_producer()
        base = dict(
            actor_id="service:airflow-scheduler",
            actor_type="service",
            customer_guid=conf["customer_guid"],
            trace_id=trace_id,
            request_id=dag_run.run_id,
        )

        if task_id == "prepare":
            producer.emit(
                event_type="auth.accessed",
                resource_type="auth",
                resource_id=str(conf.get("auth_id", "")),
                action="read",
                outcome="success",
                metadata={"workspace_id": conf["workspace_id"],
                           "stored_in": "redis"},
                **base,
            )

        elif task_id == "execute":
            # Pull stats from XCom (execute task's return value)
            stats = task_instance.xcom_pull(task_ids="execute") or {}
            producer.emit(
                event_type="data.destination.written",
                resource_type="data",
                resource_id=str(conf.get("integration_id", "")),
                action="write",
                outcome="success",
                metadata={
                    "workspace_id": conf["workspace_id"],
                    "files_processed": stats.get("files_processed"),
                    "records_written": stats.get("records_written"),
                    "records_read": stats.get("records_read"),
                },
                **base,
            )

        elif task_id == "cleanup":
            producer.emit(
                event_type="auth.cleared",
                resource_type="auth",
                resource_id=str(conf.get("auth_id", "")),
                action="delete",
                outcome="success",
                metadata={"workspace_id": conf["workspace_id"],
                           "cleared_from": "redis"},
                **base,
            )

        elif task_id == "find_due_integrations":
            # Controller DAG — dispatch event
            # The return value contains the list of dispatched integration confs
            dispatched = task_instance.xcom_pull(
                task_ids="find_due_integrations") or []
            for item in dispatched:
                item_conf = item.get("conf", {}) if isinstance(item, dict) else {}
                cg = item_conf.get("customer_guid", conf.get("customer_guid", ""))
                if cg:
                    producer.emit(
                        event_type="integration.dispatched",
                        actor_id="service:airflow-controller",
                        actor_type="service",
                        customer_guid=cg,
                        resource_type="integration",
                        resource_id=str(item_conf.get("integration_id", "")),
                        action="execute",
                        outcome="success",
                        trace_id=_trace_id_from(item_conf),
                        request_id=dag_run.run_id,
                        metadata={"workspace_id": item_conf.get("tenant_id", "")},
                    )

    except Exception:
        logger.warning("Audit listener error (task_instance_success)", exc_info=True)


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error, session):
    """Emitted when a task instance fails."""
    try:
        dag_run = task_instance.dag_run
        conf = _extract_conf(dag_run)
        if not conf.get("customer_guid"):
            return
        _get_producer().emit(
            event_type=f"task.{task_instance.task_id}.failed",
            actor_id="service:airflow-scheduler",
            actor_type="service",
            customer_guid=conf["customer_guid"],
            resource_type="integration",
            resource_id=str(conf.get("integration_id", "")),
            action="execute",
            outcome="failure",
            trace_id=_trace_id_from(conf),
            request_id=dag_run.run_id,
            metadata={
                "workspace_id": conf["workspace_id"],
                "task_id": task_instance.task_id,
                "error": str(error)[:2000] if error else None,
            },
        )
    except Exception:
        logger.warning("Audit listener error (task_instance_failed)", exc_info=True)


class _NoOpProducer:
    def emit(self, **kwargs): pass
    def close(self): pass
    @property
    def stats(self): return {"strategy": "noop"}
```

#### Plugin registration

```python
# airflow/plugins/audit_plugin.py

from airflow.plugins_manager import AirflowPlugin
from listeners import audit_listener


class AuditListenerPlugin(AirflowPlugin):
    name = "audit_listener"
    listeners = [audit_listener]
```

#### Why every listener hook is wrapped in try/except

Each `@hookimpl` function has its **own outer try/except** that swallows all exceptions. This is the final safety net — even if `_extract_conf()`, `xcom_pull()`, or the producer itself does something unexpected, the task/DAG run is never affected. Airflow's listener framework already provides some isolation, but we don't rely on it.

#### XCom data available to listeners (already pushed by existing operators)

| Task | XCom key | Data | Used for audit event |
|---|---|---|---|
| prepare | default (return value) | `{s3_bucket, s3_prefix, mongo_collection, integration_id}` | Context for `auth.accessed` |
| prepare | `traceparent` | W3C traceparent string | `trace_id` extraction |
| execute | default (return value) | `{files_processed, records_read, records_written, errors}` | Stats for `data.destination.written` |
| cleanup | *(reads, doesn't push)* | — | `auth.cleared` uses conf only |

**Zero changes to operators needed** — the listener reads what operators already produce.

#### Failure modes

| Failure | Impact on task | Listener behavior |
|---|---|---|
| Kafka broker down | **None** — `emit()` is `queue.put_nowait()` | Events queue up; bg thread retries when broker recovers |
| Listener code throws | **None** — caught by outer try/except | Warning logged, task/DAG run continues |
| Plugin fails to load | **None** — Airflow logs warning, continues without plugin | No audit events emitted; tasks run normally |
| XCom data missing | **None** — `.get()` returns None | Audit event emitted with null fields; still valuable |
| `_get_producer()` fails | **None** — returns `_NoOpProducer` | Silent no-op; warning logged once |

---

### 6.4 Design decisions

1. **`event_id` and `timestamp` generated at `emit()` call time** — before queuing. The event always reflects the true time of the action.

2. **No masking at the producer** — producers send raw data. The audit service consumer is the single place that applies masking before writing to MySQL. This prevents masking logic from being scattered across codebases.

3. **`customer_guid` is the Kafka partition key** — ensures all events for a customer are consumed in order. The producer must always provide it.

4. **Listener uses ThreadedStrategy** — the worker process is long-lived, so the background thread + queue pattern works. No fork-safety issues because the listener is loaded inside the worker process after any forking.

5. **NoOp fallback** — if the producer cannot be created at all, a no-op is used. Listeners never need conditional checks.

6. **Resolving `customer_guid` in Airflow** — The DAG run conf currently carries `tenant_id` (which is `workspace_id`). We add a join in `_build_conf()` to include `customer_guid` in conf. The listener reads it from there.

---

## 7. Producer Hook Locations

### 7.1 Control Plane (IntegrationService) — direct emit calls

```python
# integration_service.py — create_integration()
# After: await self.db.commit()
self.audit.emit(
    event_type="integration.created",
    actor_id=request.state.actor_id,      # from AuditContextMiddleware
    actor_type="user",
    customer_guid=customer_guid,           # resolved from workspace_id
    resource_type="integration",
    resource_id=str(integration.integration_id),
    action="create",
    outcome="success",
    trace_id=request.state.trace_id,
    request_id=request.state.request_id,
    actor_ip=request.state.client_ip,
    after_state=integration_dict,          # raw — consumer will mask
)
```

Same pattern for `update` (with `before_state`), `delete`, and `trigger`.

### 7.2 Kafka Consumer — direct emit calls

```python
# _trigger_integration_workflow() — after trigger_airflow_dag()
audit_producer.emit(
    event_type="integration.cdc_triggered",
    actor_id="service:kafka-consumer",
    actor_type="service",
    customer_guid=customer_guid,           # resolved from workspace join
    resource_type="integration",
    resource_id=str(integration_id),
    action="execute",
    outcome="success",
    trace_id=trace_context.trace_id,
    request_id=dag_run_id,
)
```

### 7.3 Airflow — Listener plugin (zero operator changes)

All Airflow audit events are captured by the listener plugin described in section 6.3. No `emit()` calls are added to any operator or DAG code.

| Listener hook | Emits | Data pulled from |
|---|---|---|
| `on_dag_run_running` | `integration.run.started` | `dag_run.conf` |
| `on_dag_run_success` | `integration.run.completed` | `dag_run.conf` |
| `on_dag_run_failed` | `integration.run.failed` | `dag_run.conf` + error msg |
| `on_task_instance_success` (prepare) | `auth.accessed` | `dag_run.conf` (auth_id) |
| `on_task_instance_success` (execute) | `data.destination.written` | XCom return value (stats) |
| `on_task_instance_success` (cleanup) | `auth.cleared` | `dag_run.conf` |
| `on_task_instance_success` (controller) | `integration.dispatched` (per integration) | XCom return value (dispatch list) |
| `on_task_instance_failed` (any) | `task.{task_id}.failed` | Error from callback arg |

---

## 8. Audit Service (New FastAPI :8002)

### Structure

```
audit_service/
├── app/
│   ├── main.py                  # FastAPI app, health endpoints
│   ├── core/
│   │   ├── config.py            # Settings (Kafka, MySQL, masking rules)
│   │   └── database.py          # Async MySQL connection (audit DB grants)
│   ├── services/
│   │   ├── audit_consumer.py    # Kafka consumer → mask → write to MySQL
│   │   └── audit_query.py       # Query logic for API
│   ├── api/
│   │   └── audit.py             # GET /api/v1/audit endpoint
│   ├── models/
│   │   └── audit_event.py       # ORM model
│   └── schemas/
│       └── audit.py             # Pydantic schemas
├── Dockerfile
└── tests/
```

### Consumer logic (schema-routed)

```
Poll Kafka topic "audit.events"
  │
  ├─ Deserialize JSON message
  ├─ Extract customer_guid
  ├─ Resolve target schema: f"audit_{customer_guid}"
  ├─ Validate schema exists (cache lookup; skip event if not provisioned)
  ├─ Validate required fields (event_type, actor_id)
  ├─ Apply sensitive data masking (before_state, after_state, metadata)
  ├─ INSERT INTO audit_{customer_guid}.audit_events ...
  │   ON DUPLICATE KEY UPDATE event_id = event_id
  └─ Commit offset
```

**Schema cache**: The consumer maintains an in-memory set of known schemas, refreshed every 60 seconds from `INFORMATION_SCHEMA.SCHEMATA`. If a `customer_guid` maps to an unknown schema, the event is logged as a warning and skipped (customer not yet onboarded, or offboarded).

### Masking rules (applied by consumer, not producers)

| Field path | Rule |
|---|---|
| `before_state` / `after_state` containing `json_data` with `auths` | Replace entire auth blob with `"[REDACTED]"` |
| Any key matching `password`, `secret`, `token`, `key`, `credential` | Replace value with `"[REDACTED]"` |
| `metadata.mongo_uri` | Mask password portion of connection string |
| Any string value > 1KB | Truncate to 1KB + `"...[truncated]"` |

---

## 9. Query API

### Endpoint: `GET /api/v1/audit` (served by audit service :8002)

**Required**: `customer_guid` (query param)

**Optional filters**:

| Param | Type | Example |
|---|---|---|
| `event_type` | string | `integration.run.failed` |
| `resource_type` | string | `data` |
| `resource_id` | string | `42` |
| `actor_id` | string | `service:airflow-scheduler` |
| `action` | string | `write` |
| `outcome` | string | `failure` |
| `trace_id` | string | `a1b2c3d4...` (correlate across services) |
| `from_ts` | ISO datetime | `2026-03-01T00:00:00Z` |
| `to_ts` | ISO datetime | `2026-03-22T23:59:59Z` |
| `cursor` | string (event_id) | UUIDv7 for cursor-based pagination |
| `limit` | int (1-500) | `100` |

**Response**:
```json
{
  "events": [
    {
      "event_id": "01912a4b-...",
      "timestamp": "2026-03-22T02:00:01.234567Z",
      "event_type": "data.destination.written",
      "actor_id": "service:airflow-scheduler",
      "actor_type": "service",
      "customer_guid": "cust-abc-123",
      "resource_type": "data",
      "resource_id": "acme_db.events",
      "action": "write",
      "outcome": "success",
      "before_state": null,
      "after_state": null,
      "trace_id": "a1b2c3d4e5f6...",
      "request_id": "s3_to_mongo_ondemand__manual__2026-03-22T02:00:00+00:00",
      "metadata": {
        "workspace_id": "ws-456",
        "s3_bucket": "acme-data",
        "files_processed": 12,
        "records_written": 1847
      }
    }
  ],
  "next_cursor": "01912a4c-...",
  "has_more": true
}
```

**Schema routing**: The `customer_guid` param determines which MySQL schema to query (`audit_{customer_guid}`). No row-level filtering needed.

**Trace correlation query**: `GET /api/v1/audit?customer_guid=cust-abc-123&trace_id=a1b2c3d4...` returns the complete lifecycle of a single operation across all three services — from API trigger or scheduler dispatch through credential access, data transfer, and run completion.

---

## 10. Retention & Storage

### Tiered approach (GDPR + SOC 2 aligned)

| Tier | Duration | Storage | Purpose |
|---|---|---|---|
| **Hot** | 0–90 days | MySQL per-customer schema (current partitions) | Active queries, dashboards |
| **Warm** | 90 days – 2 years | MySQL older partitions (or exported per-customer to S3 as Parquet) | SOC 2 auditor requests, incident investigation |

### Partition strategy

Each per-customer `audit_events` table starts with a single `p_current` catch-all partition (cloned from template). A monthly cron job managed by the audit service reorganizes partitions:

```sql
-- Run monthly by audit service for each customer schema
ALTER TABLE audit_{customer_guid}.audit_events REORGANIZE PARTITION p_current INTO (
    PARTITION p202603 VALUES LESS THAN (UNIX_TIMESTAMP('2026-04-01')),
    PARTITION p_current VALUES LESS THAN MAXVALUE
);
```

With ~100 customers, this is ~100 ALTER statements per month — trivial.

### Kafka topic retention

7 days on `audit.events`. Safety net if the audit consumer is down. Under normal operation, events are consumed within seconds.

### GDPR data lifecycle (simplified by schema-per-customer)

| Scenario | Action |
|---|---|
| **Customer offboarding / right-to-erasure** | `DROP SCHEMA audit_{customer_guid}` — complete erasure in one DDL statement. No scanning millions of rows. |
| **PII anonymization (active customers)** | After 2 years: replace `actor_id` with SHA-256 hash in old partitions. Per-customer job, isolated from other customers. |
| **Auditor data export** | `mysqldump audit_{customer_guid}` — gives exactly one customer's data. No filtering needed. |
| **Per-customer retention override** | Customer A wants 1 year, customer B wants 3 years? Different partition cleanup schedules per schema. |

Security-relevant events (auth changes, deletions) are exempt from anonymization per GDPR Article 17(3)(e).

---

## 11. Immutability Guarantees

| Layer | Mechanism |
|---|---|
| **Producers** | `emit()` is fire-and-forget. No ability to update or delete events. |
| **Kafka** | Append-only log by design. No message deletion (within retention). |
| **Audit Service** | Only exposes `consume()` (write) and `query()` (read). No update/delete methods. |
| **Database** | Audit service account (`audit_svc`) granted `INSERT, SELECT` only per schema. No `UPDATE` or `DELETE`. Schema-level grants — each customer schema has its own grant. |
| **Schema** | No `updated_at` column. No mutable fields. |

---

## 12. Resolving `customer_guid` in Airflow

The DAG run conf currently carries `tenant_id` (which is `workspace_id`). To produce audit events, operators need `customer_guid`.

**Change**: In `dispatch_operators._find_due_integrations()`, join `integrations` → `workspaces` to include `customer_guid` in the query result. Pass it through conf as `customer_guid`.

```python
# dispatch_operators.py — _find_due_integrations()
# Current:  SELECT ... FROM integrations WHERE utc_next_run <= :now
# Changed:  SELECT i.*, w.customer_guid
#           FROM integrations i
#           JOIN workspaces w ON i.workspace_id = w.workspace_id
#           WHERE i.utc_next_run <= :now
```

For the control plane and kafka consumer, the same join resolves `customer_guid` from the integration's `workspace_id`.

---

## 13. Implementation Plan

### New files

| File | Purpose |
|---|---|
| `packages/shared_utils/shared_utils/audit_producer.py` | `AuditProducer` class — ThreadedStrategy Kafka producer |
| `airflow/plugins/listeners/audit_listener.py` | Airflow Listener plugin — hooks into task/DAG lifecycle |
| `airflow/plugins/audit_plugin.py` | Plugin registration with Airflow's plugin manager |
| `audit_service/app/main.py` | FastAPI app entry point |
| `audit_service/app/core/config.py` | Settings (Kafka, MySQL, masking) |
| `audit_service/app/core/database.py` | Async MySQL connection (schema-routed) |
| `audit_service/app/services/audit_consumer.py` | Kafka consumer → mask → schema-routed MySQL write |
| `audit_service/app/services/audit_query.py` | Query logic (schema-routed reads) |
| `audit_service/app/services/schema_manager.py` | `AuditSchemaManager` — template provisioning, migration, deprovisioning |
| `audit_service/app/api/audit.py` | `GET /api/v1/audit` |
| `audit_service/app/api/admin.py` | `POST /api/v1/audit/provision`, `DELETE /api/v1/audit/deprovision` (internal) |
| `audit_service/app/models/audit_event.py` | ORM model |
| `audit_service/app/schemas/audit.py` | Pydantic schemas |
| `docker/Dockerfile.audit-service` | Container image |

### Modified files

| File | Change |
|---|---|
| `packages/shared_models/shared_models/tables.py` | Add `audit_events` table definition (used by template) |
| `airflow/plugins/operators/dispatch_operators.py` | Join `workspaces` for `customer_guid` in query; add `customer_guid` to conf |
| `control_plane/app/services/integration_service.py` | Add `AuditProducer.emit()` on CRUD + trigger |
| `control_plane/app/services/customer_service.py` | Call audit service provision endpoint on customer onboarding |
| `control_plane/app/core/middleware.py` | Add `AuditContextMiddleware` to capture actor/IP/trace_id |
| `kafka_consumer/app/services/kafka_consumer_service.py` | Add `AuditProducer.emit()` after DAG trigger |
| `docker-compose.yml` | Add audit-service container, `audit.events` topic creation |
| `docker/sql/init.sql` | Add `audit_template` schema + table DDL + `audit_svc` user |

**NOT modified**: `airflow/plugins/operators/s3_to_mongo_operators.py` — zero operator changes.

### Implementation order

1. **Schema template & shared library** — `audit_template` DDL in `init.sql`, `AuditProducer` (ThreadedStrategy) in `shared_utils`
2. **Schema manager** — `AuditSchemaManager` with provision/deprovision/migrate_all
3. **Airflow listener plugin** — `audit_listener.py` + `audit_plugin.py` + `customer_guid` join in dispatch_operators
4. **Audit service skeleton** — FastAPI app with schema-routed Kafka consumer, masking, MySQL writer
5. **Control plane producers** — `AuditContextMiddleware` + emit calls in `IntegrationService` + provision call in onboarding
6. **Kafka consumer producer** — emit calls in `KafkaConsumerService`
7. **Query API** — `GET /api/v1/audit` with schema routing + filters + cursor pagination
8. **Docker & infra** — Dockerfile, docker-compose, SQL init, topic creation
9. **Tests** — Unit tests for masking + schema manager, integration tests for listener + end-to-end flow

---

## 14. Open Questions

| # | Question | Default if unanswered |
|---|---|---|
| 1 | Should `audit.queried` events be emitted for every audit API call? | Yes (consistent) |
| 2 | Kafka topic partition count? | 6 (can grow later) |
| 3 | Exact retention period before PII anonymization? | 2 years |
| 4 | Same MySQL instance for audit schemas, or a separate instance? | Same instance (simpler for v1; schemas provide isolation) |
| 5 | When auth is enabled, how is `customer_guid` derived from JWT? | `customer_guid` claim in JWT payload |
| 6 | Should schema provisioning be called synchronously during customer onboarding, or async via an event? | Synchronous — it's a fast DDL operation, and the customer shouldn't receive data before their audit schema exists |
| 7 | Should `migrate_all()` be a CLI command, an API endpoint, or both? | Both — CLI for ops, API for CI/CD pipelines |
