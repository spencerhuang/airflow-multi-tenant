"""Shared SQLAlchemy Core table definitions for the control-plane database.

This is the single source of truth for all table schemas. Both the Control Plane
ORM models (SQLAlchemy 2.0) and Airflow operators (SQLAlchemy 1.4) import from
here, eliminating manual duplication.

Control Plane ORM models map to these tables via ``__table__`` and add only
ORM-level relationships.  Airflow operators use the Core tables directly for
synchronous reads/writes via pymysql.
"""

from datetime import datetime, timezone


def _utcnow():
    """Return timezone-aware UTC datetime for SQLAlchemy column defaults."""
    return datetime.now(timezone.utc)

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Index,
    Integer,
    String,
    DateTime,
    Boolean,
    Text,
    ForeignKey,
)

metadata = MetaData()

# ── customers ────────────────────────────────────────────────────────────────

customers = Table(
    "customers",
    metadata,
    Column("customer_guid", String(36), primary_key=True, index=True),
    Column("name", String(255), nullable=False),
    Column("max_integration", Integer, default=100),
)

# ── workspaces ───────────────────────────────────────────────────────────────

workspaces = Table(
    "workspaces",
    metadata,
    Column("workspace_id", String(36), primary_key=True, index=True),
    Column(
        "customer_guid",
        String(36),
        ForeignKey("customers.customer_guid"),
        nullable=False,
    ),
)

# ── auths ────────────────────────────────────────────────────────────────────

auths = Table(
    "auths",
    metadata,
    Column("auth_id", Integer, primary_key=True, autoincrement=True),
    Column(
        "workspace_id",
        String(36),
        ForeignKey("workspaces.workspace_id"),
        nullable=False,
    ),
    Column("auth_type", String(50), nullable=False),
    Column("json_data", Text, nullable=False),
)

# ── access_points ────────────────────────────────────────────────────────────

access_points = Table(
    "access_points",
    metadata,
    Column("access_pt_id", Integer, primary_key=True, autoincrement=True),
    Column("ap_type", String(50), nullable=False, index=True),
)

# ── workflows ────────────────────────────────────────────────────────────────

workflows = Table(
    "workflows",
    metadata,
    Column("workflow_id", Integer, primary_key=True, autoincrement=True),
    Column("workflow_type", String(100), nullable=False, unique=True, index=True),
)

# ── integrations ─────────────────────────────────────────────────────────────

integrations = Table(
    "integrations",
    metadata,
    Column("integration_id", Integer, primary_key=True, autoincrement=True),
    Column(
        "workspace_id",
        String(36),
        ForeignKey("workspaces.workspace_id"),
        nullable=False,
        index=True,
    ),
    Column(
        "workflow_id",
        Integer,
        ForeignKey("workflows.workflow_id"),
        nullable=False,
    ),
    Column("auth_id", Integer, ForeignKey("auths.auth_id"), nullable=False),
    Column(
        "source_access_pt_id",
        Integer,
        ForeignKey("access_points.access_pt_id"),
        nullable=False,
    ),
    Column(
        "dest_access_pt_id",
        Integer,
        ForeignKey("access_points.access_pt_id"),
        nullable=False,
    ),
    Column("integration_type", String(100), nullable=False, index=True),
    Column("usr_sch_cron", String(100)),
    Column("usr_sch_status", String(20), default="active"),
    Column("usr_timezone", String(50), default="UTC"),
    Column("utc_next_run", DateTime),
    Column("utc_sch_cron", String(100)),
    Column("schedule_type", String(20), default="on_demand"),
    Column("json_data", Text),
    Column("created_at", DateTime, default=_utcnow, nullable=False),
    Column("updated_at", DateTime, default=_utcnow, onupdate=_utcnow, nullable=False),
)

# ── integration_runs ─────────────────────────────────────────────────────────

integration_runs = Table(
    "integration_runs",
    metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column(
        "integration_id",
        Integer,
        ForeignKey("integrations.integration_id"),
        nullable=False,
        index=True,
    ),
    Column("dag_run_id", String(255), index=True),
    Column("started", DateTime, default=_utcnow),
    Column("ended", DateTime),
    Column("is_success", Boolean, default=False),
    Column("execution_date", DateTime),
)

# ── integration_run_errors ───────────────────────────────────────────────────

integration_run_errors = Table(
    "integration_run_errors",
    metadata,
    Column("error_id", Integer, primary_key=True, autoincrement=True),
    Column(
        "run_id",
        Integer,
        ForeignKey("integration_runs.run_id"),
        nullable=False,
        index=True,
    ),
    Column("error_code", String(100)),
    Column("message", Text),
    Column("task_id", String(255)),
    Column("timestamp", DateTime, default=_utcnow, nullable=False),
)

# ── dead_letter_messages ────────────────────────────────────────────────────

dead_letter_messages = Table(
    "dead_letter_messages",
    metadata,
    Column("dlq_id", Integer, primary_key=True, autoincrement=True),
    Column("integration_id", Integer, nullable=True, index=True),  # soft reference, no FK
    Column("source_topic", String(255), nullable=False),
    Column("consumer_group", String(255), nullable=False),
    Column("message_key", String(255), nullable=True, index=True),
    Column("original_message", Text, nullable=False),
    Column("error_type", String(255), nullable=False),
    Column("error_message", Text, nullable=False),
    Column("retry_count", Integer, nullable=False, default=0),
    Column("status", String(20), nullable=False, default="pending", index=True),
    Column("resolution_notes", Text, nullable=True),
    Column("resolved_at", DateTime, nullable=True),
    Column("created_at", DateTime, default=_utcnow, nullable=False),
    Column("updated_at", DateTime, default=_utcnow, onupdate=_utcnow, nullable=False),
)

# ── audit_events (template DDL — used by audit_template schema) ─────────
# No customer_guid column: the schema IS the tenant boundary.
# This table definition is used by AuditSchemaManager to create per-customer
# schemas via CREATE TABLE LIKE, not auto-created by alembic.

audit_metadata = MetaData()

audit_events = Table(
    "audit_events",
    audit_metadata,
    Column("event_id", String(36), primary_key=True),
    Column("timestamp", DateTime, nullable=False),
    Column("event_type", String(100), nullable=False),
    Column("actor_id", String(255), nullable=False),
    Column("actor_type", String(50), nullable=False),
    Column("actor_ip", String(45), nullable=True),
    Column("resource_type", String(100), nullable=False),
    Column("resource_id", String(255), nullable=False),
    Column("action", String(100), nullable=False),
    Column("outcome", String(20), nullable=False),
    Column("before_state", Text, nullable=True),
    Column("after_state", Text, nullable=True),
    Column("trace_id", String(64), nullable=True),
    Column("request_id", String(36), nullable=True),
    Column("metadata_json", Text, nullable=True),
    Index("idx_audit_event_type", "event_type"),
    Index("idx_audit_timestamp", "timestamp"),
    Index("idx_audit_resource", "resource_type", "resource_id"),
    Index("idx_audit_actor", "actor_id"),
    Index("idx_audit_trace", "trace_id"),
)
