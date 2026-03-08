"""Shared SQLAlchemy Core table definitions for the control-plane database.

This is the single source of truth for all table schemas. Both the Control Plane
ORM models (SQLAlchemy 2.0) and Airflow operators (SQLAlchemy 1.4) import from
here, eliminating manual duplication.

Control Plane ORM models map to these tables via ``__table__`` and add only
ORM-level relationships.  Airflow operators use the Core tables directly for
synchronous reads/writes via pymysql.
"""

from datetime import datetime

from sqlalchemy import (
    MetaData,
    Table,
    Column,
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
    Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
    Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False),
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
    Column("started", DateTime, default=datetime.utcnow),
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
    Column("timestamp", DateTime, default=datetime.utcnow, nullable=False),
)
