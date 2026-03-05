"""Lightweight SQLAlchemy Core table definitions for the control-plane database.

Airflow workers cannot import control_plane.app.models because the
control_plane package is not mounted into Airflow containers.  These
standalone Core tables mirror the ORM models defined in:

    control_plane/app/models/integration_run.py

All operators that need to read/write the control-plane database should
import from this module instead of duplicating table definitions.
"""

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Text,
)

metadata = MetaData()

integration_runs = Table(
    "integration_runs",
    metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column("integration_id", Integer, nullable=False),
    Column("dag_run_id", String(255)),
    Column("started", DateTime),
    Column("ended", DateTime),
    Column("is_success", Boolean),
    Column("execution_date", DateTime),
)

integration_run_errors = Table(
    "integration_run_errors",
    metadata,
    Column("error_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, nullable=False),
    Column("error_code", String(100)),
    Column("message", Text),
    Column("task_id", String(255)),
    Column("timestamp", DateTime, nullable=False),
)
