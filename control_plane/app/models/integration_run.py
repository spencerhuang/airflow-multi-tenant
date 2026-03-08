"""IntegrationRun and IntegrationRunError models for tracking execution history."""

from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base
from shared_models.tables import (
    integration_runs as integration_runs_table,
    integration_run_errors as integration_run_errors_table,
)


class IntegrationRun(Base):
    """
    IntegrationRun model representing a single execution of an integration.

    This table logs all DAG runs and their status, WITHOUT writing back
    to CDC-tracked columns to avoid CDC loops.

    Attributes:
        run_id: Unique run identifier (Primary Key)
        integration_id: Foreign key to Integration
        dag_run_id: Airflow DAG run ID
        started: Run start timestamp
        ended: Run end timestamp
        is_success: Whether the run completed successfully
        execution_date: Airflow execution date
        integration: Many-to-one relationship with Integration
        errors: One-to-many relationship with IntegrationRunError
    """

    __table__ = integration_runs_table

    # Relationships
    integration = relationship("Integration", back_populates="integration_runs")
    errors = relationship("IntegrationRunError", back_populates="integration_run", cascade="all, delete-orphan")


class IntegrationRunError(Base):
    """
    IntegrationRunError model representing errors that occurred during a run.

    This table receives all errors from Airflow DAG runs for monitoring
    and debugging purposes.

    Attributes:
        error_id: Unique error identifier (Primary Key)
        run_id: Foreign key to IntegrationRun
        error_code: Error code or type
        message: Error message
        task_id: Airflow task ID that failed
        timestamp: When the error occurred
        integration_run: Many-to-one relationship with IntegrationRun
    """

    __table__ = integration_run_errors_table

    # Relationships
    integration_run = relationship("IntegrationRun", back_populates="errors")
