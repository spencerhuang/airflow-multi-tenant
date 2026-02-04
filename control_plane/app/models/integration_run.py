"""IntegrationRun and IntegrationRunError models for tracking execution history."""

from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Boolean, Text
from sqlalchemy.orm import relationship
from datetime import datetime

from control_plane.app.core.database import Base


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

    __tablename__ = "integration_runs"

    run_id = Column(Integer, primary_key=True, autoincrement=True)
    integration_id = Column(Integer, ForeignKey("integrations.integration_id"), nullable=False, index=True)
    dag_run_id = Column(String(255), index=True)
    started = Column(DateTime, default=datetime.utcnow)
    ended = Column(DateTime)
    is_success = Column(Boolean, default=False)
    execution_date = Column(DateTime)

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

    __tablename__ = "integration_run_errors"

    error_id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("integration_runs.run_id"), nullable=False, index=True)
    error_code = Column(String(100))
    message = Column(Text)
    task_id = Column(String(255))
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    integration_run = relationship("IntegrationRun", back_populates="errors")
