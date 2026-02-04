"""Workflow model representing a workflow type/template."""

from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base


class Workflow(Base):
    """
    Workflow model representing a workflow type/template.

    Each workflow corresponds to an Airflow DAG template.
    Examples: 'S3ToMongo', 'S3ToMySQL', 'AzureToSnowflake'

    Attributes:
        workflow_id: Unique workflow identifier (Primary Key)
        workflow_type: Type of workflow matching DAG naming (e.g., 'S3ToMongo', 'S3ToMySQL')
        integrations: One-to-many relationship with Integration
    """

    __tablename__ = "workflows"

    workflow_id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_type = Column(String(100), nullable=False, unique=True, index=True)

    # Relationships
    integrations = relationship("Integration", back_populates="workflow")
