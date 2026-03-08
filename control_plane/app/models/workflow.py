"""Workflow model representing a workflow type/template."""

from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base
from shared_models.tables import workflows as workflows_table


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

    __table__ = workflows_table

    # Relationships
    integrations = relationship("Integration", back_populates="workflow")
