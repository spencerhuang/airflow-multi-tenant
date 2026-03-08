"""Integration model representing a tenant's data integration configuration."""

from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base
from shared_models.tables import integrations as integrations_table


class Integration(Base):
    """
    Integration model representing a configured data integration for a tenant.

    This is the core model tracked by CDC. Each integration represents one tenant's
    scheduled or on-demand data sync workflow.

    Attributes:
        integration_id: Unique integration identifier (Primary Key)
        workspace_id: Foreign key to Workspace (tenant identifier)
        workflow_id: Foreign key to Workflow (workflow type)
        auth_id: Foreign key to Auth (credentials)
        source_access_pt_id: Foreign key to AccessPoint (source)
        dest_access_pt_id: Foreign key to AccessPoint (destination)
        integration_type: Type of integration (matches workflow_type)
        usr_sch_cron: User's cron schedule in their local timezone
        usr_sch_status: Schedule status ('active', 'paused', 'disabled')
        usr_timezone: User's IANA timezone string (e.g., 'America/New_York')
        utc_next_run: Next scheduled run time in UTC
        utc_sch_cron: Cron schedule normalized to UTC
        schedule_type: Type of schedule ('daily', 'weekly', 'monthly', 'on_demand', 'cdc')
        json_data: Additional configuration as JSON (paths, filters, watermarks, etc.)
        created_at: Timestamp when integration was created
        updated_at: Timestamp when integration was last updated
        workspace: Many-to-one relationship with Workspace
        workflow: Many-to-one relationship with Workflow
        auth: Many-to-one relationship with Auth
        integration_runs: One-to-many relationship with IntegrationRun
    """

    __table__ = integrations_table

    # Relationships
    workspace = relationship("Workspace")
    workflow = relationship("Workflow", back_populates="integrations")
    auth = relationship("Auth", back_populates="integrations")
    source_access_point = relationship(
        "AccessPoint", foreign_keys=[integrations_table.c.source_access_pt_id]
    )
    dest_access_point = relationship(
        "AccessPoint", foreign_keys=[integrations_table.c.dest_access_pt_id]
    )
    integration_runs = relationship("IntegrationRun", back_populates="integration", cascade="all, delete-orphan")
