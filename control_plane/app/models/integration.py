"""Integration model representing a tenant's data integration configuration."""

from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Text, Boolean
from sqlalchemy.orm import relationship
from datetime import datetime

from control_plane.app.core.database import Base


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

    __tablename__ = "integrations"

    integration_id = Column(Integer, primary_key=True, autoincrement=True)
    workspace_id = Column(String(36), ForeignKey("workspaces.workspace_id"), nullable=False, index=True)
    workflow_id = Column(Integer, ForeignKey("workflows.workflow_id"), nullable=False)
    auth_id = Column(Integer, ForeignKey("auths.auth_id"), nullable=False)
    source_access_pt_id = Column(Integer, ForeignKey("access_points.access_pt_id"), nullable=False)
    dest_access_pt_id = Column(Integer, ForeignKey("access_points.access_pt_id"), nullable=False)

    integration_type = Column(String(100), nullable=False, index=True)
    usr_sch_cron = Column(String(100))  # User's cron in their timezone
    usr_sch_status = Column(String(20), default="active")  # active, paused, disabled
    usr_timezone = Column(String(50), default="UTC")  # IANA timezone
    utc_next_run = Column(DateTime)  # Next run time in UTC
    utc_sch_cron = Column(String(100))  # Cron normalized to UTC
    schedule_type = Column(String(20), default="on_demand")  # daily, weekly, monthly, on_demand, cdc
    json_data = Column(Text)  # Additional config (paths, incremental watermark, etc.)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    workspace = relationship("Workspace")
    workflow = relationship("Workflow", back_populates="integrations")
    auth = relationship("Auth", back_populates="integrations")
    source_access_point = relationship("AccessPoint", foreign_keys=[source_access_pt_id])
    dest_access_point = relationship("AccessPoint", foreign_keys=[dest_access_pt_id])
    integration_runs = relationship("IntegrationRun", back_populates="integration", cascade="all, delete-orphan")
