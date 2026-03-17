"""Pydantic schemas for Integration endpoints."""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class IntegrationBase(BaseModel):
    """Base schema for Integration."""

    workspace_id: str = Field(..., description="Workspace identifier")
    workflow_id: int = Field(..., description="Workflow identifier")
    auth_id: int = Field(..., description="Authentication identifier")
    source_access_pt_id: int = Field(..., description="Source access point identifier")
    dest_access_pt_id: int = Field(..., description="Destination access point identifier")
    integration_type: str = Field(..., description="Integration type (e.g., 'S3ToMongo')")
    usr_sch_cron: Optional[str] = Field(None, description="User's cron schedule in local timezone")
    usr_timezone: Optional[str] = Field("UTC", description="User's IANA timezone")
    schedule_type: str = Field("on_demand", description="Schedule type: daily, weekly, monthly, on_demand, cdc")
    json_data: Optional[str] = Field(None, description="Additional configuration as JSON")


class IntegrationCreate(IntegrationBase):
    """Schema for creating a new Integration."""

    pass


class IntegrationUpdate(BaseModel):
    """Schema for updating an existing Integration."""

    usr_sch_cron: Optional[str] = None
    usr_sch_status: Optional[str] = None
    usr_timezone: Optional[str] = None
    schedule_type: Optional[str] = None
    json_data: Optional[str] = None


class IntegrationResponse(IntegrationBase):
    """Schema for Integration response."""

    integration_id: int
    usr_sch_status: str
    utc_next_run: Optional[datetime]
    utc_sch_cron: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        """Pydantic configuration."""

        from_attributes = True


class IntegrationRunCreate(BaseModel):
    """Schema for creating an Integration run record."""

    integration_id: int
    dag_run_id: Optional[str] = None
    execution_date: Optional[datetime] = None


class IntegrationRunResponse(BaseModel):
    """Schema for Integration run response."""

    run_id: int
    integration_id: int
    dag_run_id: Optional[str]
    started: datetime
    ended: Optional[datetime]
    is_success: bool
    execution_date: Optional[datetime]

    class Config:
        """Pydantic configuration."""

        from_attributes = True


class TriggerIntegrationRequest(BaseModel):
    """Schema for triggering an integration manually."""

    integration_id: int
    execution_config: Optional[dict] = Field(None, description="Override configuration for this run")


class TriggerIntegrationResponse(BaseModel):
    """Schema for trigger integration response."""

    integration_id: int
    dag_run_id: str
    message: str
