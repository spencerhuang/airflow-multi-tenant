"""Pydantic models for audit API request/response."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AuditEventResponse(BaseModel):
    event_id: str
    timestamp: str
    event_type: str
    actor_id: str
    actor_type: str
    actor_ip: Optional[str] = None
    resource_type: str
    resource_id: str
    action: str
    outcome: str
    before_state: Optional[str] = None
    after_state: Optional[str] = None
    trace_id: Optional[str] = None
    request_id: Optional[str] = None
    metadata_json: Optional[str] = None


class AuditQueryResponse(BaseModel):
    events: List[AuditEventResponse]
    next_cursor: Optional[str] = None
    has_more: bool = False


class ProvisionRequest(BaseModel):
    customer_guid: str = Field(..., min_length=1)


class ProvisionResponse(BaseModel):
    customer_guid: str
    schema_name: str
    status: str = "provisioned"
