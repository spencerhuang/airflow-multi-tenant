"""API router configuration."""

from fastapi import APIRouter
from control_plane.app.api import integrations, health, dlq, diagnostics
from control_plane.app.api.endpoints import hotspot

api_router = APIRouter()

api_router.include_router(health.router, tags=["health"])
api_router.include_router(integrations.router, prefix="/integrations", tags=["integrations"])
api_router.include_router(hotspot.router, tags=["hotspot"])
api_router.include_router(dlq.router, prefix="/dlq", tags=["dlq"])
api_router.include_router(diagnostics.router, prefix="/diagnostics/cdc", tags=["diagnostics"])
