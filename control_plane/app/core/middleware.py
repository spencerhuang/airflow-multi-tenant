"""
Middleware for structured error logging and audit context.
Captures all HTTP requests/responses and logs errors with full context.
"""
import logging
import time
import traceback
import uuid
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from shared_utils.trace_context import TraceContext

logger = logging.getLogger(__name__)


class ErrorLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that logs all HTTP errors (4xx and 5xx) with structured context.
    
    Features:
    - Captures request details (method, path, user)
    - Logs response times
    - Includes stack traces for 5xx errors
    - JSON-formatted for log aggregation
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        
        # Extract user info if authenticated
        user_id = None
        username = None
        if hasattr(request.state, "user"):
            user_id = getattr(request.state.user, "id", None)
            username = getattr(request.state.user, "username", None)
        
        try:
            response = await call_next(request)
            
            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000
            
            # Log 4xx errors (client errors)
            if 400 <= response.status_code < 500:
                logger.warning(
                    f"Client error: {response.status_code}",
                    extra={
                        "event": "http_error",
                        "status_code": response.status_code,
                        "method": request.method,
                        "path": str(request.url.path),
                        "user_id": user_id,
                        "username": username,
                        "duration_ms": round(duration_ms, 2),
                    }
                )
            
            return response
            
        except Exception as exc:
            # Calculate duration even for errors
            duration_ms = (time.time() - start_time) * 1000
            
            # Log 5xx errors (server errors) with stack trace
            logger.error(
                f"Internal server error: {type(exc).__name__}: {str(exc)}",
                extra={
                    "event": "internal_error",
                    "status_code": 500,
                    "method": request.method,
                    "path": str(request.url.path),
                    "user_id": user_id,
                    "username": username,
                    "duration_ms": round(duration_ms, 2),
                    "error_type": type(exc).__name__,
                    "error_detail": str(exc),
                },
                exc_info=True  # This includes the stack trace
            )
            
            # Re-raise to let FastAPI handle the response
            raise


class AuditContextMiddleware(BaseHTTPMiddleware):
    """Captures audit context (actor, IP, trace_id, request_id) for each request.

    Stores values in ``request.state`` so downstream services can include
    them in audit events without threading context manually.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Actor
        actor_id = "anonymous"
        actor_type = "user"
        if hasattr(request.state, "user"):
            actor_id = getattr(request.state.user, "username", None) or "anonymous"

        # Client IP (respect X-Forwarded-For for proxied requests)
        forwarded = request.headers.get("x-forwarded-for")
        client_ip = forwarded.split(",")[0].strip() if forwarded else (
            request.client.host if request.client else None
        )

        # Trace context
        trace_ctx = TraceContext.new()
        request_id = str(uuid.uuid4())

        request.state.audit_actor_id = actor_id
        request.state.audit_actor_type = actor_type
        request.state.audit_actor_ip = client_ip
        request.state.audit_trace_id = trace_ctx.trace_id
        request.state.audit_request_id = request_id

        response = await call_next(request)
        return response
