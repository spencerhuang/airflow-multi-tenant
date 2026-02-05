"""
Middleware for structured error logging.
Captures all HTTP requests/responses and logs errors with full context.
"""
import logging
import time
import traceback
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

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
