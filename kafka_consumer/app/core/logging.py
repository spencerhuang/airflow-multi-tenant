"""
Structured logging configuration with JSON formatting.
Supports configurable log levels and multiple output formats.
"""
import logging
import sys
from typing import Any
import json
from datetime import datetime, timezone

class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, "event"):
            log_data["event"] = record.event
        if hasattr(record, "status_code"):
            log_data["status_code"] = record.status_code
        if hasattr(record, "method"):
            log_data["method"] = record.method
        if hasattr(record, "path"):
            log_data["path"] = record.path
        if hasattr(record, "user_id"):
            log_data["user_id"] = record.user_id
        if hasattr(record, "username"):
            log_data["username"] = record.username
        if hasattr(record, "duration_ms"):
            log_data["duration_ms"] = record.duration_ms
        if hasattr(record, "error_type"):
            log_data["error_type"] = record.error_type
        if hasattr(record, "error_detail"):
            log_data["error_detail"] = record.error_detail
        if hasattr(record, "request_body"):
            log_data["request_body"] = record.request_body

        # Add stack trace for errors
        if record.exc_info:
            log_data["stack_trace"] = self.formatException(record.exc_info).split("\n")

        return json.dumps(log_data)


def setup_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    """
    Configure application-wide logging.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_format: Output format ('json' or 'text')
    """
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))

    # Set formatter
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Reduce noise from third-party libraries but ensure they use our handler/formatter
    for logger_name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        logger = logging.getLogger(logger_name)
        logger.handlers = []  # Clear default uvicorn handlers
        logger.addHandler(console_handler)  # Use our JSON-enabled handler
        logger.propagate = False  # Prevent double logging

    # Set levels
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
