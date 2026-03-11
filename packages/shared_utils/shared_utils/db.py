"""Shared database utilities for Airflow tasks connecting to the control plane DB.

Provides a pre-configured SQLAlchemy engine factory with connect_timeout,
matching the control plane's DB_TIMEOUT default (5 seconds).
"""

import os
import logging
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Default connect timeout in seconds — matches control_plane.app.core.retry.DB_TIMEOUT
DEFAULT_DB_CONNECT_TIMEOUT = 5


def create_control_plane_engine(
    db_url: Optional[str] = None,
    connect_timeout: int = DEFAULT_DB_CONNECT_TIMEOUT,
) -> Engine:
    """Create a SQLAlchemy engine for the control plane DB with connect_timeout.

    Args:
        db_url: Database URL. If None, reads from CONTROL_PLANE_DB_URL env var.
        connect_timeout: Connection timeout in seconds (default: 5, matching
            the control plane's DB_TIMEOUT).

    Returns:
        A SQLAlchemy Engine instance.

    Raises:
        ValueError: If no db_url is provided and CONTROL_PLANE_DB_URL is not set.
    """
    url = db_url or os.environ.get("CONTROL_PLANE_DB_URL")
    if not url:
        raise ValueError(
            "No db_url provided and CONTROL_PLANE_DB_URL env var is not set"
        )

    return create_engine(
        url,
        connect_args={"connect_timeout": connect_timeout},
    )
