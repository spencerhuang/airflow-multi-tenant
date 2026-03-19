"""Shared database utilities for Airflow tasks connecting to the control plane DB.

Provides a pre-configured SQLAlchemy engine factory with connect_timeout.
Uses the unified secret provider for credential resolution.
"""

import os
import logging
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from shared_utils.secret_provider import get_infra_secrets

logger = logging.getLogger(__name__)

# Default db timeout for airflow tasks in seconds — same purpose as control_plane.app.core.retry.DB_TIMEOUT
DEFAULT_DB_CONNECT_TIMEOUT = 30


def _build_default_db_url() -> Optional[str]:
    """Build the default control plane DB URL using the secret provider.

    Checks CONTROL_PLANE_DB_URL env var first. If not set, constructs the URL
    from individual env vars and the unified secret provider for the password.
    """
    explicit_url = os.environ.get("CONTROL_PLANE_DB_URL")
    if explicit_url:
        return explicit_url

    # Build from components + secret provider
    user = os.environ.get("MYSQL_USER", "control_plane")
    password = get_infra_secrets().mysql_password
    host = os.environ.get("MYSQL_HOST", "mysql")
    port = os.environ.get("MYSQL_PORT", "3306")
    database = os.environ.get("MYSQL_DATABASE", "control_plane")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def create_control_plane_engine(
    db_url: Optional[str] = None,
    connect_timeout: int = DEFAULT_DB_CONNECT_TIMEOUT,
) -> Engine:
    """Create a SQLAlchemy engine for the control plane DB with connect_timeout.

    Args:
        db_url: Database URL. If None, resolves via CONTROL_PLANE_DB_URL env var
                or builds from individual env vars + secret provider.
        connect_timeout: Connection timeout in seconds (default: 30).

    Returns:
        A SQLAlchemy Engine instance.

    Raises:
        ValueError: If no db_url can be resolved.
    """
    url = db_url or _build_default_db_url()
    if not url:
        raise ValueError(
            "No db_url provided and CONTROL_PLANE_DB_URL env var is not set"
        )

    return create_engine(
        url,
        connect_args={"connect_timeout": connect_timeout},
    )
