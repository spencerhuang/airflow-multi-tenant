"""Shared utility for creating IntegrationRun records in the control plane DB.

Used by Prepare tasks across all workflow types to record that a DAG run
has started for a given integration. CleanUp tasks later update the same
record with end time, success status, and errors.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from shared_models.tables import integration_runs as integration_runs_table
from shared_utils.db import create_control_plane_engine

logger = logging.getLogger(__name__)


def create_integration_run(
    integration_id: Any,
    dag_run_id: str,
    log: Optional[Any] = None,
) -> None:
    """Create an IntegrationRun record in the control plane DB.

    Silently skips if integration_id is None or CONTROL_PLANE_DB_URL is not set.
    Catches and logs DB errors without re-raising, so the caller's pipeline
    is never blocked by a tracking failure.

    Args:
        integration_id: The integration's primary key. Skips if None.
        dag_run_id: The Airflow DAG run ID to associate with this record.
        log: Optional logger (falls back to module-level logger).
    """
    _log = log or logger

    if integration_id is None:
        _log.info("No integration_id; skipping IntegrationRun creation")
        return

    try:
        engine = create_control_plane_engine()
        with engine.begin() as conn:
            conn.execute(
                integration_runs_table.insert().values(
                    integration_id=integration_id,
                    dag_run_id=dag_run_id,
                    execution_date=datetime.now(timezone.utc),
                    started=datetime.now(timezone.utc),
                )
            )
        engine.dispose()
        _log.info(
            f"Created IntegrationRun for integration_id={integration_id}, "
            f"dag_run_id={dag_run_id}"
        )
    except Exception as e:
        _log.error(f"Failed to create IntegrationRun: {e}")
