"""Shared utilities for building DAG run configuration and triggering Airflow DAGs.

Centralises the "build conf → resolve auth → determine DAG ID → trigger" pipeline
that was previously duplicated across:
  - control_plane/app/services/integration_service.py
  - kafka_consumer/app/services/kafka_consumer_service.py
  - airflow/plugins/operators/dispatch_operators.py
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from sqlalchemy import select

from shared_models.tables import auths as auths_table
from shared_utils.airflow_auth import get_airflow_auth_headers

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Build base conf
# ---------------------------------------------------------------------------

def build_integration_conf(row) -> Dict[str, Any]:
    """Build the base DAG run conf dict from an integration row.

    Pure function. Works with any object that exposes the required
    attributes via attribute access (SQLAlchemy ORM instances *and*
    Core Row objects both satisfy this).

    Args:
        row: An integration record with attributes: workspace_id,
             integration_id, integration_type, auth_id,
             source_access_pt_id, dest_access_pt_id.

    Returns:
        Dict with the 6 canonical conf keys.
    """
    return {
        "tenant_id": row.workspace_id,
        "integration_id": row.integration_id,
        "integration_type": row.integration_type,
        "auth_id": row.auth_id,
        "source_access_pt_id": row.source_access_pt_id,
        "dest_access_pt_id": row.dest_access_pt_id,
    }


# ---------------------------------------------------------------------------
# 2. Merge JSON data
# ---------------------------------------------------------------------------

def merge_json_data(
    conf: Dict[str, Any],
    json_data_str: Optional[str],
    log: Optional[Any] = None,
) -> Dict[str, Any]:
    """Parse a JSON string and merge it into *conf* in place.

    Safely handles ``None``, empty strings, and malformed JSON.

    Args:
        conf: The configuration dict to update.
        json_data_str: A JSON-encoded string, or ``None``.
        log: Optional logger. Warnings are logged on parse failure;
             if omitted the module-level logger is used.

    Returns:
        The same *conf* dict (mutated in place) for chaining convenience.
    """
    if not json_data_str:
        return conf
    _log = log or logger
    try:
        conf.update(json.loads(json_data_str))
    except json.JSONDecodeError as exc:
        _log.warning(f"Failed to parse json_data: {exc}")
    return conf


# ---------------------------------------------------------------------------
# 3. Resolve auth credentials (sync SQLAlchemy Core)
# ---------------------------------------------------------------------------

def resolve_auth_credentials_sync(
    engine,
    workspace_id: str,
    log: Optional[Any] = None,
) -> Dict[str, Any]:
    """Query auth records for a workspace and return merged credentials.

    Uses **synchronous** SQLAlchemy Core — suitable for Kafka consumer
    and Airflow operator contexts. The async control-plane service
    should keep its own ORM query and use :func:`merge_json_data`
    for the JSON-parsing step.

    Args:
        engine: A SQLAlchemy ``Engine``.
        workspace_id: Workspace whose auth records to resolve.
        log: Optional logger.

    Returns:
        Merged credentials dict from all auth records for the workspace.
    """
    _log = log or logger
    credentials: Dict[str, Any] = {}

    with engine.connect() as conn:
        rows = conn.execute(
            select(auths_table.c.auth_type, auths_table.c.json_data).where(
                auths_table.c.workspace_id == workspace_id
            )
        ).fetchall()

    for row in rows:
        merge_json_data(credentials, row.json_data, log=_log)

    _log.info(f"Resolved {len(rows)} auth record(s) for workspace {workspace_id}")
    return credentials


# ---------------------------------------------------------------------------
# 4. Determine DAG ID
# ---------------------------------------------------------------------------

def determine_dag_id(
    integration_type: str,
    schedule_type: str,
    utc_sch_cron: Optional[str] = None,
) -> str:
    """Determine the Airflow DAG ID for an integration.

    For daily schedules with a valid ``utc_sch_cron``, returns a
    DAG ID like ``s3_to_mongo_daily_02``.  For everything else
    (weekly, monthly, on_demand, cdc) returns ``s3_to_mongo_ondemand``.

    Pure function — no side effects.

    Args:
        integration_type: e.g. ``"s3_to_mongo"`` or ``"S3ToMongo"``.
        schedule_type: e.g. ``"daily"``, ``"weekly"``, ``"on_demand"``.
        utc_sch_cron: UTC cron expression (e.g. ``"0 2 * * *"``).

    Returns:
        DAG ID string.
    """
    workflow_name = integration_type.lower().replace("to", "_to_")

    if schedule_type == "daily" and utc_sch_cron:
        try:
            parts = utc_sch_cron.strip().split()
            if len(parts) >= 2:
                hour = parts[1].zfill(2)
                return f"{workflow_name}_daily_{hour}"
        except (IndexError, ValueError):
            pass

    return f"{workflow_name}_ondemand"


# ---------------------------------------------------------------------------
# 5. Trigger Airflow DAG via REST API
# ---------------------------------------------------------------------------

def trigger_airflow_dag(
    api_url: str,
    username: str,
    password: str,
    dag_id: str,
    conf: Dict[str, Any],
    trigger_source: str = "manual",
    log: Optional[Any] = None,
) -> str:
    """Trigger an Airflow DAG run via the REST API.

    Builds a custom ``dag_run_id`` in the format::

        {tenant_id}_{dag_id}_{trigger_source}_{YYYYmmdd_HHMMSS}

    Args:
        api_url: Airflow API base URL (e.g. ``http://host:8080/api/v2``).
        username: Airflow username for JWT auth.
        password: Airflow password for JWT auth.
        dag_id: The DAG to trigger.
        conf: Configuration dict passed as ``dag_run.conf``.
        trigger_source: Suffix for the run ID — ``"manual"`` for
            control-plane / kafka-consumer triggers, ``"scheduled"``
            for dispatcher triggers.
        log: Optional logger.

    Returns:
        The ``dag_run_id`` from Airflow's response.

    Raises:
        requests.exceptions.RequestException: If the API call fails.
    """
    _log = log or logger

    tenant_id = conf.get("tenant_id", "unknown")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:17]
    custom_run_id = f"{tenant_id}_{dag_id}_{trigger_source}_{timestamp}"

    payload = {
        "dag_run_id": custom_run_id,
        "logical_date": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),
        "conf": conf,
    }

    headers = get_airflow_auth_headers(api_url, username, password)
    url = f"{api_url}/dags/{dag_id}/dagRuns"

    response = requests.post(url, json=payload, headers=headers, timeout=10)
    response.raise_for_status()

    dag_run_id = response.json().get("dag_run_id", custom_run_id)
    _log.info(f"Triggered DAG {dag_id}: dag_run_id={dag_run_id}")
    return dag_run_id
