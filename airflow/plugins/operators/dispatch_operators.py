"""Dispatch logic for the hourly Controller DAG.

The Controller DAG (s3_to_mongo_controller) runs every hour and uses
Dynamic Task Mapping (DTM) to trigger ondemand DAGs for all due
integrations. This module provides the Phase A logic: query the
control plane DB for integrations whose utc_next_run <= now, build
the conf for each, advance utc_next_run, and return a list of
trigger kwargs suitable for TriggerDagRunOperator.expand_kwargs().
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from croniter import croniter
from sqlalchemy import select, update

from config.airflow_config import get_control_plane_config
from shared_models.tables import integrations as integrations_table
from shared_utils import (
    create_control_plane_engine,
    TraceContext,
    build_integration_conf,
    merge_json_data,
    resolve_auth_credentials_sync,
)

logger = logging.getLogger(__name__)


def find_and_prepare_due_integrations(
    integration_type: str = "s3_to_mongo",
) -> List[Dict[str, Any]]:
    """Find all due integrations and return trigger kwargs for DTM.

    Queries the control plane DB for active integrations of the given
    type whose ``utc_next_run <= now`` (covers daily, weekly, and
    monthly schedules in a single query). For each due integration:

    1. Builds the DAG run conf (same as control plane / kafka consumer).
    2. Generates a ``trigger_run_id`` preserving the existing format.
    3. Advances ``utc_next_run`` to the next cron occurrence.

    Error isolation: if one integration fails (conf build, advance),
    it is logged and skipped — other integrations are still returned.

    Returns:
        A list of dicts, each with ``conf`` and ``trigger_run_id`` keys,
        suitable for ``TriggerDagRunOperator.expand_kwargs()``.
        Returns ``[]`` if no integrations are due or DB is unavailable.
    """
    config = get_control_plane_config()
    db_url = config.control_plane_db_url
    if not db_url:
        logger.warning("CONTROL_PLANE_DB_URL not set; nothing to dispatch")
        return []

    engine = create_control_plane_engine(db_url)
    try:
        now_utc = datetime.now(timezone.utc)
        integrations = _find_due_integrations(engine, integration_type, now_utc)
        logger.info(
            f"Found {len(integrations)} due integration(s) "
            f"for type={integration_type}"
        )

        results: List[Dict[str, Any]] = []
        for row in integrations:
            try:
                conf = _build_conf(engine, row)
                trigger_run_id = _build_trigger_run_id(
                    conf.get("tenant_id", "unknown"),
                    integration_type,
                )
                _advance_next_run(engine, row)
                results.append({
                    "conf": conf,
                    "trigger_run_id": trigger_run_id,
                })
                logger.info(
                    f"Prepared integration {row.integration_id} → {trigger_run_id}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to prepare integration {row.integration_id}: {e}"
                )

        logger.info(
            f"Dispatch summary: {len(results)} prepared, "
            f"{len(integrations) - len(results)} errors"
        )
        return results
    finally:
        engine.dispose()


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _find_due_integrations(engine, integration_type: str, now_utc: datetime):
    """Query integrations that are due now (utc_next_run <= now).

    A single unified query replaces the old per-schedule-type approach.
    Covers daily, weekly, and monthly schedules — any integration whose
    utc_next_run has passed is considered due.
    """
    t = integrations_table
    with engine.connect() as conn:
        return conn.execute(
            select(t).where(
                t.c.usr_sch_status == "active",
                t.c.integration_type == integration_type,
                t.c.schedule_type.in_(["daily", "weekly", "monthly"]),
                t.c.utc_next_run.isnot(None),
                t.c.utc_next_run <= now_utc,
            )
        ).fetchall()


def _build_conf(engine, row) -> Dict[str, Any]:
    """Build DAG run conf using shared utilities."""
    conf = build_integration_conf(row)
    conf["traceparent"] = TraceContext.new().traceparent
    merge_json_data(conf, row.json_data, log=logger)
    auth_creds = resolve_auth_credentials_sync(engine, row.workspace_id, log=logger)
    conf.update(auth_creds)
    return conf


def _build_trigger_run_id(tenant_id: str, integration_type: str) -> str:
    """Generate trigger_run_id preserving the existing dag_run_id format.

    Format: {tenant_id}_{dag_id}_scheduled_{YYYYmmdd_HHMMSS_f}
    """
    dag_id = f"{integration_type}_ondemand"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:17]
    return f"{tenant_id}_{dag_id}_scheduled_{timestamp}"


def _advance_next_run(engine, row) -> None:
    """Advance utc_next_run to the next cron occurrence.

    Backfill policy differs by schedule type:
    - **daily**: Advance from ``now`` — skips missed runs. Daily jobs are
      high-frequency; replaying 7 missed dailies after a week of downtime
      would flood the system with stale data.
    - **weekly / monthly**: Advance from the current ``utc_next_run`` — one
      interval at a time. If the system was down for 3 weeks, the weekly
      integration is dispatched once per controller cycle (hourly) until
      all missed weeks are caught up.

    Uses croniter to compute the exact next UTC time from utc_sch_cron.
    Failure is logged but does not prevent the integration from being
    dispatched — the conf was already built successfully.
    """
    if not row.utc_sch_cron:
        return
    try:
        now_utc = datetime.now(timezone.utc)
        # Daily: jump to next future occurrence (skip missed runs).
        # Weekly/monthly: step forward from current utc_next_run so each
        # missed occurrence is dispatched on the next controller cycle.
        if row.schedule_type in ("weekly", "monthly") and row.utc_next_run:
            base = row.utc_next_run
        else:
            base = now_utc
        next_run = croniter(row.utc_sch_cron, base).get_next(datetime)
        t = integrations_table
        with engine.connect() as conn:
            conn.execute(
                update(t)
                .where(t.c.integration_id == row.integration_id)
                .values(utc_next_run=next_run)
            )
            conn.commit()
        logger.info(
            f"Advanced utc_next_run for integration {row.integration_id} "
            f"({row.schedule_type}, base={'utc_next_run' if base != now_utc else 'now'}) "
            f"→ {next_run.isoformat()}"
        )
    except Exception as e:
        logger.warning(
            f"Failed to advance utc_next_run for integration {row.integration_id}: {e}"
        )
