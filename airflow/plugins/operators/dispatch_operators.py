"""Dispatcher operators for scheduled DAG execution.

Scheduled DAGs (e.g., daily_02) cannot use dag_run.conf because
scheduler-triggered runs have conf={}. Instead, they use a dispatcher
pattern: query the control plane DB for due integrations, build the
conf for each, and trigger the ondemand DAG via REST API.

This keeps each integration isolated in its own DAG run with proper
IntegrationRun tracking.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from airflow.sdk import BaseOperator
from sqlalchemy import select

from config.airflow_config import get_control_plane_config
from shared_models.tables import (
    auths as auths_table,
    integrations as integrations_table,
)
from shared_utils import get_airflow_auth_headers, create_control_plane_engine, TraceContext


class DispatchScheduledIntegrationsTask(BaseOperator):
    """Query control plane DB for due integrations and trigger ondemand DAGs.

    For each matching integration:
    1. Builds the same conf dict as integration_service/kafka_consumer
    2. Creates an IntegrationRun record in the control plane DB
    3. Triggers the ondemand DAG via Airflow REST API

    Args:
        schedule_type: One of "daily", "weekly", or "monthly".
        schedule_hour: UTC hour to match (0-23). Only used for daily
            dispatchers to select integrations by their cron hour.
            Ignored for weekly/monthly (the DAG cron controls timing).
        integration_type: Filter by integration_type (e.g., "s3_to_mongo").
    """

    def __init__(
        self,
        schedule_type: str = "daily",
        schedule_hour: int | None = None,
        integration_type: str = "s3_to_mongo",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.schedule_type = schedule_type
        self.schedule_hour = schedule_hour
        self.integration_type = integration_type

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        dag_run = context.get("dag_run")
        dag_run_id = dag_run.run_id if dag_run else "unknown"
        trace_ctx = TraceContext.new()
        trace_id = trace_ctx.trace_id
        config = get_control_plane_config()
        db_url = config.control_plane_db_url
        if not db_url:
            self.log.warning(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] CONTROL_PLANE_DB_URL not set; nothing to dispatch")
            return {"dispatched": 0, "errors": 0, "results": []}

        engine = create_control_plane_engine(db_url)
        try:
            integrations = self._find_due_integrations(engine)
            self.log.info(
                f"Found {len(integrations)} active integration(s) "
                f"for schedule_type={self.schedule_type}, "
                f"hour={self.schedule_hour}, type={self.integration_type}"
            )

            results: List[Dict[str, Any]] = []
            errors = 0

            for row in integrations:
                try:
                    conf = self._build_conf(engine, row)
                    dag_run_id = self._trigger_ondemand_dag(conf, config)
                    # IntegrationRun record is created by PrepareS3ToMongoTask
                    # inside the triggered ondemand DAG (single source of truth).
                    results.append({
                        "integration_id": row.integration_id,
                        "dag_run_id": dag_run_id,
                        "status": "triggered",
                    })
                    self.log.info(
                        f"Dispatched integration {row.integration_id} → {dag_run_id}"
                    )
                except Exception as e:
                    errors += 1
                    results.append({
                        "integration_id": row.integration_id,
                        "status": "error",
                        "error": str(e),
                    })
                    self.log.error(
                        f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Failed to dispatch integration {row.integration_id}: {e}"
                    )

            summary = {
                "dispatched": len(results) - errors,
                "errors": errors,
                "total": len(integrations),
                "results": results,
            }
            self.log.info(f"Dispatch summary: {summary}")
            return summary
        finally:
            engine.dispose()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_due_integrations(self, engine):
        """Query integrations that are due for this schedule type.

        - daily: filters by schedule_type='daily' AND cron hour = schedule_hour
        - weekly/monthly: filters by schedule_type only (the DAG's own cron
          schedule controls when it runs, so all active integrations of that
          frequency are dispatched)
        """
        t = integrations_table
        with engine.connect() as conn:
            rows = conn.execute(
                select(t).where(
                    t.c.schedule_type == self.schedule_type,
                    t.c.usr_sch_status == "active",
                    t.c.integration_type == self.integration_type,
                )
            ).fetchall()

        if self.schedule_type == "daily" and self.schedule_hour is not None:
            # For daily, filter by hour in Python
            matched = []
            for row in rows:
                hour = self._extract_hour(row.utc_sch_cron)
                if hour == self.schedule_hour:
                    matched.append(row)
            return matched

        # For weekly/monthly, return all active integrations of this type
        return rows

    @staticmethod
    def _extract_hour(cron_expr: str) -> int | None:
        """Extract hour from cron expression like '0 2 * * *'."""
        try:
            parts = cron_expr.strip().split()
            if len(parts) >= 2:
                return int(parts[1])
        except (ValueError, IndexError):
            pass
        return None

    def _build_conf(self, engine, row) -> Dict[str, Any]:
        """Build DAG run conf — same pattern as integration_service/kafka_consumer."""
        conf = {
            "tenant_id": row.workspace_id,
            "integration_id": row.integration_id,
            "integration_type": row.integration_type,
            "auth_id": row.auth_id,
            "source_access_pt_id": row.source_access_pt_id,
            "dest_access_pt_id": row.dest_access_pt_id,
            "traceparent": TraceContext.new().traceparent,
        }

        # Merge integration json_data (non-sensitive workflow config)
        if row.json_data:
            try:
                conf.update(json.loads(row.json_data))
            except json.JSONDecodeError:
                self.log.warning(
                    f"Invalid json_data for integration {row.integration_id}"
                )

        # Resolve credentials from workspace's Auth records
        with engine.connect() as conn:
            auth_rows = conn.execute(
                select(auths_table.c.auth_type, auths_table.c.json_data).where(
                    auths_table.c.workspace_id == row.workspace_id
                )
            ).fetchall()

        for auth_row in auth_rows:
            if auth_row.json_data:
                try:
                    conf.update(json.loads(auth_row.json_data))
                except json.JSONDecodeError:
                    self.log.warning(
                        f"Invalid auth json_data for workspace {row.workspace_id}"
                    )

        self.log.info(
            f"Resolved {len(auth_rows)} auth record(s) for workspace {row.workspace_id}"
        )
        return conf

    def _trigger_ondemand_dag(self, conf: Dict[str, Any], config) -> str:
        """Trigger the ondemand DAG via Airflow REST API."""
        # Build DAG ID: e.g., "s3_to_mongo_ondemand"
        dag_id = f"{self.integration_type}_ondemand"

        api_url = config.airflow_internal_api_url
        airflow_username = config.airflow_username
        airflow_password = config.airflow_password

        tenant_id = conf.get("tenant_id", "unknown")
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:17]
        custom_run_id = f"{tenant_id}_{dag_id}_scheduled_{timestamp}"

        payload = {
            "dag_run_id": custom_run_id,
            "logical_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "conf": conf,
        }

        headers = get_airflow_auth_headers(api_url, airflow_username, airflow_password)
        url = f"{api_url}/dags/{dag_id}/dagRuns"
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("dag_run_id", custom_run_id)

