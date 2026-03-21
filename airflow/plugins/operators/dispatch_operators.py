"""Dispatcher operators for scheduled DAG execution.

Scheduled DAGs (e.g., daily_02) cannot use dag_run.conf because
scheduler-triggered runs have conf={}. Instead, they use a dispatcher
pattern: query the control plane DB for due integrations, build the
conf for each, and trigger the ondemand DAG via REST API.

This keeps each integration isolated in its own DAG run with proper
IntegrationRun tracking.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

from airflow.sdk import BaseOperator
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
    trigger_airflow_dag,
)


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
                    self._advance_next_run(engine, row)
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
        """Build DAG run conf using shared utilities."""
        conf = build_integration_conf(row)
        conf["traceparent"] = TraceContext.new().traceparent

        merge_json_data(conf, row.json_data, log=self.log)

        auth_creds = resolve_auth_credentials_sync(engine, row.workspace_id, log=self.log)
        conf.update(auth_creds)

        return conf

    def _advance_next_run(self, engine, row) -> None:
        """Advance utc_next_run to the next cron occurrence after now.

        Uses croniter to compute the exact next UTC time from utc_sch_cron.
        Failure is logged but does not break the dispatch — the integration
        was already triggered successfully.
        """
        if not row.utc_sch_cron:
            return
        try:
            now_utc = datetime.now(timezone.utc)
            next_run = croniter(row.utc_sch_cron, now_utc).get_next(datetime)
            t = integrations_table
            with engine.connect() as conn:
                conn.execute(
                    update(t)
                    .where(t.c.integration_id == row.integration_id)
                    .values(utc_next_run=next_run)
                )
                conn.commit()
            self.log.info(
                f"Advanced utc_next_run for integration {row.integration_id} → {next_run.isoformat()}"
            )
        except Exception as e:
            self.log.warning(
                f"Failed to advance utc_next_run for integration {row.integration_id}: {e}"
            )

    def _trigger_ondemand_dag(self, conf: Dict[str, Any], config) -> str:
        """Trigger the ondemand DAG using shared utility."""
        dag_id = f"{self.integration_type}_ondemand"
        return trigger_airflow_dag(
            config.airflow_internal_api_url,
            config.airflow_username,
            config.airflow_password,
            dag_id,
            conf,
            trigger_source="scheduled",
            log=self.log,
        )

