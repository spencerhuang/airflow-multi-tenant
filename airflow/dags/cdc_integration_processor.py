"""CDC Integration Processor — event-driven DAG triggered by Kafka CDC events.

Scheduled on the ``integration_cdc_events`` Asset defined in
``cdc_event_listener.py``.  Each DAG run corresponds to one Kafka
message (``integration.created``) that passed the apply_function filter.

Processing pipeline:
  1. Read the AssetEvent payload (integration data + traceparent).
  2. Query the control-plane DB for the full integration row.
  3. Build the DAG-run conf, resolve auth credentials.
  4. Trigger the appropriate ondemand DAG (e.g. ``s3_to_mongo_ondemand``).
  5. Record an IntegrationRun and emit an audit event.

DLQ handling:
  If ``process_and_trigger`` exhausts its retries, ``handle_dlq`` fires
  (trigger_rule=ONE_FAILED) and persists the failed message to the
  ``dead_letter_messages`` table + Kafka DLQ topic + audit trail.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.sdk.definitions.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from config.airflow_config import get_dag_config, get_default_args, get_control_plane_config
from cdc_event_listener import integration_cdc_asset

dag_config = get_dag_config()
default_args = get_default_args()

with DAG(
    dag_id="cdc_integration_processor",
    default_args=default_args,
    description="Process CDC integration.created events from Kafka",
    schedule=[integration_cdc_asset],
    start_date=datetime(
        dag_config.start_date_year,
        dag_config.start_date_month,
        dag_config.start_date_day,
        tzinfo=timezone.utc,
    ),
    catchup=False,
    tags=["cdc", "kafka", "event-driven"],
    max_active_runs=dag_config.max_active_runs_ondemand,
    max_active_tasks=dag_config.max_active_tasks,
) as dag:

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )
    def process_and_trigger(**context) -> dict:
        """Read the CDC event payload, build conf, and trigger the ondemand DAG.

        Returns the raw payload dict (pushed to XCom for handle_dlq recovery).
        """
        import json as json_module
        import logging

        from sqlalchemy import select, func

        from shared_models.tables import (
            integrations as integrations_t,
            integration_runs as integration_runs_t,
        )
        from shared_utils import (
            create_control_plane_engine,
            TraceContext,
            build_integration_conf,
            merge_json_data,
            resolve_auth_credentials_sync,
            determine_dag_id,
            trigger_airflow_dag,
            get_audit_producer,
        )

        logger = logging.getLogger("cdc_integration_processor")

        # ── 1. Extract payload from the triggering AssetEvent ──
        # The apply_function return value is stored in AssetEvent.extra.
        # It may be nested under "payload" key or stored directly in extra.
        triggering_events = context.get("triggering_asset_events", {})
        payload = None
        for _asset, events in triggering_events.items():
            if events:
                extra = events[0].extra or {}
                payload = extra.get("payload") or extra
                break

        if not payload or not isinstance(payload, dict) or "event_type" not in payload:
            logger.warning("No valid payload in triggering_asset_events — skipping")
            return {}

        # Poison pills (parse failures, unknown formats) are now DLQ'd
        # directly in cdc_apply_function after retry exhaustion.
        # Only valid integration.created payloads reach this DAG.

        event_type = payload.get("event_type")
        data = payload.get("data", {})
        traceparent_str = payload.get("traceparent", "")
        integration_id = payload.get("integration_id")

        # ── 2. Set up trace context ──
        if traceparent_str:
            try:
                trace_ctx = TraceContext.from_traceparent(traceparent_str)
            except ValueError:
                trace_ctx = TraceContext.new()
        else:
            trace_ctx = TraceContext.new()

        trace_id = trace_ctx.trace_id
        traceparent = trace_ctx.traceparent

        logger.info(
            "Processing CDC event: event_type=%s, integration_id=%s",
            event_type, integration_id,
            extra={"trace_id": trace_id},
        )

        # ── 3. Query integration from control-plane DB ──
        cp_config = get_control_plane_config()
        engine = create_control_plane_engine(cp_config.control_plane_db_url)

        try:
            with engine.connect() as conn:
                row = conn.execute(
                    select(integrations_t).where(
                        integrations_t.c.integration_id == integration_id
                    )
                ).fetchone()

            if not row:
                logger.warning(
                    "Integration %s not found in database — stale CDC event, skipping",
                    integration_id,
                )
                return payload

            # ── 4. Build execution config ──
            execution_config = {}
            is_debezium = data.get("is_debezium_event", "__op" in data)

            if is_debezium and row.json_data:
                try:
                    execution_config = json_module.loads(row.json_data)
                except json_module.JSONDecodeError as e:
                    logger.error(
                        "Failed to parse json_data for integration %s: %s",
                        integration_id, e,
                        extra={"trace_id": trace_id},
                    )
            elif not is_debezium:
                # Legacy events carry config inline
                if data.get("source_config"):
                    execution_config.update(data["source_config"])
                if data.get("destination_config"):
                    execution_config.update(data["destination_config"])

            # ── 5. Determine DAG ID ──
            dag_id = determine_dag_id(
                row.integration_type, row.schedule_type, row.utc_sch_cron
            )

            # ── 6. Build DAG run conf ──
            conf = build_integration_conf(row)
            conf["traceparent"] = traceparent
            merge_json_data(conf, row.json_data, log=logger)

            # ── 7. Resolve auth credentials ──
            auth_creds = resolve_auth_credentials_sync(
                engine, row.workspace_id, log=logger
            )
            conf.update(auth_creds)

            # Override with execution_config (preserve traceparent)
            if execution_config:
                conf.update(execution_config)
            conf["traceparent"] = traceparent

            # ── 8. Trigger the ondemand DAG ──
            dag_run_id = trigger_airflow_dag(
                cp_config.airflow_internal_api_url,
                cp_config.airflow_username,
                cp_config.airflow_password,
                dag_id,
                conf,
                trigger_source="cdc",
                log=logger,
            )

            # ── 9. Record IntegrationRun ──
            with engine.begin() as conn:
                conn.execute(
                    integration_runs_t.insert().values(
                        integration_id=integration_id,
                        dag_run_id=dag_run_id,
                        execution_date=func.now(),
                        started=func.now(),
                    )
                )

            logger.info(
                "Workflow triggered: integration_id=%s, dag_id=%s, dag_run_id=%s",
                integration_id, dag_id, dag_run_id,
                extra={"trace_id": trace_id},
            )

            # ── 10. Emit audit event ──
            try:
                audit = get_audit_producer()
                if audit:
                    audit.emit(
                        customer_guid=conf.get("tenant_id", "unknown"),
                        event_type="integration.cdc_triggered",
                        actor_id="airflow-triggerer",
                        actor_type="system",
                        resource_type="integration",
                        resource_id=str(integration_id),
                        action="trigger",
                        outcome="success",
                        trace_id=trace_id,
                        metadata={
                            "dag_id": dag_id,
                            "dag_run_id": dag_run_id,
                            "trigger_source": "cdc",
                        },
                    )
            except Exception:
                logger.debug("Failed to emit audit event", exc_info=True)

        finally:
            engine.dispose()

        # Return payload so handle_dlq can read it from XCom if needed
        return payload

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def handle_dlq(**context) -> None:
        """Persist failed message to DLQ (database + Kafka topic + audit)."""
        import logging
        import os

        logger = logging.getLogger("cdc_integration_processor.dlq")

        # Try XCom first (set when process_and_trigger succeeds partially),
        # then fall back to the triggering AssetEvent payload directly.
        # When process_and_trigger raises before returning (e.g. poison pill),
        # XCom is empty, so we must read from the asset event.
        ti = context["ti"]
        payload = ti.xcom_pull(task_ids="process_and_trigger")
        if not payload:
            triggering_events = context.get("triggering_asset_events", {})
            for _asset, events in triggering_events.items():
                if events:
                    extra = events[0].extra or {}
                    payload = extra.get("payload") or extra
                    break
        if not payload:
            logger.warning("No payload available for DLQ — cannot persist")
            return

        # Poison pills are DLQ'd in cdc_apply_function (edge).
        # This handle_dlq only fires for processing failures (DB down, API errors, etc.)
        error_message = "process_and_trigger task failed after max retries"

        from shared_utils.dlq_utils import send_to_dlq

        send_to_dlq(
            original_message=payload.get("data", payload),
            error_message=error_message,
            source_topic="cdc.integration.events",
            consumer_group="cdc-consumer-airflow",
            message_key=payload.get("message_key", "unknown"),
            trace_id=payload.get("traceparent", ""),
            db_url=os.getenv(
                "CONTROL_PLANE_DB_URL",
                "mysql+pymysql://control_plane:control_plane@mysql:3306/control_plane",
            ),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            dlq_topic="cdc.integration.events.dlq",
        )

        logger.warning(
            "Message sent to DLQ: message_key=%s",
            payload.get("message_key"),
        )

    # DAG structure
    result = process_and_trigger()
    handle_dlq_task = handle_dlq()

    result >> handle_dlq_task
