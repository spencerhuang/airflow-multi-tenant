"""Airflow Listener plugin for audit event emission.

Hooks into DAG run and task instance lifecycle events to emit audit
events via AuditProducer. Runs in the long-lived scheduler/worker
process, so the ThreadedStrategy (daemon thread + queue) works safely.

Every hook is wrapped in try/except — audit failures never impact
task execution.
"""

import logging
import os
from typing import Any, Dict, Optional

from airflow.listeners import hookimpl

logger = logging.getLogger(__name__)

# Lazy singleton — initialised on first use inside the worker process.
_producer = None
_producer_init_attempted = False


def _get_producer():
    """Lazy-init the audit producer singleton."""
    global _producer, _producer_init_attempted
    if _producer_init_attempted:
        return _producer
    _producer_init_attempted = True
    try:
        from shared_utils.audit_producer import get_audit_producer

        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
        _producer = get_audit_producer(bootstrap_servers=bootstrap_servers)
        logger.info("Audit listener: producer initialised")
    except Exception:
        logger.exception("Audit listener: failed to initialise producer")
        _producer = None
    return _producer


def _extract_conf(dag_run) -> Dict[str, Any]:
    """Safely extract conf from a DagRun."""
    try:
        return dict(dag_run.conf or {})
    except Exception:
        return {}


def _trace_id_from(conf: Dict[str, Any]) -> Optional[str]:
    """Extract trace_id from traceparent in conf."""
    traceparent = conf.get("traceparent", "")
    if traceparent and isinstance(traceparent, str):
        parts = traceparent.split("-")
        if len(parts) >= 2:
            return parts[1]
    return None


def _is_ondemand_dag(dag_id: str) -> bool:
    """Check if this is an ondemand integration DAG (not the controller)."""
    return dag_id.endswith("_ondemand")


# ── DAG Run lifecycle hooks ─────────────────────────────────────────────


@hookimpl
def on_dag_run_running(dag_run, msg):
    """Emit integration.run.started when an ondemand DAG starts."""
    try:
        if not _is_ondemand_dag(dag_run.dag_id):
            return
        producer = _get_producer()
        if not producer:
            return

        conf = _extract_conf(dag_run)
        producer.emit(
            customer_guid=conf.get("customer_guid", "unknown"),
            event_type="integration.run.started",
            actor_id="airflow-scheduler",
            actor_type="system",
            resource_type="integration",
            resource_id=str(conf.get("integration_id", "")),
            action="start",
            outcome="success",
            trace_id=_trace_id_from(conf),
            metadata={
                "dag_id": dag_run.dag_id,
                "dag_run_id": dag_run.run_id,
                "tenant_id": conf.get("tenant_id", ""),
            },
        )
    except Exception:
        logger.exception("Audit listener: on_dag_run_running failed")


@hookimpl
def on_dag_run_success(dag_run, msg):
    """Emit integration.run.completed when an ondemand DAG succeeds."""
    try:
        if not _is_ondemand_dag(dag_run.dag_id):
            return
        producer = _get_producer()
        if not producer:
            return

        conf = _extract_conf(dag_run)
        producer.emit(
            customer_guid=conf.get("customer_guid", "unknown"),
            event_type="integration.run.completed",
            actor_id="airflow-scheduler",
            actor_type="system",
            resource_type="integration",
            resource_id=str(conf.get("integration_id", "")),
            action="complete",
            outcome="success",
            trace_id=_trace_id_from(conf),
            metadata={
                "dag_id": dag_run.dag_id,
                "dag_run_id": dag_run.run_id,
            },
        )
    except Exception:
        logger.exception("Audit listener: on_dag_run_success failed")


@hookimpl
def on_dag_run_failed(dag_run, msg):
    """Emit integration.run.failed when an ondemand DAG fails."""
    try:
        if not _is_ondemand_dag(dag_run.dag_id):
            return
        producer = _get_producer()
        if not producer:
            return

        conf = _extract_conf(dag_run)
        producer.emit(
            customer_guid=conf.get("customer_guid", "unknown"),
            event_type="integration.run.failed",
            actor_id="airflow-scheduler",
            actor_type="system",
            resource_type="integration",
            resource_id=str(conf.get("integration_id", "")),
            action="complete",
            outcome="failure",
            trace_id=_trace_id_from(conf),
            metadata={
                "dag_id": dag_run.dag_id,
                "dag_run_id": dag_run.run_id,
                "error": str(msg) if msg else None,
            },
        )
    except Exception:
        logger.exception("Audit listener: on_dag_run_failed failed")


# ── Task Instance lifecycle hooks ───────────────────────────────────────


@hookimpl
def on_task_instance_success(previous_state, task_instance, session):
    """Emit audit events based on which task completed successfully."""
    try:
        producer = _get_producer()
        if not producer:
            return

        ti = task_instance
        dag_run = ti.dag_run
        conf = _extract_conf(dag_run)
        task_id = ti.task_id
        customer_guid = conf.get("customer_guid", "unknown")
        trace_id = _trace_id_from(conf)
        integration_id = str(conf.get("integration_id", ""))

        if task_id == "prepare":
            producer.emit(
                customer_guid=customer_guid,
                event_type="auth.accessed",
                actor_id="airflow-worker",
                actor_type="system",
                resource_type="credentials",
                resource_id=str(conf.get("workspace_id", "")),
                action="access",
                outcome="success",
                trace_id=trace_id,
                metadata={"dag_id": dag_run.dag_id, "task_id": task_id},
            )

        elif task_id == "execute":
            producer.emit(
                customer_guid=customer_guid,
                event_type="data.destination.written",
                actor_id="airflow-worker",
                actor_type="system",
                resource_type="integration",
                resource_id=integration_id,
                action="write",
                outcome="success",
                trace_id=trace_id,
                metadata={"dag_id": dag_run.dag_id, "task_id": task_id},
            )

        elif task_id == "cleanup":
            producer.emit(
                customer_guid=customer_guid,
                event_type="auth.cleared",
                actor_id="airflow-worker",
                actor_type="system",
                resource_type="credentials",
                resource_id=str(conf.get("workspace_id", "")),
                action="clear",
                outcome="success",
                trace_id=trace_id,
                metadata={"dag_id": dag_run.dag_id, "task_id": task_id},
            )

        elif task_id == "find_due_integrations":
            producer.emit(
                customer_guid=customer_guid or "system",
                event_type="integration.dispatched",
                actor_id="airflow-scheduler",
                actor_type="system",
                resource_type="controller",
                resource_id=dag_run.dag_id,
                action="dispatch",
                outcome="success",
                trace_id=trace_id,
                metadata={"dag_id": dag_run.dag_id, "task_id": task_id},
            )

    except Exception:
        logger.exception("Audit listener: on_task_instance_success failed")


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error, session):
    """Emit task failure event."""
    try:
        producer = _get_producer()
        if not producer:
            return

        ti = task_instance
        dag_run = ti.dag_run
        conf = _extract_conf(dag_run)

        producer.emit(
            customer_guid=conf.get("customer_guid", "unknown"),
            event_type=f"task.{ti.task_id}.failed",
            actor_id="airflow-worker",
            actor_type="system",
            resource_type="task",
            resource_id=ti.task_id,
            action="execute",
            outcome="failure",
            trace_id=_trace_id_from(conf),
            metadata={
                "dag_id": dag_run.dag_id,
                "dag_run_id": dag_run.run_id,
                "task_id": ti.task_id,
                "error": str(error)[:500] if error else None,
            },
        )
    except Exception:
        logger.exception("Audit listener: on_task_instance_failed failed")
