"""CDC pipeline diagnostics and cleanup endpoints.

Exposes Kafka consumer group offsets alongside Airflow's internal AssetEvent
queue so operators can see whether the two systems have diverged, and provides
a cleanup endpoint to clear stale state that would otherwise trigger unwanted
DAG runs.

Why this matters:
  Airflow's AssetWatcher creates AssetEvents in a postgres metastore table.
  These are *separate* from Kafka consumer offsets.  Resetting Kafka offsets
  does NOT clear pending AssetEvents — they will still trigger
  cdc_integration_processor DAG runs.  This divergence is invisible without
  these diagnostic endpoints.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from control_plane.app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Lazy-init engines
# ---------------------------------------------------------------------------

_airflow_engine: Optional[Engine] = None


def _get_airflow_engine() -> Engine:
    """Sync engine for Airflow's postgres metastore."""
    global _airflow_engine
    if _airflow_engine is None:
        _airflow_engine = create_engine(
            settings.AIRFLOW_METADB_URL,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=1,
        )
    return _airflow_engine


# ---------------------------------------------------------------------------
# Kafka helpers (uses kafka-python, already a control-plane dependency)
# ---------------------------------------------------------------------------

def _get_kafka_offsets(
    bootstrap_servers: str,
    consumer_group: str,
    topic: str,
) -> Dict[str, Any]:
    """Query Kafka for consumer group offsets and topic end offsets."""
    from kafka import KafkaAdminClient, TopicPartition
    from kafka import KafkaConsumer

    result: Dict[str, Any] = {
        "consumer_group": consumer_group,
        "topic": topic,
        "partitions": [],
        "total_lag": 0,
        "error": None,
    }

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="control-plane-diagnostics",
            request_timeout_ms=5000,
        )

        # Get committed offsets for the consumer group
        offsets = admin.list_consumer_group_offsets(consumer_group)
        committed = {}
        for tp, offset_meta in offsets.items():
            if tp.topic == topic:
                committed[tp.partition] = offset_meta.offset

        # Get end offsets (latest) for the topic
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="control-plane-diagnostics-end",
            request_timeout_ms=5000,
        )
        partitions = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(tps)
        consumer.close()

        total_lag = 0
        for tp in tps:
            end = end_offsets.get(tp, 0)
            committed_offset = committed.get(tp.partition)
            lag = (end - committed_offset) if committed_offset is not None else None
            if lag is not None:
                total_lag += lag
            result["partitions"].append({
                "partition": tp.partition,
                "committed_offset": committed_offset,
                "end_offset": end,
                "lag": lag,
            })

        result["total_lag"] = total_lag
        admin.close()

    except Exception as e:
        logger.warning("Failed to query Kafka offsets: %s", e)
        result["error"] = str(e)

    return result


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class CleanupRequest(BaseModel):
    reset_kafka_offsets: bool = False
    clear_asset_events: bool = True


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
def cdc_diagnostics(
    topic: str = Query("cdc.integration.events", description="Kafka topic to inspect"),
    consumer_group: str = Query("cdc-consumer-airflow", description="Kafka consumer group"),
):
    """CDC pipeline diagnostic view.

    Returns Kafka consumer group offsets and Airflow's internal AssetEvent
    queue state side by side, along with a divergence flag when the two
    systems are out of sync.

    Example response::

        {
          "kafka": {
            "consumer_group": "cdc-consumer-airflow",
            "topic": "cdc.integration.events",
            "partitions": [
              {"partition": 0, "committed_offset": 42, "end_offset": 45, "lag": 3}
            ],
            "total_lag": 3
          },
          "airflow_asset_events": {
            "pending_count": 5,
            "queued_dag_runs": 0,
            "events": [...]
          },
          "diverged": true,
          "divergence_detail": "5 AssetEvent(s) pending in Airflow metastore..."
        }
    """
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    if not bootstrap_servers:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # 1. Kafka offsets
    kafka_info = _get_kafka_offsets(bootstrap_servers, consumer_group, topic)

    # 2. Airflow AssetEvents
    airflow_info = _get_asset_event_info()

    # 3. Divergence check
    diverged = False
    divergence_detail = None

    pending_events = airflow_info["pending_count"]
    queued_runs = airflow_info["queued_dag_runs"]
    kafka_lag = kafka_info["total_lag"]

    if pending_events > 0 and kafka_lag == 0:
        diverged = True
        divergence_detail = (
            f"{pending_events} AssetEvent(s) pending in Airflow metastore "
            f"but Kafka consumer is caught up (lag=0). "
            f"These events will trigger processor DAG runs even though "
            f"the Kafka offset has advanced past them."
        )
    elif pending_events > 0 and pending_events > kafka_lag:
        diverged = True
        divergence_detail = (
            f"{pending_events} AssetEvent(s) in Airflow metastore vs "
            f"{kafka_lag} message(s) lag in Kafka. Airflow has more "
            f"pending events than Kafka lag — likely stale events."
        )

    return {
        "kafka": kafka_info,
        "airflow_asset_events": airflow_info,
        "diverged": diverged,
        "divergence_detail": divergence_detail,
    }


@router.post("/cleanup")
def cdc_cleanup(body: CleanupRequest = CleanupRequest()):
    """Clean up stale CDC pipeline state.

    Clears pending AssetEvents from Airflow's metastore and optionally
    resets Kafka consumer group offsets to latest.

    Use this when:
    - Stale AssetEvents from previous test runs or failed processing
      are causing unwanted cdc_integration_processor DAG runs.
    - Kafka consumer group offsets need to be reset to skip old messages.

    Example request::

        POST /api/v1/diagnostics/cdc/cleanup
        {"clear_asset_events": true, "reset_kafka_offsets": true}

    Example response::

        {
          "asset_events_cleared": 6,
          "dag_run_queue_cleared": 0,
          "kafka_offsets_reset": true
        }
    """
    result: Dict[str, Any] = {}

    if body.clear_asset_events:
        cleared = _clear_asset_events()
        result.update(cleared)

    if body.reset_kafka_offsets:
        reset = _reset_kafka_offsets()
        result.update(reset)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_asset_event_info() -> Dict[str, Any]:
    """Query Airflow metastore for pending AssetEvents."""
    info: Dict[str, Any] = {
        "pending_count": 0,
        "queued_dag_runs": 0,
        "events": [],
        "error": None,
    }

    try:
        engine = _get_airflow_engine()
        with engine.connect() as conn:
            # Count pending events
            row = conn.execute(text("SELECT count(*) FROM asset_event")).fetchone()
            info["pending_count"] = row[0] if row else 0

            # Count queued DAG runs from asset events
            row = conn.execute(text("SELECT count(*) FROM asset_dag_run_queue")).fetchone()
            info["queued_dag_runs"] = row[0] if row else 0

            # Get recent events with payload summary
            rows = conn.execute(text(
                "SELECT id, asset_id, extra, timestamp "
                "FROM asset_event ORDER BY id DESC LIMIT 20"
            )).fetchall()

            for r in rows:
                extra = r[2] if r[2] else {}
                if isinstance(extra, str):
                    import json
                    try:
                        extra = json.loads(extra)
                    except Exception:
                        pass

                payload = extra.get("payload", {}) if isinstance(extra, dict) else {}
                info["events"].append({
                    "event_id": r[0],
                    "asset_id": r[1],
                    "integration_id": payload.get("integration_id"),
                    "event_type": payload.get("event_type"),
                    "timestamp": r[3].isoformat() if r[3] else None,
                })

    except Exception as e:
        logger.warning("Failed to query Airflow AssetEvents: %s", e)
        info["error"] = str(e)

    return info


def _clear_asset_events() -> Dict[str, int]:
    """Delete all AssetEvents and related queue entries from Airflow metastore."""
    try:
        engine = _get_airflow_engine()
        with engine.begin() as conn:
            q = conn.execute(text("DELETE FROM asset_dag_run_queue"))
            queue_cleared = q.rowcount

            conn.execute(text("DELETE FROM dagrun_asset_event"))

            e = conn.execute(text("DELETE FROM asset_event"))
            events_cleared = e.rowcount

        logger.info(
            "Cleared %d AssetEvent(s) and %d queued DAG run(s)",
            events_cleared, queue_cleared,
        )
        return {
            "asset_events_cleared": events_cleared,
            "dag_run_queue_cleared": queue_cleared,
        }

    except Exception as e:
        logger.error("Failed to clear AssetEvents: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear AssetEvents: {e}",
        )


def _reset_kafka_offsets() -> Dict[str, Any]:
    """Reset Kafka consumer group offsets to latest.

    Requires the consumer group to be inactive (no active consumers).
    The triggerer's KafkaMessageQueueTrigger must be stopped first,
    e.g. by stopping the airflow-triggerer container.

    Uses a temporary KafkaConsumer that joins the group, seeks to end,
    and commits the new offsets.
    """
    from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    if not bootstrap_servers:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    consumer_group = "cdc-consumer-airflow"
    topic = "cdc.integration.events"

    try:
        # First check if the consumer group is active
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="control-plane-check",
            request_timeout_ms=5000,
        )
        group_desc = admin.describe_consumer_groups([consumer_group])
        admin.close()

        if group_desc and group_desc[0].members:
            return {
                "kafka_offsets_reset": False,
                "kafka_error": (
                    f"Consumer group '{consumer_group}' has "
                    f"{len(group_desc[0].members)} active member(s). "
                    f"Stop the airflow-triggerer before resetting offsets."
                ),
            }

        # Group is inactive — join temporarily and commit end offsets
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group,
            client_id="control-plane-offset-reset",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            session_timeout_ms=10000,
            request_timeout_ms=15000,
        )

        # Poll multiple times to complete group join and rebalance
        for _ in range(5):
            consumer.poll(timeout_ms=2000)

        partitions = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(tps)

        commit_map = {
            tp: OffsetAndMetadata(offset, "", -1)
            for tp, offset in end_offsets.items()
        }
        consumer.commit(commit_map)
        consumer.close()

        offsets_summary = {
            f"partition_{tp.partition}": offset
            for tp, offset in end_offsets.items()
        }

        logger.info("Reset consumer group %s offsets to latest: %s", consumer_group, offsets_summary)
        return {"kafka_offsets_reset": True, "new_offsets": offsets_summary}

    except Exception as e:
        logger.error("Failed to reset Kafka offsets: %s", e, exc_info=True)
        return {"kafka_offsets_reset": False, "kafka_error": str(e)}
