"""Apply function for KafkaMessageQueueTrigger (CDC event watcher).

Runs in the Airflow triggerer process (sync, wrapped by sync_to_async).

Lifecycle per message:
  1. Extract traceparent (silent failure — tracing is best-effort).
  2. Parse & validate the CDC payload.
  3. On validation failure:
     a. If retries remain → raise to prevent offset commit.
        The trigger crashes, Airflow restarts it, and the consumer
        re-reads from the last committed offset (message redelivered).
     b. If retries exhausted → push to DLQ and return None
        (offset committed, message removed from topic).
  4. On success → return normalized payload dict.

Return value MUST be JSON-serializable (becomes TriggerEvent payload →
AssetEvent.extra["payload"]).

Retry tracking uses a file-based counter that survives trigger restarts
within the same container.
"""

import json
import logging
import os
import time

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.getenv("CDC_APPLY_MAX_RETRIES", "3"))
_RETRY_STATE_PATH = os.getenv(
    "CDC_APPLY_RETRY_STATE_PATH",
    "/tmp/cdc_apply_retries.json",
)
# Entries older than this are cleaned up (guard against leaks)
_RETRY_TTL_SECONDS = 3600

# Debezium operation → event type
# "r" = snapshot read (initial load), treated same as create
_OP_TO_EVENT_TYPE = {
    "r": "integration.snapshot",
    "c": "integration.created",
    "u": "integration.updated",
    "d": "integration.deleted",
}


class _PoisonPill(Exception):
    """Raised when a message cannot be parsed or validated."""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def cdc_apply_function(message):
    """Process a Kafka CDC message and decide whether to trigger a DAG.

    Args:
        message: confluent_kafka.Message object.

    Returns:
        dict — normalized event payload for the processor DAG.
        None  — tombstones, intentional skips, or DLQ'd messages.

    Raises:
        _PoisonPill — when retries remain (prevents offset commit so the
                      trigger restarts and the message is redelivered).
    """
    # 1. Tracing (silent failure — nice to have)
    traceparent = ""
    try:
        traceparent = _extract_traceparent(message.headers())
    except Exception:
        pass

    # Skip tombstones (Debezium sends None value on hard deletes)
    if message.value() is None:
        return None

    # 2. Parse & validate (critical)
    try:
        payload = _parse_and_validate(message.value())
    except _PoisonPill as exc:
        return _handle_poison_pill(message, exc, traceparent)

    # 3. Skip non-create events (update/delete don't need DAG processing)
    if payload is None:
        return None

    # 4. Successful path
    payload["traceparent"] = traceparent
    return payload


# ---------------------------------------------------------------------------
# Parse / validate
# ---------------------------------------------------------------------------

def _parse_and_validate(raw_value: bytes) -> dict | None:
    """Deserialize and classify a CDC message.

    Returns:
        dict with event_type, data, message_key, integration_id — for
        ``integration.created`` events.
        None — for update/delete events (intentional skip).

    Raises:
        _PoisonPill — for deserialization failures or unrecognized formats.
    """
    try:
        data = json.loads(raw_value)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise _PoisonPill(
            f"Deserialization failed: {e}",
            {"raw_value": raw_value.decode("utf-8", errors="replace")[:1000]},
        ) from e

    if "__op" in data:
        # Real Debezium CDC event
        operation = data.get("__op")
        event_type = _OP_TO_EVENT_TYPE.get(operation)
        integration_id = data.get("integration_id")
        message_key = str(integration_id) if integration_id is not None else "unknown"

        if event_type is None:
            raise _PoisonPill(
                f"Unknown Debezium operation: {operation}",
                data,
            )

    elif "event_type" in data:
        # Legacy manually-published event
        event_type = data["event_type"]
        nested = data.get("data", {})
        integration_id = nested.get("integration_id") or data.get("integration_id")
        message_key = str(integration_id) if integration_id is not None else "unknown"

    else:
        raise _PoisonPill(
            f"Unknown message format: {list(data.keys())}",
            data,
        )

    # Intentional skip: only integration.created needs DAG processing
    if event_type != "integration.created":
        logger.debug("Skipping non-create event: %s", event_type)
        return None

    return {
        "event_type": event_type,
        "data": data,
        "message_key": message_key,
        "integration_id": integration_id,
    }


# ---------------------------------------------------------------------------
# Retry / DLQ
# ---------------------------------------------------------------------------

def _handle_poison_pill(message, exc: _PoisonPill, traceparent: str):
    """Retry-before-DLQ: raise to prevent offset commit until max retries.

    On the Nth retry, push to DLQ and return None (offset committed).
    """
    msg_key = _msg_identity(message)
    attempts = _increment_retry(msg_key)

    error_message = str(exc.args[0]) if exc.args else "Unknown parse error"

    if attempts < MAX_RETRIES:
        logger.warning(
            "Poison pill (attempt %d/%d), raising to prevent offset commit: %s",
            attempts, MAX_RETRIES, error_message,
        )
        # Re-raise so the trigger crashes without committing the offset.
        # On restart the consumer re-polls from last committed offset.
        raise exc

    # Max retries exhausted — DLQ and acknowledge
    logger.error(
        "Max retries (%d) exhausted for %s — routing to DLQ: %s",
        MAX_RETRIES, msg_key, error_message,
    )
    _clear_retry(msg_key)
    _push_to_dlq(
        raw_value=message.value(),
        error_message=error_message,
        traceparent=traceparent,
        message_key=_extract_message_key(message),
    )
    return None


def _push_to_dlq(
    raw_value: bytes,
    error_message: str,
    traceparent: str,
    message_key: str,
) -> None:
    """Best-effort DLQ persistence from the triggerer process."""
    try:
        from shared_utils.dlq_utils import send_to_dlq

        # Build a minimal original_message dict for DLQ storage
        try:
            original_message = json.loads(raw_value)
        except Exception:
            original_message = {"raw_value": raw_value.decode("utf-8", errors="replace")[:1000]}

        send_to_dlq(
            original_message=original_message,
            error_message=error_message,
            source_topic=os.getenv("CDC_SOURCE_TOPIC", "cdc.integration.events"),
            consumer_group="cdc-consumer-airflow",
            message_key=message_key,
            trace_id=traceparent,
            db_url=os.getenv(
                "CONTROL_PLANE_DB_URL",
                "mysql+pymysql://control_plane:control_plane@mysql:3306/control_plane",
            ),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            dlq_topic="cdc.integration.events.dlq",
        )
    except Exception:
        logger.error("Failed to push poison pill to DLQ", exc_info=True)


# ---------------------------------------------------------------------------
# File-based retry tracker (survives trigger restarts within same container)
# ---------------------------------------------------------------------------

def _msg_identity(message) -> str:
    """Stable identity for a Kafka message across redeliveries."""
    return f"{message.topic()}:{message.partition()}:{message.offset()}"


def _extract_message_key(message) -> str:
    """Extract a human-readable key from the message."""
    key = message.key()
    if key is None:
        return "unknown"
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)


def _load_retry_state() -> dict:
    try:
        with open(_RETRY_STATE_PATH) as f:
            state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    # Prune stale entries
    now = time.time()
    return {k: v for k, v in state.items() if now - v.get("ts", 0) < _RETRY_TTL_SECONDS}


def _save_retry_state(state: dict) -> None:
    try:
        with open(_RETRY_STATE_PATH, "w") as f:
            json.dump(state, f)
    except OSError:
        logger.warning("Failed to write retry state to %s", _RETRY_STATE_PATH, exc_info=True)


def _increment_retry(msg_key: str) -> int:
    state = _load_retry_state()
    entry = state.get(msg_key, {"count": 0, "ts": time.time()})
    entry["count"] += 1
    entry["ts"] = time.time()
    state[msg_key] = entry
    _save_retry_state(state)
    return entry["count"]


def _clear_retry(msg_key: str) -> None:
    state = _load_retry_state()
    state.pop(msg_key, None)
    _save_retry_state(state)


# ---------------------------------------------------------------------------
# Tracing
# ---------------------------------------------------------------------------

def _extract_traceparent(headers) -> str:
    """Extract W3C traceparent from Kafka message headers."""
    if not headers:
        return ""
    for key, value in headers:
        if key == "traceparent":
            try:
                return value.decode("utf-8")
            except (UnicodeDecodeError, AttributeError):
                break
    return ""
