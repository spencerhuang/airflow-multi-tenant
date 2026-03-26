"""Redis-based message deduplication for Kafka consumer.

Uses a two-phase status protocol to handle the "restart gap" — the window
between processing a message and committing its Kafka offset.

Phase 1 (claim):   SET NX key="P" (processing) with short TTL (claim lease)
Phase 2 (confirm): SET key="C" (completed) with long TTL (24h)

On redelivery after restart:
  - Key missing or expired  → process normally (new or stale lease)
  - Value = "P"             → previous attempt crashed mid-process, re-process
  - Value = "C"             → true duplicate, skip

Fail-open: if Redis is unavailable, processing proceeds without dedup
(duplicates are preferable to blocked processing).
"""

import logging

logger = logging.getLogger(__name__)

STATUS_PROCESSING = "P"
STATUS_COMPLETED = "C"


class MessageDeduplicator:
    """Redis-based message deduplication with two-phase status tracking."""

    KEY_PREFIX = "dedup:kafka:"

    def __init__(self, ttl_seconds: int = 86400, claim_ttl_seconds: int = 420):
        self.ttl_seconds = ttl_seconds  # long TTL for completed keys
        self.claim_ttl_seconds = claim_ttl_seconds  # short lease for processing
        self._redis_client = None

    def _get_client(self):
        """Lazy-init Redis client. Returns None on failure (fail-open)."""
        if self._redis_client is None:
            try:
                from shared_utils.redis_client import get_redis_client
                self._redis_client = get_redis_client()
            except Exception:
                logger.warning("Redis unavailable for dedup, proceeding without dedup")
                return None
        return self._redis_client

    def build_dedup_key(self, topic: str, record, message: dict) -> str:
        """Build a dedup key from message content.

        Key strategy:
        - Debezium CDC: {prefix}{topic}:{integration_id}:{__source_ts_ms}:{__op}
        - Legacy with event_id: {prefix}{topic}:{event_id}
        - Fallback: {prefix}{topic}:{partition}:{offset}
        """
        # Debezium CDC events
        if "__op" in message:
            integration_id = message.get("integration_id")
            source_ts = message.get("__source_ts_ms")
            op = message.get("__op")
            if integration_id and source_ts:
                return f"{self.KEY_PREFIX}{topic}:{integration_id}:{source_ts}:{op}"

        # Legacy events with event_id
        if "event_id" in message and message["event_id"]:
            return f"{self.KEY_PREFIX}{topic}:{message['event_id']}"

        # Fallback: partition:offset
        return f"{self.KEY_PREFIX}{topic}:{record.partition}:{record.offset}"

    def claim(self, dedup_key: str) -> str:
        """Claim a message for processing (phase 1).

        Returns:
            "new"       — key did not exist, claimed with status "P"
            "processing"— key exists with status "P" (previous crash), safe to re-process
            "completed" — key exists with status "C", true duplicate, skip
            "unknown"   — Redis unavailable, proceed without dedup (fail-open)
        """
        client = self._get_client()
        if client is None:
            return "unknown"
        try:
            # Try atomic SET NX with short TTL
            result = client.set(
                dedup_key, STATUS_PROCESSING, nx=True, ex=self.claim_ttl_seconds,
            )
            if result:
                return "new"

            # Key exists — check what status it holds
            value = client.get(dedup_key)
            if value is None:
                # Expired between SET NX and GET — race condition, treat as new
                client.set(dedup_key, STATUS_PROCESSING, ex=self.claim_ttl_seconds)
                return "new"

            decoded = value.decode() if isinstance(value, bytes) else value
            if decoded == STATUS_COMPLETED:
                return "completed"

            # Status is "P" — previous attempt crashed, allow re-processing
            # Reset the lease TTL so this attempt has time to finish
            client.set(dedup_key, STATUS_PROCESSING, ex=self.claim_ttl_seconds)
            return "processing"

        except Exception:
            logger.warning("Redis dedup claim failed, proceeding without dedup", exc_info=True)
            return "unknown"

    def confirm(self, dedup_key: str) -> None:
        """Mark message as completed (phase 2). Overwrites "P" with "C" and long TTL."""
        client = self._get_client()
        if client is None:
            return
        try:
            client.set(dedup_key, STATUS_COMPLETED, ex=self.ttl_seconds)
        except Exception:
            logger.warning("Redis dedup confirm failed for %s", dedup_key, exc_info=True)

    def remove_dedup_key(self, dedup_key: str) -> None:
        """Remove dedup key on permanent failure (DLQ) to keep Redis clean."""
        client = self._get_client()
        if client is None:
            return
        try:
            client.delete(dedup_key)
        except Exception:
            logger.warning("Failed to remove dedup key %s", dedup_key, exc_info=True)
