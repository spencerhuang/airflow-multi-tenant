"""Redis-based message deduplication for Kafka consumer.

Uses atomic SET NX EX to check-and-mark messages as processed in a single
Redis round-trip. Fail-open: if Redis is unavailable, processing proceeds
without dedup (duplicates are preferable to blocked processing).
"""

import logging

logger = logging.getLogger(__name__)


class MessageDeduplicator:
    """Redis-based message deduplication using SET NX with TTL."""

    KEY_PREFIX = "dedup:kafka:"

    def __init__(self, ttl_seconds: int = 86400):
        self.ttl_seconds = ttl_seconds
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

    def is_duplicate(self, dedup_key: str) -> bool:
        """Check if message was already processed via atomic SET NX EX.

        Returns True if duplicate, False if new. Returns False on Redis
        failure (fail-open).
        """
        client = self._get_client()
        if client is None:
            return False
        try:
            result = client.set(dedup_key, "1", nx=True, ex=self.ttl_seconds)
            return result is None  # None means key already existed
        except Exception:
            logger.warning("Redis dedup check failed, proceeding without dedup", exc_info=True)
            return False

    def remove_dedup_key(self, dedup_key: str) -> None:
        """Remove dedup key on processing failure to allow retry."""
        client = self._get_client()
        if client is None:
            return
        try:
            client.delete(dedup_key)
        except Exception:
            logger.warning("Failed to remove dedup key %s", dedup_key, exc_info=True)
