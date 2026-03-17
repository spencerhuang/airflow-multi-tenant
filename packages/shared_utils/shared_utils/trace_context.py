"""W3C Trace Context for distributed tracing.

Implements the W3C Trace Context specification (https://www.w3.org/TR/trace-context/)
for propagating trace correlation across Debezium, Kafka consumer, and Airflow tasks.

traceparent format: {version}-{trace-id}-{parent-id}-{trace-flags}
Example: 00-4bf92f3577b6a814af67ee0d97c2145d-00f067aa0ba902b7-01
"""

import os
import uuid


class TraceContext:
    """W3C Trace Context container.

    Holds trace_id, span_id, and trace_flags following the W3C traceparent spec.
    Used across Kafka consumer and Airflow tasks for distributed log correlation.
    """

    VERSION = "00"
    TRACE_ID_LENGTH = 32
    SPAN_ID_LENGTH = 16

    def __init__(self, trace_id: str, span_id: str = "", trace_flags: str = "01"):
        """Initialize a TraceContext.

        Args:
            trace_id: 32 lowercase hex characters
            span_id: 16 lowercase hex characters (generated if empty)
            trace_flags: 2 lowercase hex characters (default "01" = sampled)
        """
        self.trace_id = trace_id
        self.span_id = span_id or self._generate_span_id()
        self.trace_flags = trace_flags

    @classmethod
    def new(cls) -> "TraceContext":
        """Create a new trace context with a fresh trace_id and span_id."""
        return cls(
            trace_id=uuid.uuid4().hex,
            span_id=cls._generate_span_id(),
        )

    @classmethod
    def from_traceparent(cls, traceparent: str) -> "TraceContext":
        """Parse a W3C traceparent header string.

        Args:
            traceparent: W3C traceparent header value
                e.g. "00-4bf92f3577b6a814af67ee0d97c2145d-00f067aa0ba902b7-01"

        Returns:
            TraceContext instance

        Raises:
            ValueError: If the traceparent string is malformed
        """
        parts = traceparent.strip().split("-")
        if len(parts) != 4:
            raise ValueError(f"Invalid traceparent format (expected 4 parts): {traceparent}")

        version, trace_id, span_id, trace_flags = parts

        if len(trace_id) != cls.TRACE_ID_LENGTH:
            raise ValueError(f"Invalid trace_id length (expected {cls.TRACE_ID_LENGTH}): {trace_id}")
        if len(span_id) != cls.SPAN_ID_LENGTH:
            raise ValueError(f"Invalid span_id length (expected {cls.SPAN_ID_LENGTH}): {span_id}")

        return cls(trace_id=trace_id, span_id=span_id, trace_flags=trace_flags)

    @classmethod
    def from_kafka_headers(cls, headers) -> "TraceContext":
        """Extract TraceContext from Kafka message headers.

        Looks for the 'traceparent' header added by Debezium's tracing interceptor.
        Falls back to generating a new context if the header is missing or malformed.

        Args:
            headers: Kafka ConsumerRecord.headers (list of (key, value_bytes) tuples, or None)

        Returns:
            TraceContext instance (always returns a valid context)
        """
        if headers:
            for key, value in headers:
                if key == "traceparent":
                    try:
                        return cls.from_traceparent(value.decode("utf-8"))
                    except (ValueError, UnicodeDecodeError, AttributeError):
                        break
        return cls.new()

    @property
    def traceparent(self) -> str:
        """Format as W3C traceparent header value."""
        return f"{self.VERSION}-{self.trace_id}-{self.span_id}-{self.trace_flags}"

    @staticmethod
    def _generate_span_id() -> str:
        """Generate a random 16-char hex span_id."""
        return os.urandom(8).hex()

    def __str__(self) -> str:
        """Return the trace_id for use in log messages."""
        return self.trace_id

    def __repr__(self) -> str:
        return f"TraceContext(traceparent='{self.traceparent}')"
