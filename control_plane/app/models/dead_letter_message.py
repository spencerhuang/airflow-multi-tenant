"""DeadLetterMessage model for DLQ entry tracking."""

from control_plane.app.core.database import Base
from shared_models.tables import dead_letter_messages as dead_letter_messages_table


class DeadLetterMessage(Base):
    """
    DeadLetterMessage model representing a failed message routed to the DLQ.

    Attributes:
        dlq_id: Unique DLQ entry identifier (Primary Key)
        integration_id: Soft reference to Integration (nullable)
        source_topic: Kafka topic the message originated from
        consumer_group: Consumer group that was processing the message
        message_key: Correlation key (typically integration_id as str)
        original_message: JSON-serialized original Kafka message payload
        error_type: Classification of the error
        error_message: Human-readable error description
        retry_count: Number of retry attempts before DLQ
        status: Current status (pending, retrying, resolved, expired)
        resolution_notes: Notes on how the entry was resolved
        resolved_at: Timestamp when the entry was resolved
        created_at: Timestamp when the entry was created
        updated_at: Timestamp when the entry was last updated
    """

    __table__ = dead_letter_messages_table
