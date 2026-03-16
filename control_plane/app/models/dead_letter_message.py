"""DeadLetterMessage model for DLQ persistence."""

from control_plane.app.core.database import Base
from shared_models.tables import dead_letter_messages as dead_letter_messages_table


class DeadLetterMessage(Base):
    """
    DeadLetterMessage model representing a failed Kafka message stored in the database.

    Uses a soft reference for integration_id (no FK) so DLQ entries survive
    integration deletion.
    """

    __table__ = dead_letter_messages_table
