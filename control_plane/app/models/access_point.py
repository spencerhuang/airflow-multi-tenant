"""AccessPoint model representing a data source or destination."""

from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base
from shared_models.tables import access_points as access_points_table, integrations as integrations_table


class AccessPoint(Base):
    """
    AccessPoint model representing a data source or destination endpoint.

    Examples: S3 bucket, Azure container, MongoDB database, MySQL database

    Attributes:
        access_pt_id: Unique access point identifier (Primary Key)
        ap_type: Type of access point (e.g., 'S3', 'Azure', 'MongoDB', 'MySQL', 'Snowflake', 'GBQ')
        source_integrations: Integrations using this as source
        dest_integrations: Integrations using this as destination
    """

    __table__ = access_points_table

    # Relationships
    source_integrations = relationship(
        "Integration",
        foreign_keys=[integrations_table.c.source_access_pt_id],
        back_populates="source_access_point",
    )
    dest_integrations = relationship(
        "Integration",
        foreign_keys=[integrations_table.c.dest_access_pt_id],
        back_populates="dest_access_point",
    )
