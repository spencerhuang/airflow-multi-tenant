"""AccessPoint model representing a data source or destination."""

from control_plane.app.core.database import Base
from shared_models.tables import access_points as access_points_table


class AccessPoint(Base):
    """
    AccessPoint model representing a data source or destination endpoint.

    Examples: S3 bucket, Azure container, MongoDB database, MySQL database

    Attributes:
        access_pt_id: Unique access point identifier (Primary Key)
        ap_type: Type of access point (e.g., 'S3', 'Azure', 'MongoDB', 'MySQL', 'Snowflake', 'GBQ')
    """

    __table__ = access_points_table
