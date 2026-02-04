"""AccessPoint model representing a data source or destination."""

from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base


class AccessPoint(Base):
    """
    AccessPoint model representing a data source or destination endpoint.

    Examples: S3 bucket, Azure container, MongoDB database, MySQL database

    Attributes:
        access_pt_id: Unique access point identifier (Primary Key)
        ap_type: Type of access point (e.g., 'S3', 'Azure', 'MongoDB', 'MySQL', 'Snowflake', 'GBQ')
        integrations: Many-to-many relationship with Integration (source or destination)
    """

    __tablename__ = "access_points"

    access_pt_id = Column(Integer, primary_key=True, autoincrement=True)
    ap_type = Column(String(50), nullable=False, index=True)

    # Relationships - AccessPoints are referenced by Integrations
    # No direct relationship defined here as Integration has foreign keys
