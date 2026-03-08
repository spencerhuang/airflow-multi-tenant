"""Workspace model representing a workspace within a customer."""

from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base
from shared_models.tables import workspaces as workspaces_table


class Workspace(Base):
    """
    Workspace model representing a workspace/environment within a customer.

    Attributes:
        workspace_id: Unique workspace identifier (Primary Key)
        customer_guid: Foreign key to Customer
        customer: Many-to-one relationship with Customer
        auths: One-to-many relationship with Auth
    """

    __table__ = workspaces_table

    # Relationships
    customer = relationship("Customer", back_populates="workspaces")
    auths = relationship("Auth", back_populates="workspace", cascade="all, delete-orphan")
