"""Workspace model representing a workspace within a customer."""

from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base


class Workspace(Base):
    """
    Workspace model representing a workspace/environment within a customer.

    Attributes:
        workspace_id: Unique workspace identifier (Primary Key)
        customer_guid: Foreign key to Customer
        customer: Many-to-one relationship with Customer
        auths: One-to-many relationship with Auth
    """

    __tablename__ = "workspaces"

    workspace_id = Column(String(36), primary_key=True, index=True)
    customer_guid = Column(String(36), ForeignKey("customers.customer_guid"), nullable=False)

    # Relationships
    customer = relationship("Customer", back_populates="workspaces")
    auths = relationship("Auth", back_populates="workspace", cascade="all, delete-orphan")
