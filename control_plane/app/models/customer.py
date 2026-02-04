"""Customer model representing a customer entity."""

from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base


class Customer(Base):
    """
    Customer model representing a customer/organization.

    Attributes:
        customer_guid: Unique customer identifier (Primary Key)
        name: Customer name
        max_integration: Maximum number of integrations allowed for this customer
        workspaces: One-to-many relationship with Workspace
    """

    __tablename__ = "customers"

    customer_guid = Column(String(36), primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    max_integration = Column(Integer, default=100)

    # Relationships
    workspaces = relationship("Workspace", back_populates="customer", cascade="all, delete-orphan")
