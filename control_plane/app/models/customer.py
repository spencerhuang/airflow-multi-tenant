"""Customer model representing a customer entity."""

from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base
from shared_models.tables import customers as customers_table


class Customer(Base):
    """
    Customer model representing a customer/organization.

    Attributes:
        customer_guid: Unique customer identifier (Primary Key)
        name: Customer name
        max_integration: Maximum number of integrations allowed for this customer
        workspaces: One-to-many relationship with Workspace
    """

    __table__ = customers_table

    # Relationships
    workspaces = relationship("Workspace", back_populates="customer", cascade="all, delete-orphan")
