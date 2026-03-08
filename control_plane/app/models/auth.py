"""Auth model representing authentication credentials."""

from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base
from shared_models.tables import auths as auths_table


class Auth(Base):
    """
    Auth model representing encrypted authentication credentials.

    This stores credentials for various data sources (S3, Azure, databases, etc.)
    in encrypted format.

    Attributes:
        auth_id: Unique authentication identifier (Primary Key)
        workspace_id: Foreign key to Workspace
        auth_type: Type of authentication (e.g., 'aws_iam', 'azure_service_principal', 'basic')
        json_data: Encrypted JSON blob containing credentials
        workspace: Many-to-one relationship with Workspace
        integrations: One-to-many relationship with Integration
    """

    __table__ = auths_table

    # Relationships
    workspace = relationship("Workspace", back_populates="auths")
    integrations = relationship("Integration", back_populates="auth")
