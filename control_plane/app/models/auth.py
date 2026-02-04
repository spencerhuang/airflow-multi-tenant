"""Auth model representing authentication credentials."""

from sqlalchemy import Column, String, Integer, ForeignKey, Text
from sqlalchemy.orm import relationship

from control_plane.app.core.database import Base


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

    __tablename__ = "auths"

    auth_id = Column(Integer, primary_key=True, autoincrement=True)
    workspace_id = Column(String(36), ForeignKey("workspaces.workspace_id"), nullable=False)
    auth_type = Column(String(50), nullable=False)
    json_data = Column(Text, nullable=False)  # Encrypted JSON

    # Relationships
    workspace = relationship("Workspace", back_populates="auths")
    integrations = relationship("Integration", back_populates="auth")
