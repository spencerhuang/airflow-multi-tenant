"""Azure Blob Storage authentication module."""

from typing import Dict
from dataclasses import dataclass


@dataclass
class AzureBlobAuth:
    """
    Azure Blob Storage authentication credentials.

    Attributes:
        account_name: Azure storage account name
        account_key: Azure storage account key (optional, for key-based auth)
        connection_string: Full connection string (optional)
        tenant_id: Azure AD tenant ID (for service principal auth)
        client_id: Azure AD client ID (for service principal auth)
        client_secret: Azure AD client secret (for service principal auth)
    """

    account_name: str
    account_key: str = None
    connection_string: str = None
    tenant_id: str = None
    client_id: str = None
    client_secret: str = None

    @classmethod
    def from_dict(cls, data: Dict) -> "AzureBlobAuth":
        """
        Create AzureBlobAuth instance from dictionary.

        Args:
            data: Dictionary containing authentication credentials

        Returns:
            AzureBlobAuth instance
        """
        return cls(
            account_name=data.get("account_name"),
            account_key=data.get("account_key"),
            connection_string=data.get("connection_string"),
            tenant_id=data.get("tenant_id"),
            client_id=data.get("client_id"),
            client_secret=data.get("client_secret"),
        )
