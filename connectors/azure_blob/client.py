"""Azure Blob Storage client module (simplified implementation)."""

from typing import List
from azure.storage.blob import BlobServiceClient
from connectors.azure_blob.auth import AzureBlobAuth


class AzureBlobClient:
    """
    Azure Blob Storage client for interacting with Azure Blob Storage.

    This is a simplified implementation wrapping Azure SDK.

    Attributes:
        auth: Azure Blob authentication credentials
        client: BlobServiceClient instance
    """

    def __init__(self, auth: AzureBlobAuth):
        """
        Initialize Azure Blob client with authentication credentials.

        Args:
            auth: AzureBlobAuth instance containing credentials
        """
        self.auth = auth
        self.client = self._create_client()

    def _create_client(self) -> BlobServiceClient:
        """
        Create Azure Blob Service client with authentication.

        Returns:
            BlobServiceClient instance
        """
        if self.auth.connection_string:
            return BlobServiceClient.from_connection_string(self.auth.connection_string)
        elif self.auth.account_key:
            return BlobServiceClient(
                account_url=f"https://{self.auth.account_name}.blob.core.windows.net",
                credential=self.auth.account_key,
            )
        else:
            # Service principal auth would go here
            raise NotImplementedError("Service principal auth not yet implemented")

    def list_blobs(self, container: str, prefix: str = "") -> List[str]:
        """
        List blobs in container with optional prefix.

        Args:
            container: Container name
            prefix: Blob name prefix to filter by

        Returns:
            List of blob names
        """
        container_client = self.client.get_container_client(container)
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blob_list]

    def blob_exists(self, container: str, blob_name: str) -> bool:
        """
        Check if a blob exists in container.

        Args:
            container: Container name
            blob_name: Blob name

        Returns:
            True if blob exists, False otherwise
        """
        blob_client = self.client.get_blob_client(container=container, blob=blob_name)
        return blob_client.exists()

    def download_blob(self, container: str, blob_name: str) -> bytes:
        """
        Download blob content from container.

        Args:
            container: Container name
            blob_name: Blob name

        Returns:
            Blob content as bytes
        """
        blob_client = self.client.get_blob_client(container=container, blob=blob_name)
        return blob_client.download_blob().readall()
