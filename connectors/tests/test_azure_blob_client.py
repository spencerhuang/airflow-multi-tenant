"""Unit tests for Azure Blob connector."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from connectors.azure_blob.auth import AzureBlobAuth
from connectors.azure_blob.client import AzureBlobClient


class TestAzureBlobAuth:
    """Test AzureBlobAuth class."""

    def test_auth_creation_with_account_key(self):
        """Test creating AzureBlobAuth with account key."""
        auth = AzureBlobAuth(
            account_name="mystorageaccount",
            account_key="myaccountkey123==",
        )

        assert auth.account_name == "mystorageaccount"
        assert auth.account_key == "myaccountkey123=="
        assert auth.connection_string is None

    def test_auth_creation_with_connection_string(self):
        """Test creating AzureBlobAuth with connection string."""
        conn_str = "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
        auth = AzureBlobAuth(
            account_name="mystorageaccount",
            connection_string=conn_str,
        )

        assert auth.account_name == "mystorageaccount"
        assert auth.connection_string == conn_str

    def test_auth_creation_with_service_principal(self):
        """Test creating AzureBlobAuth with service principal credentials."""
        auth = AzureBlobAuth(
            account_name="mystorageaccount",
            tenant_id="tenant-123",
            client_id="client-456",
            client_secret="secret-789",
        )

        assert auth.account_name == "mystorageaccount"
        assert auth.tenant_id == "tenant-123"
        assert auth.client_id == "client-456"
        assert auth.client_secret == "secret-789"

    def test_auth_from_dict_with_account_key(self):
        """Test creating auth from dictionary with account key."""
        data = {
            "account_name": "mystorageaccount",
            "account_key": "mykey123==",
        }

        auth = AzureBlobAuth.from_dict(data)

        assert auth.account_name == "mystorageaccount"
        assert auth.account_key == "mykey123=="

    def test_auth_from_dict_with_connection_string(self):
        """Test creating auth from dictionary with connection string."""
        conn_str = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key"
        data = {
            "account_name": "mystorageaccount",
            "connection_string": conn_str,
        }

        auth = AzureBlobAuth.from_dict(data)

        assert auth.account_name == "mystorageaccount"
        assert auth.connection_string == conn_str

    def test_auth_from_dict_with_service_principal(self):
        """Test creating auth from dictionary with service principal."""
        data = {
            "account_name": "mystorageaccount",
            "tenant_id": "tenant-123",
            "client_id": "client-456",
            "client_secret": "secret-789",
        }

        auth = AzureBlobAuth.from_dict(data)

        assert auth.account_name == "mystorageaccount"
        assert auth.tenant_id == "tenant-123"
        assert auth.client_id == "client-456"
        assert auth.client_secret == "secret-789"


class TestAzureBlobClient:
    """Test AzureBlobClient class."""

    @pytest.fixture
    def mock_blob_service_client(self):
        """Fixture for mocked BlobServiceClient."""
        with patch("connectors.azure_blob.client.BlobServiceClient") as mock:
            yield mock

    @pytest.fixture
    def auth_with_connection_string(self):
        """Fixture for AzureBlobAuth with connection string."""
        return AzureBlobAuth(
            account_name="mystorageaccount",
            connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key",
        )

    @pytest.fixture
    def auth_with_account_key(self):
        """Fixture for AzureBlobAuth with account key."""
        return AzureBlobAuth(
            account_name="mystorageaccount",
            account_key="myaccountkey123==",
        )

    def test_client_initialization_with_connection_string(
        self, mock_blob_service_client, auth_with_connection_string
    ):
        """Test AzureBlobClient initialization with connection string."""
        mock_client_instance = Mock()
        mock_blob_service_client.from_connection_string.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_connection_string)

        assert client.auth == auth_with_connection_string
        mock_blob_service_client.from_connection_string.assert_called_once_with(
            auth_with_connection_string.connection_string
        )

    def test_client_initialization_with_account_key(
        self, mock_blob_service_client, auth_with_account_key
    ):
        """Test AzureBlobClient initialization with account key."""
        mock_client_instance = Mock()
        mock_blob_service_client.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_account_key)

        assert client.auth == auth_with_account_key
        mock_blob_service_client.assert_called_once_with(
            account_url="https://mystorageaccount.blob.core.windows.net",
            credential="myaccountkey123==",
        )

    def test_client_initialization_service_principal_not_implemented(self):
        """Test that service principal auth raises NotImplementedError."""
        auth = AzureBlobAuth(
            account_name="mystorageaccount",
            tenant_id="tenant-123",
            client_id="client-456",
            client_secret="secret-789",
        )

        with pytest.raises(NotImplementedError):
            AzureBlobClient(auth)

    def test_list_blobs_success(self, mock_blob_service_client, auth_with_connection_string):
        """Test listing blobs successfully."""
        mock_client_instance = Mock()
        mock_container_client = Mock()
        mock_blob1 = Mock()
        mock_blob1.name = "data/file1.json"
        mock_blob2 = Mock()
        mock_blob2.name = "data/file2.json"
        mock_container_client.list_blobs.return_value = [mock_blob1, mock_blob2]

        mock_client_instance.get_container_client.return_value = mock_container_client
        mock_blob_service_client.from_connection_string.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_connection_string)
        blobs = client.list_blobs("test-container", prefix="data/")

        assert len(blobs) == 2
        assert blobs[0] == "data/file1.json"
        assert blobs[1] == "data/file2.json"
        mock_container_client.list_blobs.assert_called_once_with(name_starts_with="data/")

    def test_list_blobs_without_prefix(
        self, mock_blob_service_client, auth_with_connection_string
    ):
        """Test listing blobs without prefix."""
        mock_client_instance = Mock()
        mock_container_client = Mock()
        mock_blob = Mock()
        mock_blob.name = "file.json"
        mock_container_client.list_blobs.return_value = [mock_blob]

        mock_client_instance.get_container_client.return_value = mock_container_client
        mock_blob_service_client.from_connection_string.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_connection_string)
        blobs = client.list_blobs("test-container")

        assert len(blobs) == 1
        mock_container_client.list_blobs.assert_called_once_with(name_starts_with="")

    def test_list_blobs_empty(self, mock_blob_service_client, auth_with_connection_string):
        """Test listing blobs when none exist."""
        mock_client_instance = Mock()
        mock_container_client = Mock()
        mock_container_client.list_blobs.return_value = []

        mock_client_instance.get_container_client.return_value = mock_container_client
        mock_blob_service_client.from_connection_string.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_connection_string)
        blobs = client.list_blobs("test-container")

        assert blobs == []

    def test_blob_exists_true(self, mock_blob_service_client, auth_with_connection_string):
        """Test checking if blob exists (returns True)."""
        mock_client_instance = Mock()
        mock_blob_client = Mock()
        mock_blob_client.exists.return_value = True

        mock_client_instance.get_blob_client.return_value = mock_blob_client
        mock_blob_service_client.from_connection_string.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_connection_string)
        exists = client.blob_exists("test-container", "data/file.json")

        assert exists is True
        mock_client_instance.get_blob_client.assert_called_once_with(
            container="test-container", blob="data/file.json"
        )

    def test_blob_exists_false(self, mock_blob_service_client, auth_with_connection_string):
        """Test checking if blob exists (returns False)."""
        mock_client_instance = Mock()
        mock_blob_client = Mock()
        mock_blob_client.exists.return_value = False

        mock_client_instance.get_blob_client.return_value = mock_blob_client
        mock_blob_service_client.from_connection_string.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_connection_string)
        exists = client.blob_exists("test-container", "data/missing.json")

        assert exists is False

    def test_download_blob_success(self, mock_blob_service_client, auth_with_connection_string):
        """Test downloading blob content successfully."""
        mock_client_instance = Mock()
        mock_blob_client = Mock()
        mock_download_stream = Mock()
        mock_download_stream.readall.return_value = b'{"key": "value"}'

        mock_blob_client.download_blob.return_value = mock_download_stream
        mock_client_instance.get_blob_client.return_value = mock_blob_client
        mock_blob_service_client.from_connection_string.return_value = mock_client_instance

        client = AzureBlobClient(auth_with_connection_string)
        content = client.download_blob("test-container", "data/file.json")

        assert content == b'{"key": "value"}'
        mock_client_instance.get_blob_client.assert_called_once_with(
            container="test-container", blob="data/file.json"
        )
        mock_blob_client.download_blob.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
