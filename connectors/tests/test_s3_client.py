"""Unit tests for S3 connector."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from connectors.s3.auth import S3Auth
from connectors.s3.client import S3Client


class TestS3Auth:
    """Test S3Auth class."""

    def test_auth_creation_with_credentials(self):
        """Test creating S3Auth with access key credentials."""
        auth = S3Auth(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="us-west-2",
        )

        assert auth.aws_access_key_id == "test_key"
        assert auth.aws_secret_access_key == "test_secret"
        assert auth.region_name == "us-west-2"

    def test_auth_to_dict(self):
        """Test converting auth to dictionary."""
        auth = S3Auth(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="us-west-2",
        )

        auth_dict = auth.to_dict()

        assert auth_dict["aws_access_key_id"] == "test_key"
        assert auth_dict["aws_secret_access_key"] == "test_secret"
        assert auth_dict["region_name"] == "us-west-2"

    def test_auth_from_dict(self):
        """Test creating auth from dictionary."""
        data = {
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "region_name": "eu-west-1",
        }

        auth = S3Auth.from_dict(data)

        assert auth.aws_access_key_id == "test_key"
        assert auth.aws_secret_access_key == "test_secret"
        assert auth.region_name == "eu-west-1"


class TestS3Client:
    """Test S3Client class."""

    @pytest.fixture
    def mock_boto3_client(self):
        """Fixture for mocked boto3 client."""
        with patch("connectors.s3.client.boto3.client") as mock:
            yield mock

    @pytest.fixture
    def s3_auth(self):
        """Fixture for S3Auth."""
        return S3Auth(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="us-east-1",
        )

    def test_client_initialization(self, mock_boto3_client, s3_auth):
        """Test S3Client initialization."""
        mock_boto3_client.return_value = Mock()

        client = S3Client(s3_auth)

        assert client.auth == s3_auth
        assert client.client is not None
        mock_boto3_client.assert_called_once()

    def test_list_objects_success(self, mock_boto3_client, s3_auth):
        """Test listing S3 objects successfully."""
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.json", "Size": 100},
                {"Key": "file2.json", "Size": 200},
            ]
        }
        mock_boto3_client.return_value = mock_s3

        client = S3Client(s3_auth)
        objects = client.list_objects("test-bucket", "data/")

        assert len(objects) == 2
        assert objects[0]["Key"] == "file1.json"
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="data/", MaxKeys=1000
        )

    def test_list_objects_empty(self, mock_boto3_client, s3_auth):
        """Test listing S3 objects when none exist."""
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {}
        mock_boto3_client.return_value = mock_s3

        client = S3Client(s3_auth)
        objects = client.list_objects("test-bucket")

        assert objects == []

    def test_object_exists_true(self, mock_boto3_client, s3_auth):
        """Test checking if object exists (returns True)."""
        mock_s3 = Mock()
        mock_s3.head_object.return_value = {"ContentLength": 100}
        mock_boto3_client.return_value = mock_s3

        client = S3Client(s3_auth)
        exists = client.object_exists("test-bucket", "file.json")

        assert exists is True
        mock_s3.head_object.assert_called_once_with(Bucket="test-bucket", Key="file.json")

    def test_object_exists_false(self, mock_boto3_client, s3_auth):
        """Test checking if object exists (returns False)."""
        mock_s3 = Mock()
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        mock_boto3_client.return_value = mock_s3

        client = S3Client(s3_auth)
        exists = client.object_exists("test-bucket", "missing.json")

        assert exists is False

    def test_get_object_success(self, mock_boto3_client, s3_auth):
        """Test getting object content successfully."""
        mock_body = Mock()
        mock_body.read.return_value = b'{"key": "value"}'

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {"Body": mock_body}
        mock_boto3_client.return_value = mock_s3

        client = S3Client(s3_auth)
        content = client.get_object("test-bucket", "data.json")

        assert content == b'{"key": "value"}'
        mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="data.json")

    def test_put_object_success(self, mock_boto3_client, s3_auth):
        """Test putting object successfully."""
        mock_s3 = Mock()
        mock_boto3_client.return_value = mock_s3

        client = S3Client(s3_auth)
        client.put_object("test-bucket", "data.json", b'{"key": "value"}')

        mock_s3.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="data.json", Body=b'{"key": "value"}'
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
