"""S3 client module for interacting with Amazon S3."""

import boto3
from typing import Optional, List, Dict, Any
from botocore.exceptions import ClientError

from connectors.s3.auth import S3Auth


class S3Client:
    """
    S3 client for interacting with Amazon S3.

    This class wraps boto3 S3 client and provides reusable methods
    for common S3 operations.

    Attributes:
        auth: S3 authentication credentials
        client: boto3 S3 client instance
    """

    def __init__(self, auth: S3Auth):
        """
        Initialize S3 client with authentication credentials.

        Args:
            auth: S3Auth instance containing credentials
        """
        self.auth = auth
        self.client = self._create_client()

    def _create_client(self):
        """
        Create boto3 S3 client with authentication.

        Returns:
            boto3 S3 client instance

        Raises:
            Exception: If client creation fails
        """
        if self.auth.role_arn:
            # Assume IAM role
            sts_client = boto3.client("sts", **self.auth.to_dict())
            assumed_role = sts_client.assume_role(
                RoleArn=self.auth.role_arn, RoleSessionName=self.auth.session_name
            )
            credentials = assumed_role["Credentials"]
            client_kwargs = {
                "aws_access_key_id": credentials["AccessKeyId"],
                "aws_secret_access_key": credentials["SecretAccessKey"],
                "aws_session_token": credentials["SessionToken"],
                "region_name": self.auth.region_name,
            }
            if self.auth.endpoint_url:
                client_kwargs["endpoint_url"] = self.auth.endpoint_url
            return boto3.client("s3", **client_kwargs)
        else:
            # Use direct credentials
            return boto3.client("s3", **self.auth.to_dict())

    def list_objects(
        self, bucket: str, prefix: str = "", max_keys: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with optional prefix.

        Args:
            bucket: S3 bucket name
            prefix: Object key prefix to filter by
            max_keys: Maximum number of keys to return

        Returns:
            List of object metadata dictionaries

        Raises:
            ClientError: If S3 operation fails
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, MaxKeys=max_keys
            )
            return response.get("Contents", [])
        except ClientError as e:
            raise Exception(f"Failed to list objects in bucket {bucket}: {str(e)}")

    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Check if an object exists in S3 bucket.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            True if object exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def get_object(self, bucket: str, key: str) -> bytes:
        """
        Get object content from S3 bucket.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            Object content as bytes

        Raises:
            ClientError: If S3 operation fails
        """
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            raise Exception(f"Failed to get object {key} from bucket {bucket}: {str(e)}")

    def put_object(self, bucket: str, key: str, data: bytes) -> None:
        """
        Put object to S3 bucket.

        Args:
            bucket: S3 bucket name
            key: Object key
            data: Object content as bytes

        Raises:
            ClientError: If S3 operation fails
        """
        try:
            self.client.put_object(Bucket=bucket, Key=key, Body=data)
        except ClientError as e:
            raise Exception(f"Failed to put object {key} to bucket {bucket}: {str(e)}")

    def delete_object(self, bucket: str, key: str) -> None:
        """
        Delete object from S3 bucket.

        Args:
            bucket: S3 bucket name
            key: Object key

        Raises:
            ClientError: If S3 operation fails
        """
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise Exception(f"Failed to delete object {key} from bucket {bucket}: {str(e)}")
