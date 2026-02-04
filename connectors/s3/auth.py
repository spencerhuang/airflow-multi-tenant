"""S3 authentication module."""

from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class S3Auth:
    """
    S3 authentication credentials.

    This class encapsulates AWS credentials for S3 access.
    Supports IAM role assumption and direct credentials.

    Attributes:
        aws_access_key_id: AWS access key ID (optional if using IAM role)
        aws_secret_access_key: AWS secret access key (optional if using IAM role)
        region_name: AWS region name
        role_arn: IAM role ARN to assume (optional)
        session_name: Session name for role assumption (optional)
        endpoint_url: Custom S3 endpoint URL (for MinIO, LocalStack, etc.)
    """

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region_name: str = "us-east-1"
    role_arn: Optional[str] = None
    session_name: Optional[str] = "airflow-multi-tenant-session"
    endpoint_url: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        """
        Convert authentication credentials to dictionary format.

        Returns:
            Dictionary of non-None credentials
        """
        auth_dict = {}
        if self.aws_access_key_id:
            auth_dict["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            auth_dict["aws_secret_access_key"] = self.aws_secret_access_key
        if self.region_name:
            auth_dict["region_name"] = self.region_name
        if self.endpoint_url:
            auth_dict["endpoint_url"] = self.endpoint_url
        return auth_dict

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "S3Auth":
        """
        Create S3Auth instance from dictionary.

        Args:
            data: Dictionary containing authentication credentials

        Returns:
            S3Auth instance
        """
        return cls(
            aws_access_key_id=data.get("aws_access_key_id"),
            aws_secret_access_key=data.get("aws_secret_access_key"),
            region_name=data.get("region_name", "us-east-1"),
            role_arn=data.get("role_arn"),
            session_name=data.get("session_name", "airflow-multi-tenant-session"),
            endpoint_url=data.get("endpoint_url"),
        )
