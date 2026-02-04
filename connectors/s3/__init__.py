"""S3 Connector module for interacting with Amazon S3."""

from connectors.s3.client import S3Client
from connectors.s3.auth import S3Auth
from connectors.s3.reader import S3Reader

__all__ = ["S3Client", "S3Auth", "S3Reader"]
