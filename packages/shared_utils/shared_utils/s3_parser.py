"""S3 URI parser.

Parses s3://bucket/prefix URIs into bucket and prefix components.
"""

from typing import Dict
from urllib.parse import urlparse


def parse_s3_uri(uri: str) -> Dict[str, str]:
    """Parse an S3 URI into bucket and prefix components.

    Args:
        uri: An S3 URI, e.g. "s3://my-bucket/path/to/data"

    Returns:
        Dict with keys: bucket, prefix.

    Raises:
        ValueError: If the URI scheme is not s3:// or the bucket is empty.
    """
    if not uri or not uri.strip():
        raise ValueError("S3 URI must not be empty")

    parsed = urlparse(uri)

    if parsed.scheme != "s3":
        raise ValueError(
            f"Unsupported S3 URI scheme: '{parsed.scheme}'. Expected 's3'."
        )

    bucket = parsed.netloc
    if not bucket:
        raise ValueError(f"S3 URI is missing a bucket name: {uri}")

    # Strip leading slash from path to get prefix
    prefix = parsed.path.lstrip("/") if parsed.path else ""

    return {
        "bucket": bucket,
        "prefix": prefix,
    }
