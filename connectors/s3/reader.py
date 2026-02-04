"""S3 reader module for reading data from S3."""

from typing import Iterator, Optional, List
import json
import csv
from io import StringIO, BytesIO

from connectors.s3.client import S3Client


class S3Reader:
    """
    S3 reader for reading and parsing data from S3 objects.

    Supports common data formats: JSON, CSV, text, binary.

    Attributes:
        client: S3Client instance
    """

    def __init__(self, client: S3Client):
        """
        Initialize S3 reader with S3 client.

        Args:
            client: S3Client instance
        """
        self.client = client

    def read_json(self, bucket: str, key: str) -> dict:
        """
        Read and parse JSON file from S3.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            Parsed JSON as dictionary

        Raises:
            json.JSONDecodeError: If JSON parsing fails
        """
        data = self.client.get_object(bucket, key)
        return json.loads(data.decode("utf-8"))

    def read_jsonl(self, bucket: str, key: str) -> Iterator[dict]:
        """
        Read and parse JSON Lines file from S3.

        Args:
            bucket: S3 bucket name
            key: Object key

        Yields:
            Parsed JSON objects, one per line

        Raises:
            json.JSONDecodeError: If JSON parsing fails
        """
        data = self.client.get_object(bucket, key)
        for line in data.decode("utf-8").splitlines():
            if line.strip():
                yield json.loads(line)

    def read_csv(
        self, bucket: str, key: str, delimiter: str = ",", has_header: bool = True
    ) -> List[dict]:
        """
        Read and parse CSV file from S3.

        Args:
            bucket: S3 bucket name
            key: Object key
            delimiter: CSV delimiter character
            has_header: Whether CSV has header row

        Returns:
            List of dictionaries (if has_header) or list of lists

        Raises:
            csv.Error: If CSV parsing fails
        """
        data = self.client.get_object(bucket, key)
        csv_file = StringIO(data.decode("utf-8"))

        if has_header:
            reader = csv.DictReader(csv_file, delimiter=delimiter)
            return list(reader)
        else:
            reader = csv.reader(csv_file, delimiter=delimiter)
            return list(reader)

    def read_text(self, bucket: str, key: str, encoding: str = "utf-8") -> str:
        """
        Read text file from S3.

        Args:
            bucket: S3 bucket name
            key: Object key
            encoding: Text encoding

        Returns:
            File content as string

        Raises:
            UnicodeDecodeError: If decoding fails
        """
        data = self.client.get_object(bucket, key)
        return data.decode(encoding)

    def read_bytes(self, bucket: str, key: str) -> bytes:
        """
        Read binary file from S3.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            File content as bytes
        """
        return self.client.get_object(bucket, key)

    def list_and_filter(
        self, bucket: str, prefix: str = "", suffix: str = "", max_keys: int = 1000
    ) -> List[str]:
        """
        List objects in S3 bucket and filter by suffix.

        Args:
            bucket: S3 bucket name
            prefix: Object key prefix
            suffix: Object key suffix (e.g., '.json')
            max_keys: Maximum number of keys to return

        Returns:
            List of object keys matching filter
        """
        objects = self.client.list_objects(bucket, prefix, max_keys)
        keys = [obj["Key"] for obj in objects]

        if suffix:
            keys = [key for key in keys if key.endswith(suffix)]

        return keys
