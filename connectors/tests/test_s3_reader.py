"""Unit tests for S3 Reader."""

import pytest
import json
from unittest.mock import Mock, patch

from connectors.s3.auth import S3Auth
from connectors.s3.client import S3Client
from connectors.s3.reader import S3Reader


class TestS3Reader:
    """Test S3Reader class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Fixture for mocked S3Client."""
        return Mock(spec=S3Client)

    def test_reader_initialization(self, mock_s3_client):
        """Test S3Reader initialization."""
        reader = S3Reader(mock_s3_client)
        assert reader.client == mock_s3_client

    def test_read_json_success(self, mock_s3_client):
        """Test reading JSON file successfully."""
        json_data = {"name": "John", "age": 30, "city": "New York"}
        mock_s3_client.get_object.return_value = json.dumps(json_data).encode("utf-8")

        reader = S3Reader(mock_s3_client)
        result = reader.read_json("test-bucket", "data/file.json")

        assert result == json_data
        mock_s3_client.get_object.assert_called_once_with("test-bucket", "data/file.json")

    def test_read_json_invalid(self, mock_s3_client):
        """Test reading invalid JSON raises error."""
        mock_s3_client.get_object.return_value = b"not valid json {"

        reader = S3Reader(mock_s3_client)

        with pytest.raises(json.JSONDecodeError):
            reader.read_json("test-bucket", "data/invalid.json")

    def test_read_jsonl_success(self, mock_s3_client):
        """Test reading JSON Lines file successfully."""
        jsonl_data = '{"name": "John", "age": 30}\n{"name": "Jane", "age": 25}\n{"name": "Bob", "age": 35}'
        mock_s3_client.get_object.return_value = jsonl_data.encode("utf-8")

        reader = S3Reader(mock_s3_client)
        results = list(reader.read_jsonl("test-bucket", "data/file.jsonl"))

        assert len(results) == 3
        assert results[0] == {"name": "John", "age": 30}
        assert results[1] == {"name": "Jane", "age": 25}
        assert results[2] == {"name": "Bob", "age": 35}

    def test_read_jsonl_empty_lines(self, mock_s3_client):
        """Test reading JSON Lines file with empty lines."""
        jsonl_data = '{"name": "John"}\n\n{"name": "Jane"}\n  \n{"name": "Bob"}'
        mock_s3_client.get_object.return_value = jsonl_data.encode("utf-8")

        reader = S3Reader(mock_s3_client)
        results = list(reader.read_jsonl("test-bucket", "data/file.jsonl"))

        assert len(results) == 3

    def test_read_csv_with_header(self, mock_s3_client):
        """Test reading CSV file with header."""
        csv_data = "name,age,city\nJohn,30,New York\nJane,25,Boston\nBob,35,Chicago"
        mock_s3_client.get_object.return_value = csv_data.encode("utf-8")

        reader = S3Reader(mock_s3_client)
        results = reader.read_csv("test-bucket", "data/file.csv", has_header=True)

        assert len(results) == 3
        assert results[0] == {"name": "John", "age": "30", "city": "New York"}
        assert results[1] == {"name": "Jane", "age": "25", "city": "Boston"}
        assert results[2] == {"name": "Bob", "age": "35", "city": "Chicago"}

    def test_read_csv_without_header(self, mock_s3_client):
        """Test reading CSV file without header."""
        csv_data = "John,30,New York\nJane,25,Boston"
        mock_s3_client.get_object.return_value = csv_data.encode("utf-8")

        reader = S3Reader(mock_s3_client)
        results = reader.read_csv("test-bucket", "data/file.csv", has_header=False)

        assert len(results) == 2
        assert results[0] == ["John", "30", "New York"]
        assert results[1] == ["Jane", "25", "Boston"]

    def test_read_csv_custom_delimiter(self, mock_s3_client):
        """Test reading CSV file with custom delimiter."""
        tsv_data = "name\tage\tcity\nJohn\t30\tNew York"
        mock_s3_client.get_object.return_value = tsv_data.encode("utf-8")

        reader = S3Reader(mock_s3_client)
        results = reader.read_csv("test-bucket", "data/file.tsv", delimiter="\t", has_header=True)

        assert len(results) == 1
        assert results[0] == {"name": "John", "age": "30", "city": "New York"}

    def test_read_text_default_encoding(self, mock_s3_client):
        """Test reading text file with default encoding."""
        text_data = "Hello, World!\nThis is a test file."
        mock_s3_client.get_object.return_value = text_data.encode("utf-8")

        reader = S3Reader(mock_s3_client)
        result = reader.read_text("test-bucket", "data/file.txt")

        assert result == text_data
        mock_s3_client.get_object.assert_called_once_with("test-bucket", "data/file.txt")

    def test_read_text_custom_encoding(self, mock_s3_client):
        """Test reading text file with custom encoding."""
        text_data = "Hello, World!"
        mock_s3_client.get_object.return_value = text_data.encode("latin-1")

        reader = S3Reader(mock_s3_client)
        result = reader.read_text("test-bucket", "data/file.txt", encoding="latin-1")

        assert result == text_data

    def test_read_bytes(self, mock_s3_client):
        """Test reading binary file."""
        binary_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
        mock_s3_client.get_object.return_value = binary_data

        reader = S3Reader(mock_s3_client)
        result = reader.read_bytes("test-bucket", "data/image.png")

        assert result == binary_data
        mock_s3_client.get_object.assert_called_once_with("test-bucket", "data/image.png")

    def test_list_and_filter_with_suffix(self, mock_s3_client):
        """Test listing and filtering objects by suffix."""
        mock_objects = [
            {"Key": "data/file1.json", "Size": 100},
            {"Key": "data/file2.json", "Size": 200},
            {"Key": "data/file3.csv", "Size": 150},
            {"Key": "data/file4.json", "Size": 300},
        ]
        mock_s3_client.list_objects.return_value = mock_objects

        reader = S3Reader(mock_s3_client)
        results = reader.list_and_filter("test-bucket", prefix="data/", suffix=".json")

        assert len(results) == 3
        assert "data/file1.json" in results
        assert "data/file2.json" in results
        assert "data/file4.json" in results
        assert "data/file3.csv" not in results
        mock_s3_client.list_objects.assert_called_once_with("test-bucket", "data/", 1000)

    def test_list_and_filter_without_suffix(self, mock_s3_client):
        """Test listing objects without suffix filter."""
        mock_objects = [
            {"Key": "data/file1.json", "Size": 100},
            {"Key": "data/file2.csv", "Size": 200},
        ]
        mock_s3_client.list_objects.return_value = mock_objects

        reader = S3Reader(mock_s3_client)
        results = reader.list_and_filter("test-bucket", prefix="data/")

        assert len(results) == 2
        assert "data/file1.json" in results
        assert "data/file2.csv" in results

    def test_list_and_filter_custom_max_keys(self, mock_s3_client):
        """Test listing objects with custom max_keys."""
        mock_objects = [{"Key": "data/file1.json", "Size": 100}]
        mock_s3_client.list_objects.return_value = mock_objects

        reader = S3Reader(mock_s3_client)
        results = reader.list_and_filter("test-bucket", prefix="data/", max_keys=100)

        mock_s3_client.list_objects.assert_called_once_with("test-bucket", "data/", 100)

    def test_list_and_filter_empty_result(self, mock_s3_client):
        """Test listing objects with no matches."""
        mock_s3_client.list_objects.return_value = []

        reader = S3Reader(mock_s3_client)
        results = reader.list_and_filter("test-bucket", prefix="data/", suffix=".json")

        assert len(results) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
