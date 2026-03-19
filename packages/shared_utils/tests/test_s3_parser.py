"""Tests for s3_parser.parse_s3_uri."""

import pytest
from shared_utils.s3_parser import parse_s3_uri


class TestParseS3Uri:
    """Tests for S3 URI parsing."""

    def test_basic_uri(self):
        result = parse_s3_uri("s3://my-bucket/path/to/data")
        assert result["bucket"] == "my-bucket"
        assert result["prefix"] == "path/to/data"

    def test_no_prefix(self):
        result = parse_s3_uri("s3://my-bucket")
        assert result["bucket"] == "my-bucket"
        assert result["prefix"] == ""

    def test_trailing_slash_on_bucket(self):
        result = parse_s3_uri("s3://my-bucket/")
        assert result["bucket"] == "my-bucket"
        assert result["prefix"] == ""

    def test_trailing_slash_on_prefix(self):
        result = parse_s3_uri("s3://my-bucket/prefix/")
        assert result["bucket"] == "my-bucket"
        assert result["prefix"] == "prefix/"

    def test_deeply_nested_prefix(self):
        result = parse_s3_uri("s3://bucket/a/b/c/d/file.json")
        assert result["bucket"] == "bucket"
        assert result["prefix"] == "a/b/c/d/file.json"

    def test_invalid_scheme(self):
        with pytest.raises(ValueError, match="Unsupported S3 URI scheme"):
            parse_s3_uri("http://bucket/key")

    def test_empty_bucket(self):
        with pytest.raises(ValueError, match="missing a bucket name"):
            parse_s3_uri("s3:///prefix")

    def test_empty_string(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_s3_uri("")

    def test_whitespace_only(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_s3_uri("   ")
