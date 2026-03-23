"""Tests for audit event sensitive data masking.

The audit consumer masks sensitive fields (password, secret, token, key)
before persisting to the database. These tests verify the masking logic.
"""

import json

import pytest

from audit_service.app.services.audit_consumer import _mask_sensitive, _mask_dict


class TestMaskSensitive:
    """Test the _mask_sensitive function."""

    def test_masks_password_field(self):
        """Password fields should be replaced with [REDACTED]."""
        data = json.dumps({"user": "admin", "password": "secret123"})
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["password"] == "[REDACTED]"
        assert parsed["user"] == "admin"

    def test_masks_secret_key_field(self):
        """Fields containing 'secret' or 'key' should be masked."""
        data = json.dumps({
            "s3_access_key": "AKIAIOSFODNN7EXAMPLE",
            "s3_secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "bucket": "my-bucket",
        })
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["s3_access_key"] == "[REDACTED]"
        assert parsed["s3_secret_key"] == "[REDACTED]"
        assert parsed["bucket"] == "my-bucket"

    def test_masks_token_field(self):
        """Fields containing 'token' should be masked."""
        data = json.dumps({"auth_token": "eyJhbGciOiJIUzI1NiJ9...", "user_id": "42"})
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["auth_token"] == "[REDACTED]"
        assert parsed["user_id"] == "42"

    def test_masks_api_key_field(self):
        """Fields containing 'api_key' should be masked."""
        data = json.dumps({"api_key": "sk-abc123", "endpoint": "https://api.example.com"})
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["api_key"] == "[REDACTED]"
        assert parsed["endpoint"] == "https://api.example.com"

    def test_masks_credential_field(self):
        """Fields containing 'credential' should be masked."""
        data = json.dumps({"credential": "my-cred", "type": "oauth"})
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["credential"] == "[REDACTED]"

    def test_masks_nested_sensitive_fields(self):
        """Sensitive fields in nested dicts should also be masked."""
        data = json.dumps({
            "config": {
                "mongo_password": "root123",
                "mongo_host": "localhost",
            }
        })
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["config"]["mongo_password"] == "[REDACTED]"
        assert parsed["config"]["mongo_host"] == "localhost"

    def test_masks_sensitive_in_lists(self):
        """Sensitive fields inside list items should be masked."""
        data = json.dumps({
            "credentials": [
                {"password": "pass1", "type": "basic"},
                {"token": "tok123", "type": "bearer"},
            ]
        })
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["credentials"][0]["password"] == "[REDACTED]"
        assert parsed["credentials"][1]["token"] == "[REDACTED]"

    def test_truncates_large_values(self):
        """Values > 1KB should be truncated."""
        large_data = json.dumps({"data": "x" * 2000})
        result = _mask_sensitive(large_data)
        assert len(result) < len(large_data)
        assert result.endswith("...[TRUNCATED]")

    def test_returns_none_for_none(self):
        """None input should return None."""
        assert _mask_sensitive(None) is None

    def test_returns_empty_for_empty(self):
        """Empty string should return empty string."""
        assert _mask_sensitive("") == ""

    def test_handles_non_json_string(self):
        """Non-JSON strings should be returned as-is (with truncation)."""
        result = _mask_sensitive("not json")
        assert result == "not json"

    def test_case_insensitive_matching(self):
        """Matching should be case-insensitive."""
        data = json.dumps({"PASSWORD": "secret", "Api_Key": "key123"})
        result = _mask_sensitive(data)
        parsed = json.loads(result)
        assert parsed["PASSWORD"] == "[REDACTED]"
        assert parsed["Api_Key"] == "[REDACTED]"


class TestMaskDict:
    """Test the _mask_dict helper."""

    def test_masks_in_place(self):
        """_mask_dict should modify the dict in place."""
        data = {"password": "secret", "user": "admin"}
        _mask_dict(data)
        assert data["password"] == "[REDACTED]"
        assert data["user"] == "admin"

    def test_handles_non_dict(self):
        """Non-dict/list values should be ignored."""
        _mask_dict("string")
        _mask_dict(42)
        _mask_dict(None)
