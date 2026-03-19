"""Tests for the TLS-aware Redis client and credential vault helpers."""

import json
import os
import pytest
from unittest.mock import patch, Mock, MagicMock

from shared_utils.redis_client import (
    get_redis_client,
    reset_redis_client,
    store_credentials,
    fetch_credentials,
    delete_credentials,
    _credential_key,
    CREDENTIAL_TTL_SECONDS,
)


class TestCredentialKey:
    """Test Redis key format."""

    def test_key_format(self):
        assert _credential_key("run_123") == "airflow:run_run_123:creds"

    def test_key_with_complex_run_id(self):
        run_id = "ws1_s3_to_mongo_ondemand_scheduled_20260310_0200"
        expected = f"airflow:run_{run_id}:creds"
        assert _credential_key(run_id) == expected


class TestGetRedisClient:
    """Test TLS-aware Redis client singleton."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_infra_secrets")
    @patch("shared_utils.redis_client.redis_lib.Redis")
    def test_plain_tcp_when_no_ca_cert(self, mock_redis_cls, mock_secrets):
        """When CA cert doesn't exist, connects via plain TCP."""
        mock_secrets.return_value = Mock(redis_password="test_pw")
        mock_client = MagicMock()
        mock_redis_cls.return_value = mock_client

        with patch("shared_utils.redis_client.os.path.isfile", return_value=False):
            client = get_redis_client()

        mock_redis_cls.assert_called_once_with(
            host="redis",
            port=6379,
            password="test_pw",
            decode_responses=True,
        )
        mock_client.ping.assert_called_once()
        assert client is mock_client

    @patch("shared_utils.redis_client.get_infra_secrets")
    @patch("shared_utils.redis_client.redis_lib.Redis")
    def test_tls_when_ca_cert_exists(self, mock_redis_cls, mock_secrets):
        """When CA cert exists, enables SSL with cert paths."""
        mock_secrets.return_value = Mock(redis_password="test_pw")
        mock_client = MagicMock()
        mock_redis_cls.return_value = mock_client

        with patch("shared_utils.redis_client.os.path.isfile", return_value=True):
            client = get_redis_client()

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["ssl"] is True
        assert call_kwargs["ssl_ca_certs"] == "/etc/ssl/redis/ca.crt"
        assert call_kwargs["password"] == "test_pw"
        mock_client.ping.assert_called_once()

    @patch("shared_utils.redis_client.get_infra_secrets")
    @patch("shared_utils.redis_client.redis_lib.Redis")
    def test_singleton_returns_same_client(self, mock_redis_cls, mock_secrets):
        """get_redis_client() returns the same instance on repeated calls."""
        mock_secrets.return_value = Mock(redis_password="test_pw")
        mock_redis_cls.return_value = MagicMock()

        with patch("shared_utils.redis_client.os.path.isfile", return_value=False):
            c1 = get_redis_client()
            c2 = get_redis_client()

        assert c1 is c2
        # Redis constructor called only once
        mock_redis_cls.assert_called_once()

    @patch("shared_utils.redis_client.get_infra_secrets")
    @patch("shared_utils.redis_client.redis_lib.Redis")
    def test_custom_host_and_port(self, mock_redis_cls, mock_secrets):
        """Reads REDIS_HOST and REDIS_PORT from env."""
        mock_secrets.return_value = Mock(redis_password="pw")
        mock_redis_cls.return_value = MagicMock()

        env = {"REDIS_HOST": "my-redis", "REDIS_PORT": "6380"}
        with patch("shared_utils.redis_client.os.path.isfile", return_value=False):
            with patch.dict(os.environ, env):
                get_redis_client()

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["host"] == "my-redis"
        assert call_kwargs["port"] == 6380


class TestStoreCredentials:
    """Test store_credentials vault writer."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_redis_client")
    def test_stores_with_setex(self, mock_get_client):
        """store_credentials uses setex for atomic write+TTL."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        creds = {"s3_access_key": "ak", "s3_secret_key": "sk"}
        store_credentials("run_123", creds)

        mock_client.setex.assert_called_once_with(
            "airflow:run_run_123:creds",
            CREDENTIAL_TTL_SECONDS,
            json.dumps(creds),
        )

    @patch("shared_utils.redis_client.get_redis_client")
    def test_custom_ttl(self, mock_get_client):
        """store_credentials respects custom TTL."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        store_credentials("run_123", {"key": "val"}, ttl=600)

        assert mock_client.setex.call_args[0][1] == 600


class TestFetchCredentials:
    """Test fetch_credentials vault reader."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_redis_client")
    def test_fetches_and_deserializes(self, mock_get_client):
        """fetch_credentials returns deserialized dict."""
        creds = {"s3_access_key": "ak", "s3_secret_key": "sk"}
        mock_client = MagicMock()
        mock_client.get.return_value = json.dumps(creds)
        mock_get_client.return_value = mock_client

        result = fetch_credentials("run_123")

        assert result == creds
        mock_client.get.assert_called_once_with("airflow:run_run_123:creds")

    @patch("shared_utils.redis_client.get_redis_client")
    def test_raises_on_missing_key(self, mock_get_client):
        """fetch_credentials raises RuntimeError when key is missing (TTL expired)."""
        mock_client = MagicMock()
        mock_client.get.return_value = None
        mock_get_client.return_value = mock_client

        with pytest.raises(RuntimeError, match="Credentials expired"):
            fetch_credentials("run_123")


class TestDeleteCredentials:
    """Test delete_credentials cleanup."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_redis_client")
    def test_deletes_key(self, mock_get_client):
        """delete_credentials calls Redis DELETE."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        delete_credentials("run_123")

        mock_client.delete.assert_called_once_with("airflow:run_run_123:creds")
