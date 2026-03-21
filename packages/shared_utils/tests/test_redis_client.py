"""Tests for the TLS-aware Redis client and credential vault helpers."""

import json
import os
import pytest
from unittest.mock import patch, Mock, MagicMock, call

import redis as redis_lib

from shared_utils.redis_client import (
    get_redis_client,
    reset_redis_client,
    store_credentials,
    fetch_credentials,
    delete_credentials,
    _credential_key,
    _parse_sentinel_hosts,
    _retry_on_transient,
    _build_tls_kwargs,
    CREDENTIAL_TTL_SECONDS,
    REDIS_MAX_RETRIES,
    REDIS_RETRY_BACKOFF_BASE,
)


class TestCredentialKey:
    """Test Redis key format."""

    def test_key_format(self):
        assert _credential_key("run_123") == "airflow:run_run_123:creds"

    def test_key_with_complex_run_id(self):
        run_id = "ws1_s3_to_mongo_ondemand_scheduled_20260310_0200"
        expected = f"airflow:run_{run_id}:creds"
        assert _credential_key(run_id) == expected


class TestParseSentinelHosts:
    """Test Sentinel host string parsing."""

    def test_single_host(self):
        result = _parse_sentinel_hosts("sentinel-0:26379")
        assert result == [("sentinel-0", 26379)]

    def test_multiple_hosts(self):
        result = _parse_sentinel_hosts(
            "sentinel-0:26379,sentinel-1:26379,sentinel-2:26379"
        )
        assert result == [
            ("sentinel-0", 26379),
            ("sentinel-1", 26379),
            ("sentinel-2", 26379),
        ]

    def test_default_port(self):
        result = _parse_sentinel_hosts("sentinel-0")
        assert result == [("sentinel-0", 26379)]

    def test_empty_string(self):
        result = _parse_sentinel_hosts("")
        assert result == []

    def test_whitespace_handling(self):
        result = _parse_sentinel_hosts(" host-a:26379 , host-b:26379 ")
        assert result == [("host-a", 26379), ("host-b", 26379)]


class TestBuildTlsKwargs:
    """Test TLS kwargs builder."""

    def test_returns_tls_kwargs_when_cert_exists(self):
        with patch("shared_utils.redis_client.os.path.isfile", return_value=True):
            kwargs = _build_tls_kwargs()
        assert kwargs["ssl"] is True
        assert "ssl_ca_certs" in kwargs
        assert "ssl_certfile" in kwargs
        assert "ssl_keyfile" in kwargs

    def test_returns_empty_when_no_cert(self):
        with patch("shared_utils.redis_client.os.path.isfile", return_value=False):
            kwargs = _build_tls_kwargs()
        assert kwargs == {}


class TestRetryOnTransient:
    """Test _retry_on_transient helper."""

    @patch("shared_utils.redis_client.time.sleep")
    def test_succeeds_on_first_try(self, mock_sleep):
        result = _retry_on_transient(lambda: "ok", "test_op")
        assert result == "ok"
        mock_sleep.assert_not_called()

    @patch("shared_utils.redis_client.time.sleep")
    def test_retries_on_connection_error_then_succeeds(self, mock_sleep):
        call_count = 0

        def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise redis_lib.ConnectionError("gone")
            return "recovered"

        result = _retry_on_transient(flaky_op, "test_op", max_retries=3)
        assert result == "recovered"
        assert mock_sleep.call_count == 2
        # Verify exponential backoff: 0.5s, 1.0s
        mock_sleep.assert_has_calls([
            call(REDIS_RETRY_BACKOFF_BASE * 1),
            call(REDIS_RETRY_BACKOFF_BASE * 2),
        ])

    @patch("shared_utils.redis_client.time.sleep")
    def test_retries_on_timeout_error(self, mock_sleep):
        call_count = 0

        def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise redis_lib.TimeoutError("slow")
            return "ok"

        result = _retry_on_transient(flaky_op, "test_op")
        assert result == "ok"
        assert mock_sleep.call_count == 1

    @patch("shared_utils.redis_client.time.sleep")
    def test_raises_connection_error_after_max_retries(self, mock_sleep):
        """Non-Airflow context: raises ConnectionError after retries exhausted."""

        def always_fail():
            raise redis_lib.ConnectionError("down")

        with pytest.raises(ConnectionError, match="Redis unavailable after 3"):
            _retry_on_transient(always_fail, "test_op", max_retries=3)
        assert mock_sleep.call_count == 2  # retries between attempts 1-2 and 2-3

    @patch("shared_utils.redis_client.time.sleep")
    def test_does_not_retry_on_non_transient_error(self, mock_sleep):
        """Non-transient errors (e.g. DataError) propagate immediately."""

        def bad_data():
            raise redis_lib.DataError("bad data")

        with pytest.raises(redis_lib.DataError, match="bad data"):
            _retry_on_transient(bad_data, "test_op")
        mock_sleep.assert_not_called()


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
            with patch("shared_utils.redis_client.REDIS_SENTINEL_HOSTS", ""):
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
            with patch("shared_utils.redis_client.REDIS_SENTINEL_HOSTS", ""):
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
            with patch("shared_utils.redis_client.REDIS_SENTINEL_HOSTS", ""):
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
            with patch("shared_utils.redis_client.REDIS_SENTINEL_HOSTS", ""):
                with patch.dict(os.environ, env):
                    get_redis_client()

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["host"] == "my-redis"
        assert call_kwargs["port"] == 6380


class TestGetRedisClientSentinel:
    """Test Sentinel mode for get_redis_client."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_infra_secrets")
    @patch("shared_utils.redis_client.redis_lib.sentinel.Sentinel")
    def test_sentinel_mode_when_env_set(self, mock_sentinel_cls, mock_secrets):
        """When REDIS_SENTINEL_HOSTS is set, uses Sentinel for master discovery."""
        mock_secrets.return_value = Mock(redis_password="pw")
        mock_master = MagicMock()
        mock_sentinel = MagicMock()
        mock_sentinel.master_for.return_value = mock_master
        mock_sentinel_cls.return_value = mock_sentinel

        sentinel_hosts = "sentinel-0:26379,sentinel-1:26379"
        with patch("shared_utils.redis_client.REDIS_SENTINEL_HOSTS", sentinel_hosts):
            with patch("shared_utils.redis_client.REDIS_SENTINEL_MASTER", "mymaster"):
                with patch("shared_utils.redis_client.os.path.isfile", return_value=False):
                    client = get_redis_client()

        # Verify Sentinel was constructed with parsed hosts
        mock_sentinel_cls.assert_called_once()
        sentinel_args = mock_sentinel_cls.call_args
        assert sentinel_args[0][0] == [("sentinel-0", 26379), ("sentinel-1", 26379)]

        # Verify master_for was called
        mock_sentinel.master_for.assert_called_once_with(
            "mymaster",
            password="pw",
            decode_responses=True,
        )
        mock_master.ping.assert_called_once()
        assert client is mock_master

    @patch("shared_utils.redis_client.get_infra_secrets")
    @patch("shared_utils.redis_client.redis_lib.Redis")
    def test_standalone_mode_when_sentinel_not_set(self, mock_redis_cls, mock_secrets):
        """When REDIS_SENTINEL_HOSTS is empty, uses standalone Redis."""
        mock_secrets.return_value = Mock(redis_password="pw")
        mock_redis_cls.return_value = MagicMock()

        with patch("shared_utils.redis_client.REDIS_SENTINEL_HOSTS", ""):
            with patch("shared_utils.redis_client.os.path.isfile", return_value=False):
                get_redis_client()

        mock_redis_cls.assert_called_once()


class TestStoreCredentials:
    """Test store_credentials vault writer."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_stores_with_setex(self, mock_sleep, mock_get_client):
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
    @patch("shared_utils.redis_client.time.sleep")
    def test_custom_ttl(self, mock_sleep, mock_get_client):
        """store_credentials respects custom TTL."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        store_credentials("run_123", {"key": "val"}, ttl=600)

        assert mock_client.setex.call_args[0][1] == 600

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_retries_on_transient_failure(self, mock_sleep, mock_get_client):
        """store_credentials retries on ConnectionError."""
        mock_client = MagicMock()
        mock_client.setex.side_effect = [
            redis_lib.ConnectionError("gone"),
            None,  # success on second try
        ]
        mock_get_client.return_value = mock_client

        store_credentials("run_123", {"k": "v"})

        assert mock_client.setex.call_count == 2
        mock_sleep.assert_called_once()


class TestFetchCredentials:
    """Test fetch_credentials vault reader."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_fetches_and_deserializes(self, mock_sleep, mock_get_client):
        """fetch_credentials returns deserialized dict."""
        creds = {"s3_access_key": "ak", "s3_secret_key": "sk"}
        mock_client = MagicMock()
        mock_client.get.return_value = json.dumps(creds)
        mock_get_client.return_value = mock_client

        result = fetch_credentials("run_123")

        assert result == creds
        mock_client.get.assert_called_once_with("airflow:run_run_123:creds")

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_raises_runtime_error_on_missing_key(self, mock_sleep, mock_get_client):
        """fetch_credentials raises RuntimeError when key expired (non-Airflow)."""
        mock_client = MagicMock()
        mock_client.get.return_value = None
        mock_get_client.return_value = mock_client

        with pytest.raises(RuntimeError, match="Credentials expired"):
            fetch_credentials("run_123")

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_raises_connection_error_when_redis_down(self, mock_sleep, mock_get_client):
        """fetch_credentials raises ConnectionError (retryable) when Redis is down."""
        mock_client = MagicMock()
        mock_client.get.side_effect = redis_lib.ConnectionError("down")
        mock_get_client.return_value = mock_client

        with pytest.raises(ConnectionError, match="Redis unavailable"):
            fetch_credentials("run_123")

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_retries_transient_then_returns(self, mock_sleep, mock_get_client):
        """fetch_credentials retries on transient error, then returns value."""
        creds = {"key": "val"}
        mock_client = MagicMock()
        mock_client.get.side_effect = [
            redis_lib.ConnectionError("blip"),
            json.dumps(creds),
        ]
        mock_get_client.return_value = mock_client

        result = fetch_credentials("run_123")

        assert result == creds
        assert mock_client.get.call_count == 2
        mock_sleep.assert_called_once()

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_error_message_distinguishes_expired_vs_down(
        self, mock_sleep, mock_get_client
    ):
        """Expired key gets 'expired' message, Redis down gets 'unavailable'."""
        mock_client = MagicMock()

        # Test expired key message
        mock_client.get.return_value = None
        mock_get_client.return_value = mock_client
        with pytest.raises(RuntimeError, match="expired"):
            fetch_credentials("run_expired")

        # Test Redis down message
        mock_client.get.side_effect = redis_lib.ConnectionError("nope")
        mock_client.get.return_value = None  # won't be used
        with pytest.raises(ConnectionError, match="unavailable"):
            fetch_credentials("run_down")


class TestDeleteCredentials:
    """Test delete_credentials cleanup."""

    def setup_method(self):
        reset_redis_client()

    def teardown_method(self):
        reset_redis_client()

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_deletes_key(self, mock_sleep, mock_get_client):
        """delete_credentials calls Redis DELETE."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        delete_credentials("run_123")

        mock_client.delete.assert_called_once_with("airflow:run_run_123:creds")

    @patch("shared_utils.redis_client.get_redis_client")
    @patch("shared_utils.redis_client.time.sleep")
    def test_retries_on_transient_failure(self, mock_sleep, mock_get_client):
        """delete_credentials retries on ConnectionError."""
        mock_client = MagicMock()
        mock_client.delete.side_effect = [
            redis_lib.ConnectionError("gone"),
            1,  # success
        ]
        mock_get_client.return_value = mock_client

        delete_credentials("run_123")

        assert mock_client.delete.call_count == 2
        mock_sleep.assert_called_once()
