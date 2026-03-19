"""Tests for the unified secret provider."""

import os
import pytest
from unittest.mock import patch, mock_open

from shared_utils.secret_provider import (
    read_secret,
    InfraSecrets,
    get_infra_secrets,
    reset_infra_secrets,
)


class TestReadSecret:
    """Test read_secret resolution order: file -> env -> default."""

    def test_reads_from_file_first(self, tmp_path):
        """File-based secret takes priority over env and default."""
        secret_file = tmp_path / "my_secret"
        secret_file.write_text("file_value\n")

        with patch.dict(os.environ, {"MY_SECRET": "env_value"}):
            result = read_secret("my_secret", default="default_value")
            # Falls through to env because SECRETS_DIR doesn't point to tmp_path
            assert result == "env_value"

        # Now point SECRETS_DIR to tmp_path
        with patch("shared_utils.secret_provider.SECRETS_DIR", str(tmp_path)):
            result = read_secret("my_secret", default="default_value")
            assert result == "file_value"

    def test_falls_back_to_env(self):
        """When no file exists, reads from environment."""
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            with patch.dict(os.environ, {"MY_SECRET": "env_value"}, clear=False):
                result = read_secret("my_secret", default="default_value")
                assert result == "env_value"

    def test_falls_back_to_default(self):
        """When no file or env var, returns default."""
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("MY_SECRET", None)
                result = read_secret("my_secret", default="default_value")
                assert result == "default_value"

    def test_returns_none_when_no_default(self):
        """When nothing found and no default, returns None."""
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("MY_SECRET", None)
                result = read_secret("my_secret")
                assert result is None

    def test_strips_whitespace_from_file(self, tmp_path):
        """File values are stripped of whitespace/newlines."""
        secret_file = tmp_path / "my_secret"
        secret_file.write_text("  secret_with_spaces  \n")

        with patch("shared_utils.secret_provider.SECRETS_DIR", str(tmp_path)):
            result = read_secret("my_secret")
            assert result == "secret_with_spaces"

    def test_skips_empty_file(self, tmp_path):
        """Empty files are treated as missing."""
        secret_file = tmp_path / "my_secret"
        secret_file.write_text("  \n")

        with patch("shared_utils.secret_provider.SECRETS_DIR", str(tmp_path)):
            with patch.dict(os.environ, {"MY_SECRET": "env_value"}, clear=False):
                result = read_secret("my_secret")
                assert result == "env_value"

    def test_case_insensitive_file_lookup(self, tmp_path):
        """File lookup uses lowercase key."""
        secret_file = tmp_path / "redis_password"
        secret_file.write_text("redis_secret")

        with patch("shared_utils.secret_provider.SECRETS_DIR", str(tmp_path)):
            result = read_secret("REDIS_PASSWORD")
            assert result == "redis_secret"

    def test_case_insensitive_env_lookup(self):
        """Env lookup tries uppercase key."""
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            with patch.dict(os.environ, {"MY_SECRET": "env_value"}, clear=False):
                result = read_secret("my_secret")
                assert result == "env_value"


class TestInfraSecrets:
    """Test InfraSecrets dataclass loading."""

    def test_load_with_defaults(self):
        """InfraSecrets.load() returns sensible defaults."""
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            with patch.dict(os.environ, {}, clear=False):
                # Remove any env vars that might interfere
                for key in ["MYSQL_PASSWORD", "POSTGRES_PASSWORD", "REDIS_PASSWORD",
                            "AIRFLOW_FERNET_KEY", "AIRFLOW_WEBSERVER_SECRET_KEY",
                            "AIRFLOW_PASSWORD", "KAFKA_PASSWORD"]:
                    os.environ.pop(key, None)

                secrets = InfraSecrets.load()

                assert secrets.mysql_password == "control_plane"
                assert secrets.postgres_password == "airflow"
                assert secrets.redis_password == "changeme_redis"
                assert secrets.airflow_fernet_key == ""
                assert secrets.airflow_password == "airflow"
                assert secrets.kafka_password is None

    def test_load_from_env(self):
        """InfraSecrets.load() reads from environment."""
        env = {
            "MYSQL_PASSWORD": "mysql_pw",
            "POSTGRES_PASSWORD": "pg_pw",
            "REDIS_PASSWORD": "redis_pw",
            "AIRFLOW_FERNET_KEY": "fernet123",
            "AIRFLOW_PASSWORD": "airflow_pw",
        }
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            with patch.dict(os.environ, env, clear=False):
                secrets = InfraSecrets.load()

                assert secrets.mysql_password == "mysql_pw"
                assert secrets.postgres_password == "pg_pw"
                assert secrets.redis_password == "redis_pw"
                assert secrets.airflow_fernet_key == "fernet123"
                assert secrets.airflow_password == "airflow_pw"

    def test_frozen_dataclass(self):
        """InfraSecrets instances are immutable."""
        secrets = InfraSecrets(
            mysql_password="a", postgres_password="b", redis_password="c",
            airflow_fernet_key="d", airflow_webserver_secret="e", airflow_password="f",
        )
        with pytest.raises(AttributeError):
            secrets.mysql_password = "changed"


class TestGetInfraSecrets:
    """Test singleton behavior."""

    def setup_method(self):
        reset_infra_secrets()

    def teardown_method(self):
        reset_infra_secrets()

    def test_singleton_returns_same_instance(self):
        """get_infra_secrets() returns the same instance on repeated calls."""
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            s1 = get_infra_secrets()
            s2 = get_infra_secrets()
            assert s1 is s2

    def test_reset_allows_reload(self):
        """reset_infra_secrets() allows a fresh load."""
        with patch("shared_utils.secret_provider.SECRETS_DIR", "/nonexistent"):
            s1 = get_infra_secrets()
            reset_infra_secrets()
            s2 = get_infra_secrets()
            assert s1 is not s2
