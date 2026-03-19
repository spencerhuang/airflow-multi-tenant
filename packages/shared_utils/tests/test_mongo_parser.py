"""Tests for mongo_parser.parse_mongo_uri."""

import pytest
from shared_utils.mongo_parser import parse_mongo_uri


class TestParseMongoUri:
    """Tests for robust MongoDB URI parsing."""

    def test_basic_auth_uri(self):
        result = parse_mongo_uri("mongodb://user:pass@host:27017/mydb")
        assert result["host"] == "host"
        assert result["port"] == 27017
        assert result["username"] == "user"
        assert result["password"] == "pass"
        assert result["database"] == "mydb"

    def test_no_auth(self):
        result = parse_mongo_uri("mongodb://host:27017/")
        assert result["host"] == "host"
        assert result["port"] == 27017
        assert result["username"] is None
        assert result["password"] is None

    def test_default_port(self):
        result = parse_mongo_uri("mongodb://user:pass@host/mydb")
        assert result["port"] == 27017

    def test_no_database_in_path(self):
        result = parse_mongo_uri("mongodb://host:27017/")
        assert result["database"] == "default"

    def test_no_database_no_trailing_slash(self):
        result = parse_mongo_uri("mongodb://host:27017")
        assert result["database"] == "default"

    def test_srv_scheme(self):
        result = parse_mongo_uri("mongodb+srv://user:pass@cluster.example.com/db")
        assert result["host"] == "cluster.example.com"
        assert result["port"] is None
        assert result["username"] == "user"
        assert result["database"] == "db"

    def test_srv_with_port_raises(self):
        with pytest.raises(ValueError, match="must not include a port"):
            parse_mongo_uri("mongodb+srv://user:pass@cluster.example.com:27017/db")

    def test_password_with_at_sign(self):
        # @ is URL-encoded as %40
        result = parse_mongo_uri("mongodb://user:p%40ss@host:27017/db")
        assert result["password"] == "p@ss"

    def test_password_with_colon(self):
        # : is URL-encoded as %3A
        result = parse_mongo_uri("mongodb://user:p%3Ass@host:27017/db")
        assert result["password"] == "p:ss"

    def test_password_with_percent(self):
        # % is URL-encoded as %25
        result = parse_mongo_uri("mongodb://user:100%25done@host:27017/db")
        assert result["password"] == "100%done"

    def test_password_with_slash(self):
        # / is URL-encoded as %2F
        result = parse_mongo_uri("mongodb://user:a%2Fb@host:27017/db")
        assert result["password"] == "a/b"

    def test_query_param_auth_source(self):
        result = parse_mongo_uri("mongodb://user:pass@host/db?authSource=other")
        assert result["auth_source"] == "other"

    def test_query_param_replica_set(self):
        result = parse_mongo_uri("mongodb://user:pass@host/db?replicaSet=rs0")
        assert result["replica_set"] == "rs0"

    def test_query_param_ssl_true(self):
        result = parse_mongo_uri("mongodb://user:pass@host/db?ssl=true")
        assert result["use_ssl"] is True

    def test_query_param_tls_true(self):
        result = parse_mongo_uri("mongodb://user:pass@host/db?tls=true")
        assert result["use_ssl"] is True

    def test_query_param_ssl_false(self):
        result = parse_mongo_uri("mongodb://user:pass@host/db?ssl=false")
        assert result["use_ssl"] is False

    def test_no_query_params_defaults(self):
        result = parse_mongo_uri("mongodb://host:27017/db")
        assert result["auth_source"] == "admin"
        assert result["replica_set"] is None
        assert result["use_ssl"] is False

    def test_multiple_query_params(self):
        uri = "mongodb://user:pass@host/db?authSource=other&replicaSet=rs0&ssl=true"
        result = parse_mongo_uri(uri)
        assert result["auth_source"] == "other"
        assert result["replica_set"] == "rs0"
        assert result["use_ssl"] is True

    def test_invalid_scheme(self):
        with pytest.raises(ValueError, match="Unsupported MongoDB URI scheme"):
            parse_mongo_uri("postgres://host/db")

    def test_empty_string(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_mongo_uri("")

    def test_whitespace_only(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_mongo_uri("   ")

    def test_missing_hostname(self):
        with pytest.raises(ValueError, match="missing a hostname"):
            parse_mongo_uri("mongodb:///db")

    def test_compatible_with_mongo_auth_from_dict(self):
        """Ensure the returned dict keys match MongoAuth.from_dict() expectations."""
        result = parse_mongo_uri("mongodb://user:pass@host:27017/mydb?authSource=admin&replicaSet=rs0&ssl=true")
        expected_keys = {"host", "port", "username", "password", "database", "auth_source", "replica_set", "use_ssl"}
        assert set(result.keys()) == expected_keys
