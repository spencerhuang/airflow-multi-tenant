"""Unit tests for MongoDB connector."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pymongo.errors import PyMongoError

from connectors.mongo.auth import MongoAuth
from connectors.mongo.client import MongoClient


class TestMongoAuth:
    """Test MongoAuth class."""

    def test_auth_creation_with_credentials(self):
        """Test creating MongoAuth with credentials."""
        auth = MongoAuth(
            host="localhost",
            port=27017,
            username="admin",
            password="secret",
            database="testdb",
        )

        assert auth.host == "localhost"
        assert auth.port == 27017
        assert auth.username == "admin"
        assert auth.password == "secret"
        assert auth.database == "testdb"

    def test_auth_creation_without_credentials(self):
        """Test creating MongoAuth without credentials."""
        auth = MongoAuth(host="localhost", database="testdb")

        assert auth.host == "localhost"
        assert auth.port == 27017
        assert auth.username is None
        assert auth.password is None
        assert auth.database == "testdb"

    def test_get_connection_string_with_auth(self):
        """Test generating connection string with authentication."""
        auth = MongoAuth(
            host="localhost",
            port=27017,
            username="admin",
            password="secret",
            database="testdb",
            auth_source="admin",
        )

        conn_str = auth.get_connection_string()
        assert "mongodb://admin:secret@localhost:27017/testdb" in conn_str
        assert "authSource=admin" in conn_str

    def test_get_connection_string_without_auth(self):
        """Test generating connection string without authentication."""
        auth = MongoAuth(host="localhost", port=27017, database="testdb")

        conn_str = auth.get_connection_string()
        assert conn_str == "mongodb://localhost:27017/testdb"

    def test_get_connection_string_with_ssl(self):
        """Test generating connection string with SSL."""
        auth = MongoAuth(
            host="localhost",
            port=27017,
            username="admin",
            password="secret",
            database="testdb",
            use_ssl=True,
        )

        conn_str = auth.get_connection_string()
        assert "ssl=true" in conn_str

    def test_get_connection_string_with_replica_set(self):
        """Test generating connection string with replica set."""
        auth = MongoAuth(
            host="localhost",
            port=27017,
            username="admin",
            password="secret",
            database="testdb",
            replica_set="rs0",
        )

        conn_str = auth.get_connection_string()
        assert "replicaSet=rs0" in conn_str

    def test_auth_from_dict(self):
        """Test creating auth from dictionary."""
        data = {
            "host": "localhost",
            "port": 27017,
            "username": "admin",
            "password": "secret",
            "database": "testdb",
            "auth_source": "admin",
            "replica_set": "rs0",
            "use_ssl": True,
        }

        auth = MongoAuth.from_dict(data)

        assert auth.host == "localhost"
        assert auth.port == 27017
        assert auth.username == "admin"
        assert auth.password == "secret"
        assert auth.database == "testdb"
        assert auth.auth_source == "admin"
        assert auth.replica_set == "rs0"
        assert auth.use_ssl is True

    def test_auth_from_dict_with_defaults(self):
        """Test creating auth from dictionary with default values."""
        data = {"host": "mongo.example.com"}

        auth = MongoAuth.from_dict(data)

        assert auth.host == "mongo.example.com"
        assert auth.port == 27017
        assert auth.database == "default"
        assert auth.use_ssl is False


class TestMongoClient:
    """Test MongoClient class."""

    @pytest.fixture
    def mock_pymongo_client(self):
        """Fixture for mocked PyMongo client."""
        with patch("connectors.mongo.client.PyMongoClient") as mock:
            yield mock

    @pytest.fixture
    def mongo_auth(self):
        """Fixture for MongoAuth."""
        return MongoAuth(
            host="localhost",
            port=27017,
            username="admin",
            password="secret",
            database="testdb",
        )

    def test_client_initialization(self, mock_pymongo_client, mongo_auth):
        """Test MongoClient initialization."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)

        assert client.auth == mongo_auth
        mock_pymongo_client.assert_called_once()

    def test_insert_one_success(self, mock_pymongo_client, mongo_auth):
        """Test inserting a single document successfully."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.inserted_id = "507f1f77bcf86cd799439011"

        mock_collection.insert_one.return_value = mock_result
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)
        document = {"name": "John", "age": 30}
        result_id = client.insert_one("users", document)

        assert result_id == "507f1f77bcf86cd799439011"
        mock_collection.insert_one.assert_called_once_with(document)

    def test_insert_one_failure(self, mock_pymongo_client, mongo_auth):
        """Test insert_one raises exception on failure."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_collection.insert_one.side_effect = PyMongoError("Connection failed")

        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)

        with pytest.raises(Exception) as exc_info:
            client.insert_one("users", {"name": "John"})

        assert "Failed to insert document into users" in str(exc_info.value)

    def test_insert_many_success(self, mock_pymongo_client, mongo_auth):
        """Test inserting multiple documents successfully."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.inserted_ids = ["id1", "id2", "id3"]

        mock_collection.insert_many.return_value = mock_result
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)
        documents = [{"name": "John"}, {"name": "Jane"}, {"name": "Bob"}]
        result_ids = client.insert_many("users", documents)

        assert result_ids == ["id1", "id2", "id3"]
        mock_collection.insert_many.assert_called_once_with(documents)

    def test_find_success(self, mock_pymongo_client, mongo_auth):
        """Test finding documents successfully."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(
            return_value=iter([{"name": "John", "age": 30}, {"name": "Jane", "age": 25}])
        )

        mock_collection.find.return_value = mock_cursor
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)
        results = client.find("users", {"age": {"$gte": 25}})

        assert len(results) == 2
        assert results[0]["name"] == "John"
        mock_collection.find.assert_called_once_with({"age": {"$gte": 25}})

    def test_find_with_limit(self, mock_pymongo_client, mongo_auth):
        """Test finding documents with limit."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_cursor = Mock()
        mock_cursor.limit = Mock(return_value=mock_cursor)
        mock_cursor.__iter__ = Mock(return_value=iter([{"name": "John"}]))

        mock_collection.find.return_value = mock_cursor
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)
        results = client.find("users", {}, limit=1)

        assert len(results) == 1
        mock_cursor.limit.assert_called_once_with(1)

    def test_update_one_success(self, mock_pymongo_client, mongo_auth):
        """Test updating a document successfully."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.modified_count = 1

        mock_collection.update_one.return_value = mock_result
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)
        modified = client.update_one("users", {"name": "John"}, {"$set": {"age": 31}})

        assert modified == 1
        mock_collection.update_one.assert_called_once()

    def test_delete_many_success(self, mock_pymongo_client, mongo_auth):
        """Test deleting multiple documents successfully."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.deleted_count = 5

        mock_collection.delete_many.return_value = mock_result
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)
        deleted = client.delete_many("users", {"age": {"$lt": 18}})

        assert deleted == 5
        mock_collection.delete_many.assert_called_once_with({"age": {"$lt": 18}})

    def test_close(self, mock_pymongo_client, mongo_auth):
        """Test closing MongoDB connection."""
        mock_client_instance = Mock()
        mock_db = Mock()
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_pymongo_client.return_value = mock_client_instance

        client = MongoClient(mongo_auth)
        client.close()

        mock_client_instance.close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
