"""MongoDB client module for interacting with MongoDB."""

from typing import List, Dict, Any, Optional
from pymongo import MongoClient as PyMongoClient
from pymongo.errors import PyMongoError

from connectors.mongo.auth import MongoAuth


class MongoClient:
    """
    MongoDB client for interacting with MongoDB databases.

    This class wraps pymongo client and provides reusable methods
    for common MongoDB operations.

    Attributes:
        auth: MongoDB authentication credentials
        client: PyMongo client instance
        db: Database instance
    """

    def __init__(self, auth: MongoAuth):
        """
        Initialize MongoDB client with authentication credentials.

        Args:
            auth: MongoAuth instance containing credentials
        """
        self.auth = auth
        self.client = PyMongoClient(auth.get_connection_string())
        self.db = self.client[auth.database]

    def insert_one(self, collection: str, document: Dict[str, Any]) -> str:
        """
        Insert a single document into collection.

        Args:
            collection: Collection name
            document: Document to insert

        Returns:
            Inserted document ID as string

        Raises:
            PyMongoError: If insert operation fails
        """
        try:
            result = self.db[collection].insert_one(document)
            return str(result.inserted_id)
        except PyMongoError as e:
            raise Exception(f"Failed to insert document into {collection}: {str(e)}")

    def insert_many(self, collection: str, documents: List[Dict[str, Any]]) -> List[str]:
        """
        Insert multiple documents into collection.

        Args:
            collection: Collection name
            documents: List of documents to insert

        Returns:
            List of inserted document IDs as strings

        Raises:
            PyMongoError: If insert operation fails
        """
        try:
            result = self.db[collection].insert_many(documents)
            return [str(oid) for oid in result.inserted_ids]
        except PyMongoError as e:
            raise Exception(f"Failed to insert documents into {collection}: {str(e)}")

    def find(
        self, collection: str, query: Dict[str, Any] = None, limit: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Find documents in collection matching query.

        Args:
            collection: Collection name
            query: Query filter (default: {})
            limit: Maximum number of documents to return (0 = no limit)

        Returns:
            List of matching documents

        Raises:
            PyMongoError: If find operation fails
        """
        try:
            query = query or {}
            cursor = self.db[collection].find(query)
            if limit > 0:
                cursor = cursor.limit(limit)
            return list(cursor)
        except PyMongoError as e:
            raise Exception(f"Failed to find documents in {collection}: {str(e)}")

    def update_one(
        self, collection: str, filter_query: Dict[str, Any], update: Dict[str, Any]
    ) -> int:
        """
        Update a single document in collection.

        Args:
            collection: Collection name
            filter_query: Filter to match document
            update: Update operations (e.g., {"$set": {"field": "value"}})

        Returns:
            Number of documents modified

        Raises:
            PyMongoError: If update operation fails
        """
        try:
            result = self.db[collection].update_one(filter_query, update)
            return result.modified_count
        except PyMongoError as e:
            raise Exception(f"Failed to update document in {collection}: {str(e)}")

    def delete_many(self, collection: str, query: Dict[str, Any]) -> int:
        """
        Delete documents from collection matching query.

        Args:
            collection: Collection name
            query: Query filter

        Returns:
            Number of documents deleted

        Raises:
            PyMongoError: If delete operation fails
        """
        try:
            result = self.db[collection].delete_many(query)
            return result.deleted_count
        except PyMongoError as e:
            raise Exception(f"Failed to delete documents from {collection}: {str(e)}")

    def close(self) -> None:
        """Close MongoDB connection."""
        self.client.close()
