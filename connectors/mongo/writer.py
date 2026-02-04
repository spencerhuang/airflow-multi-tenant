"""MongoDB writer module for writing data to MongoDB."""

from typing import List, Dict, Any, Iterator
from connectors.mongo.client import MongoClient


class MongoWriter:
    """
    MongoDB writer for bulk writing data to MongoDB collections.

    Provides batch writing capabilities with configurable batch size.

    Attributes:
        client: MongoClient instance
        batch_size: Number of documents to batch before inserting
    """

    def __init__(self, client: MongoClient, batch_size: int = 1000):
        """
        Initialize MongoDB writer with client and batch size.

        Args:
            client: MongoClient instance
            batch_size: Number of documents per batch
        """
        self.client = client
        self.batch_size = batch_size

    def write_batch(self, collection: str, documents: List[Dict[str, Any]]) -> int:
        """
        Write a batch of documents to collection.

        Args:
            collection: Collection name
            documents: List of documents to write

        Returns:
            Number of documents inserted

        Raises:
            Exception: If write operation fails
        """
        if not documents:
            return 0

        inserted_ids = self.client.insert_many(collection, documents)
        return len(inserted_ids)

    def write_stream(
        self, collection: str, document_stream: Iterator[Dict[str, Any]]
    ) -> int:
        """
        Write documents from a stream to collection in batches.

        Args:
            collection: Collection name
            document_stream: Iterator yielding documents

        Returns:
            Total number of documents inserted

        Raises:
            Exception: If write operation fails
        """
        total_inserted = 0
        batch = []

        for document in document_stream:
            batch.append(document)

            if len(batch) >= self.batch_size:
                total_inserted += self.write_batch(collection, batch)
                batch = []

        # Write remaining documents
        if batch:
            total_inserted += self.write_batch(collection, batch)

        return total_inserted

    def upsert(
        self,
        collection: str,
        documents: List[Dict[str, Any]],
        key_field: str = "_id",
    ) -> int:
        """
        Upsert documents into collection (insert or update based on key field).

        Args:
            collection: Collection name
            documents: List of documents to upsert
            key_field: Field to use as unique key for matching

        Returns:
            Number of documents modified

        Raises:
            Exception: If upsert operation fails
        """
        modified_count = 0

        for document in documents:
            if key_field not in document:
                raise ValueError(f"Document missing key field: {key_field}")

            filter_query = {key_field: document[key_field]}
            update = {"$set": document}

            result = self.client.db[collection].update_one(
                filter_query, update, upsert=True
            )
            modified_count += result.modified_count + result.upserted_id is not None

        return modified_count
