"""MongoDB authentication module."""

from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class MongoAuth:
    """
    MongoDB authentication credentials.

    Attributes:
        host: MongoDB host
        port: MongoDB port
        username: MongoDB username (optional)
        password: MongoDB password (optional)
        database: Database name
        auth_source: Authentication database (default: 'admin')
        replica_set: Replica set name (optional)
        use_ssl: Use SSL/TLS connection
    """

    host: str
    port: int = 27017
    username: Optional[str] = None
    password: Optional[str] = None
    database: str = "default"
    auth_source: str = "admin"
    replica_set: Optional[str] = None
    use_ssl: bool = False

    def get_connection_string(self) -> str:
        """
        Generate MongoDB connection string.

        Returns:
            MongoDB connection URI
        """
        if self.username and self.password:
            auth_part = f"{self.username}:{self.password}@"
        else:
            auth_part = ""

        connection_string = f"mongodb://{auth_part}{self.host}:{self.port}/{self.database}"

        params = []
        if self.auth_source and self.username:
            params.append(f"authSource={self.auth_source}")
        if self.replica_set:
            params.append(f"replicaSet={self.replica_set}")
        if self.use_ssl:
            params.append("ssl=true")

        if params:
            connection_string += "?" + "&".join(params)

        return connection_string

    @classmethod
    def from_dict(cls, data: Dict) -> "MongoAuth":
        """
        Create MongoAuth instance from dictionary.

        Args:
            data: Dictionary containing authentication credentials

        Returns:
            MongoAuth instance
        """
        return cls(
            host=data.get("host", "localhost"),
            port=data.get("port", 27017),
            username=data.get("username"),
            password=data.get("password"),
            database=data.get("database", "default"),
            auth_source=data.get("auth_source", "admin"),
            replica_set=data.get("replica_set"),
            use_ssl=data.get("use_ssl", False),
        )
