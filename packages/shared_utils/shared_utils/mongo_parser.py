"""Robust MongoDB URI parser.

Replaces brittle string-splitting with stdlib urllib.parse for safe handling
of special characters, SRV schemes, replica sets, and query parameters.
"""

from typing import Dict, Any, Optional
from urllib.parse import urlparse, parse_qs, unquote, uses_netloc

# Register MongoDB schemes so urlparse correctly parses the netloc (host, port, user, password).
# Without this, urlparse treats the entire URI as an opaque path.
for _scheme in ("mongodb", "mongodb+srv"):
    if _scheme not in uses_netloc:
        uses_netloc.append(_scheme)


def parse_mongo_uri(uri: str) -> Dict[str, Any]:
    """Parse a MongoDB connection URI into a dict compatible with MongoAuth.from_dict().

    Handles mongodb:// and mongodb+srv:// schemes, URL-encoded credentials,
    query parameters (authSource, replicaSet, ssl/tls), and optional database paths.

    Args:
        uri: A MongoDB connection string, e.g. "mongodb://user:p%40ss@host:27017/mydb?authSource=admin"

    Returns:
        Dict with keys: host, port, username, password, database, auth_source, replica_set, use_ssl.

    Raises:
        ValueError: If the URI scheme is not mongodb:// or mongodb+srv://, or the URI is malformed.
    """
    if not uri or not uri.strip():
        raise ValueError("MongoDB URI must not be empty")

    parsed = urlparse(uri)

    if parsed.scheme not in ("mongodb", "mongodb+srv"):
        raise ValueError(
            f"Unsupported MongoDB URI scheme: '{parsed.scheme}'. "
            "Expected 'mongodb' or 'mongodb+srv'."
        )

    if not parsed.hostname:
        raise ValueError(f"MongoDB URI is missing a hostname: {uri}")

    # SRV records handle port resolution; specifying a port is invalid per the MongoDB spec.
    if parsed.scheme == "mongodb+srv" and parsed.port is not None:
        raise ValueError(
            "mongodb+srv:// URIs must not include a port. "
            "SRV records handle port resolution."
        )

    # Credentials — unquote to handle URL-encoded special characters like @ : %
    username: Optional[str] = unquote(parsed.username) if parsed.username else None
    password: Optional[str] = unquote(parsed.password) if parsed.password else None

    # Database from path (strip leading slash)
    path_db = parsed.path.lstrip("/") if parsed.path else ""
    database = path_db if path_db else "default"

    # Port defaults
    if parsed.scheme == "mongodb+srv":
        port = None
    else:
        port = parsed.port if parsed.port is not None else 27017

    # Query parameters
    query_params = parse_qs(parsed.query)

    auth_source_values = query_params.get("authSource", [])
    auth_source = auth_source_values[0] if auth_source_values else "admin"

    replica_set_values = query_params.get("replicaSet", [])
    replica_set = replica_set_values[0] if replica_set_values else None

    # Support both ssl= and tls= (MongoDB drivers accept both)
    ssl_values = query_params.get("ssl", []) or query_params.get("tls", [])
    use_ssl = ssl_values[0].lower() == "true" if ssl_values else False

    return {
        "host": parsed.hostname,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
        "auth_source": auth_source,
        "replica_set": replica_set,
        "use_ssl": use_ssl,
    }
