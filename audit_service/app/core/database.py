"""Sync SQLAlchemy engine for the audit service.

The consumer thread is synchronous, so we use pymysql (not aiomysql).
"""

from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from audit_service.app.core.config import settings

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """Lazy-initialised singleton engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=2,
        )
    return _engine
