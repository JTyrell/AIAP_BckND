"""
AIAP — Database Connection Helper
Provides a graceful MockConnection stub when DB_AVAILABLE=false,
and a real psycopg2 connection when DB_AVAILABLE=true.
"""
import logging
from config import Config

logger = logging.getLogger(__name__)


class MockCursor:
    """Stub cursor that raises descriptive errors on query execution."""

    def execute(self, *args, **kwargs):
        raise ConnectionError(
            "[STUB MODE] Database is not available. "
            "Set DB_AVAILABLE=true and provide DATABASE_URL in .env."
        )

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def close(self):
        pass


class MockConnection:
    """Placeholder connection returned when DB is unavailable."""

    def cursor(self):
        return MockCursor()

    def commit(self):
        pass

    def close(self):
        pass


def get_db_connection():
    """
    Returns a live PostgreSQL connection if DB_AVAILABLE=true.
    Otherwise returns a MockConnection that raises descriptive errors
    on any query execution — preventing silent failures.
    """
    if not Config.DB_AVAILABLE:
        logger.warning(
            "\u26a0\ufe0f DB_UNAVAILABLE: Returning mock connection. "
            "Set DB_AVAILABLE=true and configure DATABASE_URL to use PostgreSQL."
        )
        return MockConnection()

    try:
        import psycopg2
        conn = psycopg2.connect(Config.DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise