from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Optional, Any, Dict, List
import os


class BaseDatabaseClient(ABC):
    """Abstract base class for database clients with common patterns."""

    def __init__(self):
        """Initialize with connection parameters."""
        self.connection_params = self._build_connection_params()
        self._engine = None

    @abstractmethod
    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from environment or config."""
        pass

    @abstractmethod
    def _create_connection(self):
        """Create a new database connection."""
        pass

    @abstractmethod
    def get_dlt_destination(self):
        """Get DLT destination for this database."""
        pass

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = self._create_connection()
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def _execute_query(self, query: str, params: Optional[tuple] = None):
        """Context manager for query execution with cursor management."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                yield cursor
            finally:
                cursor.close()

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query and return the first result."""
        try:
            with self._execute_query(query, params) as cursor:
                return cursor.fetchone()
        except Exception:
            return None

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> List[Any]:
        """Execute a query and return all results."""
        with self._execute_query(query, params) as cursor:
            return cursor.fetchall()

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """Execute a query without returning results (INSERT, UPDATE, DELETE)."""
        with self._execute_query(query, params) as cursor:
            cursor.connection.commit()

    @staticmethod
    def _get_env_var(key: str, default: str = None) -> str:
        """Helper method to get environment variables."""
        return os.getenv(key, default)