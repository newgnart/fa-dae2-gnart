from typing import Optional, Any, Dict
from contextlib import contextmanager
import os
import logging

import psycopg2
from sqlalchemy import create_engine
import dlt

logger = logging.getLogger(__name__)


class PostgresClient:
    """Object-oriented PostgreSQL client for database operations."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
    ):
        """
        Initialize PostgresDestination with database configuration.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._engine = None

    @classmethod
    def from_env(cls) -> "PostgresClient":
        """Create from environment variables"""
        return cls(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def get_connection_params(self) -> Dict[str, Any]:
        """Return connection parameters for database clients."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }

    def get_connection_url(self) -> str:
        """Return connection URL for database clients."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_dlt_destination(self) -> Any:
        """Return DLT destination for pipeline operations."""
        return dlt.destinations.postgres(self.get_connection_url())

    @contextmanager
    def get_connection(self):
        """
        Context manager for PostgreSQL database connections.

        Yields:
            psycopg2.connection: Database connection

        Example:
            with destination.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM table")
                result = cursor.fetchone()
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.get_connection_params())
            yield conn
        finally:
            if conn:
                conn.close()

    @property
    def sqlalchemy_engine(self):
        """
        Get SQLAlchemy engine for pandas operations (cached).

        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy engine
        """
        if self._engine is None:
            params = self.get_connection_params()
            connection_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
            self._engine = create_engine(connection_string)
        return self._engine

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        Execute a query and return the first result.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            Query result (fetchone())
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                result = cursor.fetchone()
                cursor.close()
                return result
        except Exception as e:
            logger.warning(f"Failed to query {query} with error {e}, returning None")
            return None

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> list:
        """
        Execute a query and return all results.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            List of query results (fetchall())
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchall()
            cursor.close()
            return result

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Execute a query without returning results (INSERT, UPDATE, DELETE).

        Args:
            query: SQL query string
            params: Query parameters (optional)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            cursor.close()
