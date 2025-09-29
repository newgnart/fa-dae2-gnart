import snowflake.connector
import os
from contextlib import contextmanager
from typing import Optional, Dict, Any


class SnowflakeClient:
    """Reusable Snowflake client with connection management."""

    def __init__(self):
        """Initialize Snowflake client with environment configuration."""
        self.connection_params = self._build_connection_params()

    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from environment variables."""
        private_key_pwd = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD")

        params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }

        # Only include password if it's not empty
        if private_key_pwd and private_key_pwd.strip():
            params["private_key_file_pwd"] = private_key_pwd

        return params

    def get_connection(self):
        """Get a new Snowflake connection."""
        return snowflake.connector.connect(**self.connection_params)

    @contextmanager
    def connection(self):
        """Context manager for Snowflake connections."""
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def cursor(self):
        """Context manager for Snowflake cursor."""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a single query and return results."""
        with self.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def execute_command(self, command: str, params: Optional[tuple] = None) -> bool:
        """Execute a command and return success status."""
        try:
            with self.cursor() as cursor:
                if params:
                    cursor.execute(command, params)
                else:
                    cursor.execute(command)
                return True
        except Exception as e:
            print(f"❌ Command failed: {e}")
            return False


def get_snowflake_client() -> SnowflakeClient:
    """Factory function to get a Snowflake client instance."""
    return SnowflakeClient()


# def create_schema_if_not_exists(schema_name: str, database_name: Optional[str] = None) -> bool:
#     """Create schema if it doesn't exist."""
#     client = get_snowflake_client()
#     db_name = database_name or os.getenv("SNOWFLAKE_DATABASE")
#     return client.execute_command(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}")


# def create_stage_if_not_exists(stage_name: str, file_format: str = "CSV",
#                               schema_name: Optional[str] = None) -> bool:
#     """Create stage if it doesn't exist."""
#     client = get_snowflake_client()

#     if schema_name:
#         with client.cursor() as cursor:
#             cursor.execute(f"USE SCHEMA {schema_name}")

#     format_clause = f"FILE_FORMAT = (TYPE = '{file_format}')" if file_format != "CSV" else ""
#     command = f"CREATE STAGE IF NOT EXISTS {stage_name} {format_clause}"

#     return client.execute_command(command)
