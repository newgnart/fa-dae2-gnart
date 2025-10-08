import snowflake.connector
import os
from contextlib import contextmanager
from typing import Optional, Dict, Any
import dlt

from .base_client import BaseDatabaseClient


class SnowflakeClient(BaseDatabaseClient):
    """Reusable Snowflake client with connection management."""

    def __init__(self):
        """Initialize Snowflake client with environment configuration."""
        super().__init__()

    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from environment variables."""
        private_key_pwd = self._get_env_var("SNOWFLAKE_PRIVATE_KEY_FILE_PWD")

        params = {
            "account": self._get_env_var("SNOWFLAKE_ACCOUNT"),
            "user": self._get_env_var("SNOWFLAKE_USER"),
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": self._get_env_var("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            "warehouse": self._get_env_var("SNOWFLAKE_WAREHOUSE"),
            "database": self._get_env_var("SNOWFLAKE_DATABASE"),
            "schema": self._get_env_var("SNOWFLAKE_SCHEMA"),
            "role": self._get_env_var("SNOWFLAKE_ROLE"),
        }

        # Only include password if it's not empty
        if private_key_pwd and private_key_pwd.strip():
            params["private_key_file_pwd"] = private_key_pwd

        return params

    def _create_connection(self):
        """Create a new Snowflake connection."""
        return snowflake.connector.connect(**self.connection_params)


    @contextmanager
    def cursor(self):
        """Context manager for Snowflake cursor."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a single query and return results."""
        return self.fetch_all(query, params)

    def execute_command(self, command: str, params: Optional[tuple] = None) -> bool:
        """Execute a command and return success status."""
        try:
            self.execute(command, params)
            return True
        except Exception as e:
            print(f"❌ Command failed: {e}")
            return False

    def get_dlt_destination(self):
        """Get DLT destination configuration for Snowflake."""

        # Read private key file and convert to private_key for DLT
        private_key_path = os.path.expanduser(
            self.connection_params["private_key_file"]
        )

        try:
            with open(private_key_path, "r") as key_file:
                private_key_data = key_file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Private key file not found: {private_key_path}")

        # Create credentials object that DLT expects
        credentials = {
            "database": self.connection_params["database"],
            "username": self.connection_params["user"],
            "host": self.connection_params["account"],
            "warehouse": self.connection_params["warehouse"],
            "role": self.connection_params["role"],
            "private_key": private_key_data,
        }

        # Only add private key password if it exists
        if self.connection_params.get("private_key_file_pwd"):
            credentials["private_key_passphrase"] = self.connection_params[
                "private_key_file_pwd"
            ]

        return dlt.destinations.snowflake(credentials=credentials)
