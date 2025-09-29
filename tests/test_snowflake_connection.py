import os
import snowflake.connector
from dotenv import load_dotenv


def get_snowflake_connection():
    """Get Snowflake connection using private key authentication."""
    load_dotenv()

    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="SNOWFLAKE_JWT",
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        return conn
    except Exception as e:
        print(f"Connection failed: {e}")
        return None


# Test connection
if __name__ == "__main__":
    conn = get_snowflake_connection()
    if conn:
        print("✅ Snowflake connection successful!")
        conn.close()
    else:
        print("❌ Snowflake connection failed!")
