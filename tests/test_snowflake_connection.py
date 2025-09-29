import os
import snowflake.connector
from dotenv import load_dotenv
from capstone_package.utils import SnowflakeClient


def get_snowflake_connection():
    """Get Snowflake connection using private key authentication."""
    load_dotenv()

    try:
        conn = SnowflakeClient().get_connection()
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
