from dotenv import load_dotenv

load_dotenv()
import os
from capstone_package.utils import SnowflakeClient


def verify_snowflake_setup():
    """Verify all required Snowflake objects exist."""
    conn = SnowflakeClient().get_connection()
    try:

        cursor = conn.cursor()

        # Check warehouse
        warehouse_name = os.getenv("SNOWFLAKE_WAREHOUSE")
        cursor.execute(f"SHOW WAREHOUSES LIKE '{warehouse_name}'")
        if not cursor.fetchall():
            print(f"❌ Warehouse {warehouse_name} not found")
            return False
        else:
            print(f"✅ Warehouse {warehouse_name} found")

        # Check database
        database_name = os.getenv("SNOWFLAKE_DATABASE")
        cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
        if not cursor.fetchall():
            print(f"❌ Database {database_name} not found")
            return False
        else:
            print(f"✅ Database {database_name} found")

        # Check current schema exists
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database_name}")
        schemas = [row[1] for row in cursor.fetchall()]

        print(f"Available schemas: {schemas}")

    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    verify_snowflake_setup()
