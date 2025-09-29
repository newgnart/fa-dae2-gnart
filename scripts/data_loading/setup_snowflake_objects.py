from dotenv import load_dotenv

load_dotenv()
import os
from capstone_package.utils import SnowflakeClient


def setup_schema(cursor, database_name):
    """Create required Snowflake schemas"""
    print(f"Setting up schemas in database: {database_name}")

    # Create RAW_DATA schema (required for lab)
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.RAW_DATA")
    print("✅ Created/verified schema: RAW_DATA")

    # Create STAGING schema (mentioned in lab)
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.STAGING")
    print("✅ Created/verified schema: STAGING")

    # Create ANALYTICS schema (mentioned in lab)
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.ANALYTICS")
    print("✅ Created/verified schema: ANALYTICS")


def setup_stage(cursor, database_name, schema_name="RAW_DATA"):
    """Create required Snowflake stages"""
    print(f"Setting up stages in database: {database_name}")

    # Create stages in RAW_DATA schema
    cursor.execute(f"USE SCHEMA {database_name}.{schema_name}")

    # Create CSV_STAGE
    cursor.execute(
        """
        CREATE STAGE IF NOT EXISTS CSV_STAGE
        COMMENT = 'Stage for CSV file uploads'
    """
    )
    print("✅ Created/verified stage: CSV_STAGE")

    # Create JSON_STAGE
    cursor.execute(
        """
        CREATE STAGE IF NOT EXISTS JSON_STAGE
        FILE_FORMAT = (TYPE = 'JSON')
        COMMENT = 'Stage for JSON file uploads'
    """
    )
    print("✅ Created/verified stage: JSON_STAGE")


def setup_snowflake_objects():
    """Create required Snowflake objects"""

    try:
        conn = SnowflakeClient().get_connection()
        cursor = conn.cursor()
        database_name = os.getenv("SNOWFLAKE_DATABASE")

        print(f"Setting up Snowflake objects in database: {database_name}")

        # Setup schemas
        setup_schema(cursor, database_name)

        # Setup stages
        setup_stage(cursor, database_name, schema_name="RAW_DATA")

        return True

    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    setup_snowflake_objects()
