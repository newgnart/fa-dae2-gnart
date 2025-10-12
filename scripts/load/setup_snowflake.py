from dotenv import load_dotenv

load_dotenv()
import os
from capstone_package.utils import SnowflakeClient


def setup_schema(
    snowflake_client: SnowflakeClient,
    schemas: list[str],
    database_name: str = os.getenv("SNOWFLAKE_DATABASE"),
):
    """Create required Snowflake schemas"""
    with snowflake_client.cursor() as cursor:
        for schema in schemas:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema}")
            print(f"✅ Created/verified schema: {schema}")


def setup_stage(
    snowflake_client: SnowflakeClient,
    stages: list[str],
    database_name: str = os.getenv("SNOWFLAKE_DATABASE"),
    schema_name: str = os.getenv("SNOWFLAKE_SCHEMA"),
):
    """Create required Snowflake stages"""
    with snowflake_client.cursor() as cursor:
        for stage in stages:

            cursor.execute(
                f"""
                CREATE STAGE IF NOT EXISTS {database_name}.{schema_name}.{stage}
                COMMENT = 'Stage for {stage} file uploads'
            """
            )
            print(f"✅ Created/verified stage: {stage}")


def setup_snowflake():
    """Create required Snowflake objects"""

    try:
        snowflake_client = SnowflakeClient()
        # cursor = snowflake_client.cursor()
        database_name = os.getenv("SNOWFLAKE_DATABASE")

        print(f"Setting up Snowflake objects in database: {database_name}")

        # Setup schemas
        setup_schema(snowflake_client, database_name)

        # Setup stages
        setup_stage(snowflake_client, database_name, schema_name="RAW_DATA")

        return True

    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False


if __name__ == "__main__":
    setup_snowflake()
