#!/usr/bin/env python3
"""
Simple database connection test for Week 02 Lab
"""

import psycopg
from dotenv import load_dotenv

load_dotenv()
from capstone_package.utils import PostgresClient, SnowflakeClient

# Load environment variables


def test_postgres_connection():
    """Test database connection and return True if successful."""
    try:
        params = PostgresClient.from_env().get_connection_params()
        print(f"🔌 Connecting to PostgreSQL at {params['host']}:{params['port']}...")

        with psycopg.connect(**params) as conn:
            with conn.cursor() as cur:
                # Test basic query
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                print("✅ Connected successfully!")
                print(f"📊 PostgreSQL version: {version.split(',')[0]}")

                return True

    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        print("💡 Make sure PostgreSQL container is running with: docker-compose up -d")
        return False


def test_snowflake_connection():
    """Get Snowflake connection using private key authentication."""
    load_dotenv()

    try:
        conn = SnowflakeClient().connection()
        print("✅ Snowflake Connected successfully!")
        return True
    except Exception as e:
        print(f"Snowflake Connection failed: {e}")
        return False


if __name__ == "__main__":
    success = test_postgres_connection()
    if not success:
        print("❌ PostgreSQL connection failed")
        exit(1)
    success = test_snowflake_connection()
    if not success:
        print("❌ Snowflake connection failed")
        exit(1)
