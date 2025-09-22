#!/usr/bin/env python3
"""
Simple database connection test for Week 02 Lab
"""

import os

import psycopg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_connection_params():
    """Get database connection parameters from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def test_connection():
    """Test database connection and return True if successful."""
    try:
        params = get_connection_params()
        print(f"🔌 Connecting to PostgreSQL at {params['host']}:{params['port']}...")

        with psycopg.connect(**params) as conn:
            with conn.cursor() as cur:
                # Test basic query
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                print("✅ Connected successfully!")
                print(f"📊 PostgreSQL version: {version.split(',')[0]}")

                # Test staging schema access
                cur.execute("SELECT COUNT(*) FROM staging.raw_data")
                count = cur.fetchone()[0]
                print(f"📋 Staging table has {count} records")

                return True

    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        print("💡 Make sure PostgreSQL container is running with: docker-compose up -d")
        return False


def main():
    """Main function for standalone execution."""
    return test_connection()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
