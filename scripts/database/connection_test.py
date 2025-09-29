#!/usr/bin/env python3
"""
Simple database connection test for Week 02 Lab
"""

import os

import psycopg
from dotenv import load_dotenv
from helper import get_connection_params

# Load environment variables
load_dotenv()


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
