#!/usr/bin/env python3
"""
Simple CRUD operations demo for Week 02 Lab
"""

import os

import psycopg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_connection():
    """Get database connection."""
    params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }
    return psycopg.connect(**params)


def demonstrate_crud():
    """Demonstrate basic CRUD operations."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                print("📝 Creating sample data...")

                # CREATE - Insert sample data
                sample_data = [
                    ("Sample JSON data", "users.json"),
                    ("Sample CSV data", "products.csv"),
                    ("Sample log data", "app.log"),
                ]

                for content, filename in sample_data:
                    cur.execute(
                        "INSERT INTO staging.raw_data (data_content, file_name) VALUES (%s, %s) RETURNING id",
                        (content, filename),
                    )
                    record_id = cur.fetchone()[0]
                    print(f"   ✅ Created record {record_id}: {filename}")

                # READ - Query all records
                print("\n📖 Reading all records...")
                cur.execute(
                    "SELECT id, data_content, file_name, loaded_at FROM staging.raw_data ORDER BY id"
                )
                records = cur.fetchall()

                for record in records:
                    print(
                        f"   📋 ID: {record[0]}, File: {record[2]}, Content: {record[1][:30]}..."
                    )

                # UPDATE - Modify a record
                print("\n✏️ Updating first record...")
                cur.execute(
                    "UPDATE staging.raw_data SET data_content = %s WHERE id = %s",
                    ("Updated JSON data with new content", records[0][0]),
                )
                print(f"   ✅ Updated record {records[0][0]}")

                # DELETE - Remove last record
                print("\n🗑️ Deleting last record...")
                cur.execute(
                    "DELETE FROM staging.raw_data WHERE id = %s", (records[-1][0],)
                )
                print(f"   ✅ Deleted record {records[-1][0]}")

                # Final count
                cur.execute("SELECT COUNT(*) FROM staging.raw_data")
                final_count = cur.fetchone()[0]
                print(f"\n📊 Final record count: {final_count}")

                return True

    except Exception as e:
        print(f"❌ CRUD operations failed: {str(e)}")
        return False


def main():
    """Main function for standalone execution."""
    return demonstrate_crud()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
