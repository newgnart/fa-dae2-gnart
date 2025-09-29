"""
Helper functions for database operations
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
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def get_connection():
    """Get database connection."""
    return psycopg.connect(**get_connection_params())
