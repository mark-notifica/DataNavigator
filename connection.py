"""
Database connection handler.
TWO connections: Source (to catalog) and Catalog (to store metadata).
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def get_source_connection():
    """
    Connect to the SOURCE database (the one we want to catalog).
    Used for: Reading tables, columns, metadata.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("SOURCE_DB_HOST"),
            port=os.getenv("SOURCE_DB_PORT"),
            database=os.getenv("SOURCE_DB_NAME"),
            user=os.getenv("SOURCE_DB_USER"),
            password=os.getenv("SOURCE_DB_PASSWORD")
        )
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to SOURCE database: {e}")


def get_catalog_connection():
    """
    Connect to the CATALOG database (where we store our metadata).
    Used for: Storing descriptions, saving catalog data.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("CATALOG_DB_HOST"),
            port=os.getenv("CATALOG_DB_PORT"),
            database=os.getenv("CATALOG_DB_NAME"),
            user=os.getenv("CATALOG_DB_USER"),
            password=os.getenv("CATALOG_DB_PASSWORD")
        )
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to CATALOG database: {e}")


def test_connections():
    """Test both database connections."""
    print("Testing SOURCE database connection...")
    try:
        conn = get_source_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"✅ SOURCE connected: {version[0][:50]}...")
    except Exception as e:
        print(f"❌ SOURCE failed: {e}")

    print("\nTesting CATALOG database connection...")
    try:
        conn = get_catalog_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"✅ CATALOG connected: {version[0][:50]}...")
    except Exception as e:
        print(f"❌ CATALOG failed: {e}")


if __name__ == "__main__":
    test_connections()
