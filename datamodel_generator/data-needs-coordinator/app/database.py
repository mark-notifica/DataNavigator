import psycopg2
from psycopg2 import sql
import logging
from typing import List, Optional

DB_CONFIG = {
    "host": "localhost",
    "database": "udg",
    "user": "postgres",
    "password": "BefzgrX8ZC76Op",
    "port": "5432"
}

def init_database() -> bool:
    try:
        # Check database
        conn = psycopg2.connect(**{**DB_CONFIG, "database": "postgres"})
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create UDG database if not exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname='udg'")
        if not cursor.fetchone():
            cursor.execute('CREATE DATABASE udg')
        
        cursor.close()
        conn.close()

        # Create table in UDG database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS informatiebehoefte (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            omschrijving TEXT NOT NULL,
            entiteiten TEXT NOT NULL
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        logging.error(f"Database initialization error: {e}")
        return False

def save_information_need(description: str, entities: list, relationships: list = None, user: str = None):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute(
            '''
            INSERT INTO informatiebehoefte 
                (omschrijving, entiteiten, relaties, created_by, created_at, last_modified_by, last_modified_at) 
            VALUES 
                (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP)
            ''',
            (
                description, 
                ",".join(entities),
                str(relationships) if relationships else None,
                user,
                user
            )
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error saving: {e}")
        return False

def get_users():
    """Get list of users for dropdown"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute('SELECT username FROM users ORDER BY username')
        users = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return users
    except Exception as e:
        logging.error(f"Error getting users: {e}")
        return []