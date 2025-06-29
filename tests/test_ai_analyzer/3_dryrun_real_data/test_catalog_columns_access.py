from data_catalog.connection_handler import get_catalog_connection

def check_catalog_columns_exists():
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'catalog'
                  AND table_name = 'catalog_columns'
            """)
            row = cur.fetchone()
            if row:
                print("✅ Tabel catalog.catalog_columns bestaat.")
            else:
                print("❌ Tabel catalog.catalog_columns bestaat NIET.")
    finally:
        conn.close()

if __name__ == "__main__":
    check_catalog_columns_exists()
