from data_catalog.connection_handler import get_catalog_connection

conn = get_catalog_connection()
with conn.cursor() as cur:
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'catalog'")
    print(cur.fetchall())
