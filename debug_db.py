"""Quick debug to see what's in the database."""

from connection import get_source_connection

conn = get_source_connection()
cursor = conn.cursor()

# 1. List ALL schemas
print("=== SCHEMAS ===")
cursor.execute("""
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
    ORDER BY schema_name;
""")
schemas = cursor.fetchall()
for s in schemas:
    print(f"  - {s[0]}")

# 2. List ALL tables in ALL schemas
print("\n=== ALL TABLES ===")
cursor.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name;
""")
tables = cursor.fetchall()
if tables:
    for t in tables:
        print(f"  - {t[0]}.{t[1]}")
else:
    print("  No tables found!")

# 3. Count tables per schema
print("\n=== TABLES PER SCHEMA ===")
cursor.execute("""
    SELECT table_schema, COUNT(*)
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema')
    GROUP BY table_schema
    ORDER BY table_schema;
""")
counts = cursor.fetchall()
for c in counts:
    print(f"  - {c[0]}: {c[1]} tables")

cursor.close()
conn.close()