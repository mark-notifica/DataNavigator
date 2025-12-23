"""
DataNavigator - Home Page
"""

import streamlit as st
from storage import get_catalog_servers, get_catalog_databases, get_catalog_tables_for_database
from connection_db_postgres import get_catalog_connection

st.set_page_config(
    page_title="DataNavigator",
    page_icon="🧭",
    layout="wide"
)

st.title("🧭 DataNavigator")
st.markdown("A simple data catalog for managing database metadata and descriptions")

st.divider()

# Quick stats
try:
    conn = get_catalog_connection()
    cursor = conn.cursor()

    # Get counts
    cursor.execute("""
        SELECT object_type_code, COUNT(*)
        FROM catalog.nodes
        WHERE deleted_at IS NULL
        GROUP BY object_type_code
        ORDER BY object_type_code
    """)
    counts = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("""
        SELECT COUNT(*)
        FROM catalog.nodes
        WHERE deleted_at IS NULL
          AND description IS NOT NULL
          AND description != ''
    """)
    described_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    # Display stats
    st.subheader("Catalog Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Servers", counts.get('DB_SERVER', 0))
    with col2:
        st.metric("Databases", counts.get('DB_DATABASE', 0))
    with col3:
        st.metric("Tables", counts.get('DB_TABLE', 0) + counts.get('DB_VIEW', 0))
    with col4:
        st.metric("Columns", counts.get('DB_COLUMN', 0))

    total = sum(counts.values())
    if total > 0:
        pct = (described_count / total) * 100
        st.progress(pct / 100, text=f"{described_count} of {total} items have descriptions ({pct:.1f}%)")

    st.divider()

    # Server list
    st.subheader("Cataloged Servers")

    servers = get_catalog_servers()
    if servers:
        for server in servers:
            with st.container():
                name = server['name']
                alias = f" ({server['alias']})" if server['alias'] else ""
                desc = server['description'] or "_No description_"

                databases = get_catalog_databases(name)
                db_count = len(databases)

                table_count = 0
                for db in databases:
                    tables = get_catalog_tables_for_database(name, db['name'])
                    table_count += len(tables)

                st.markdown(f"**{name}{alias}** - {db_count} databases, {table_count} tables")
                st.caption(desc)
                st.divider()
    else:
        st.info("No servers cataloged yet. Use **Run Cataloger** to add your first database.")

except Exception as e:
    st.warning(f"Could not load catalog stats: {e}")
    st.info("Make sure the catalog database is accessible and run the cataloger to populate it.")

# Quick Start Workflow
st.subheader("Quick Start")

st.markdown("""
1. **Extract** - Use *Run Cataloger* to extract metadata from your databases
2. **Browse** - Use *Catalog* to explore tables/columns and add descriptions manually
3. **Bulk Edit** - Use *Bulk Operations* to export items, enrich with AI, and import back

See the **Help** page for detailed documentation.
""")

st.divider()

# Navigation help
st.subheader("Pages")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🔄 Run Cataloger")
    st.markdown("Extract metadata from a database into the catalog")

with col2:
    st.markdown("### 📚 Catalog")
    st.markdown("Browse and edit descriptions for tables and columns")

with col3:
    st.markdown("### 🔄 Bulk Operations")
    st.markdown("Export/import descriptions for AI enrichment")
