"""
Help - Documentation and workflow guide.
"""

import streamlit as st

st.set_page_config(
    page_title="Help - DataNavigator",
    page_icon="❓",
    layout="wide"
)

st.title("❓ Help")
st.markdown("Documentation and workflow guide for DataNavigator")

st.divider()

# Workflow
st.header("Workflow")

st.markdown("""
DataNavigator helps you build and maintain a data catalog with descriptions for your database objects.
The typical workflow consists of three main steps:
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    ### Step 1: Extract
    **Page: Run Cataloger**

    Extract metadata from your PostgreSQL databases:
    - Schemas
    - Tables and Views
    - Columns with data types

    The cataloger tracks changes between runs, marking new, updated, and deleted items.
    """)

with col2:
    st.markdown("""
    ### Step 2: Browse & Edit
    **Page: Catalog**

    Explore your catalog and add descriptions:
    - Navigate by server/database/schema
    - View table structures
    - Edit descriptions at any level

    Descriptions help teams understand your data assets.
    """)

with col3:
    st.markdown("""
    ### Step 3: Bulk Enrich
    **Page: Bulk Operations**

    Use AI to generate descriptions at scale:
    1. Export items without descriptions
    2. Send CSV to AI (ChatGPT, Claude, etc.)
    3. Import the enriched descriptions

    Much faster than manual editing!
    """)

st.divider()

# Detailed documentation
st.header("Page Documentation")

# Run Cataloger
with st.expander("Run Cataloger", expanded=False):
    st.markdown("""
    ### Purpose
    Extract database metadata and store it in the catalog.

    ### Required Fields
    - **Server Name**: Logical identifier for the server (e.g., "VPS2", "Production")
    - **Database Name**: The database to catalog
    - **Host**: Connection hostname or IP
    - **Username/Password**: Database credentials

    ### Optional Fields
    - **Server Alias**: Friendly name (e.g., "Development Server")
    - **IP Address**: For documentation purposes
    - **Port**: Defaults to 5432 for PostgreSQL

    ### What Gets Extracted
    - All schemas (excluding system schemas like pg_catalog)
    - All tables and views
    - All columns with data types and nullability
    - View definitions (DDL)

    ### Command Line Alternative
    ```bash
    python run_db_catalog.py \\
        --server VPS2 \\
        --database mydb \\
        --host localhost \\
        --port 5432 \\
        --user postgres \\
        --password secret \\
        --alias "Dev Server"
    ```
    """)

# Catalog
with st.expander("Catalog", expanded=False):
    st.markdown("""
    ### Purpose
    Browse the catalog and edit descriptions for database objects.

    ### Navigation
    1. Select a **Server** from the dropdown
    2. Select a **Database**
    3. Optionally filter by **Schema**
    4. Use the **search box** to find specific tables
    5. Select a **Table** to view its details

    ### Editing Descriptions
    You can edit descriptions at multiple levels:
    - **Server Description**: General info about the server
    - **Database Description**: What this database contains
    - **Schema Description**: Purpose of the schema (shown when schema filter is active)
    - **Table Description**: What data this table holds
    - **Column Description**: Meaning of individual columns

    ### Tips
    - Descriptions are saved immediately when you click "Save"
    - Use clear, concise language
    - Include business context, not just technical details
    - Mention relationships to other tables where relevant
    """)

# Bulk Operations
with st.expander("Bulk Operations", expanded=False):
    st.markdown("""
    ### Purpose
    Export catalog items for AI enrichment, then import the generated descriptions.

    ### Export Tab

    **Filters:**
    - **Server/Database**: Limit export to specific scope
    - **Include already described**: Toggle to re-export items with existing descriptions
    - **Object types**: Select which types to export (servers, databases, schemas, tables, views, columns)

    **Output:**
    CSV file with semicolon (`;`) delimiter containing:
    - `node_id`: Unique identifier (don't modify)
    - `object_type`: Type of object
    - `qualified_name`: Full path (server/database/schema/table/column)
    - `data_type`: For columns only
    - `current_description`: Existing description
    - `new_description`: **Fill this column** with AI-generated descriptions

    ### Import Tab

    1. **Upload** your CSV file with the `new_description` column filled
    2. **Dry Run**: Preview what will be updated (recommended first step)
    3. **Import Now**: Apply the descriptions to the catalog

    ### AI Prompt Example

    When sending the CSV to an AI, you can use a prompt like:

    > "I have a CSV export from my data catalog. Please analyze the qualified_name and data_type columns
    > and generate clear, concise descriptions for each item. Fill in the new_description column.
    > Focus on business meaning, not just technical details. Keep descriptions under 200 characters."

    ### Tips
    - Always do a **Dry Run** before importing
    - Export small batches for better AI results
    - Review AI-generated descriptions before importing
    - Imported descriptions are marked as 'ai_generated' in the status field
    """)

st.divider()

# Technical info
st.header("Technical Information")

with st.expander("Database Schema", expanded=False):
    st.markdown("""
    ### Catalog Structure

    The catalog uses a node-based structure stored in PostgreSQL:

    **Main Table: `catalog.nodes`**
    - `node_id`: Primary key
    - `object_type_code`: DB_SERVER, DB_DATABASE, DB_SCHEMA, DB_TABLE, DB_VIEW, DB_COLUMN
    - `name`: Object name
    - `qualified_name`: Full path (e.g., "VPS2/mydb/public/users/email")
    - `description`: User-provided or AI-generated description
    - `description_status`: draft, ai_generated, approved

    **Detail Tables:**
    - `catalog.node_server`: Server-specific info (alias, IP, type)
    - `catalog.node_database`: Database-server relationship
    - `catalog.node_schema`: Schema-database relationship
    - `catalog.node_table`: Table info (type, view DDL)
    - `catalog.node_column`: Column info (data type, nullability)

    ### Run Tracking

    Each cataloger run is tracked in `catalog.catalog_runs`:
    - Timestamp and duration
    - Counts of created, updated, deleted nodes
    - Error messages if any

    Nodes track which run created them and when they were last seen, enabling detection of deleted objects.
    """)

with st.expander("Configuration", expanded=False):
    st.markdown("""
    ### Environment Variables

    Create a `.env` file with your catalog database connection:

    ```
    CATALOG_DB_HOST=localhost
    CATALOG_DB_PORT=5432
    CATALOG_DB_NAME=catalog
    CATALOG_DB_USER=catalog_user
    CATALOG_DB_PASSWORD=your_password
    ```

    ### Running the App

    ```bash
    # Activate virtual environment
    ./venv/Scripts/activate  # Windows
    source venv/bin/activate  # Linux/Mac

    # Run Streamlit
    streamlit run app.py
    ```

    ### Project Structure

    ```
    DataNavigator/
    ├── app.py                 # Home page
    ├── pages/
    │   ├── 1_run_cataloger.py # Cataloger UI
    │   ├── 2_catalog.py       # Catalog browser
    │   ├── 3_bulk_operations.py # Export/Import
    │   └── 99_help.py         # This page
    ├── run_db_catalog.py      # CLI cataloger
    ├── extractor_db_postgres.py # PostgreSQL metadata extractor
    ├── storage.py             # Catalog database operations
    ├── catalog_export.py      # Export/import functions
    └── connection_db_postgres.py # Database connections
    ```
    """)

st.divider()

st.caption("DataNavigator v1 - A simple data catalog for PostgreSQL databases")
