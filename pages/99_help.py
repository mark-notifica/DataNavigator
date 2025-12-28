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
DataNavigator helps you build and maintain a documented data catalog. Follow these 5 steps to get started:
""")

st.markdown("""
### Step 1: Extract Metadata
**Page: Run Cataloger**

Connect to your databases and extract metadata:
- Select a configured database source
- Run the cataloger to extract schemas, tables, views, and columns
- Monitor progress in real-time with the progress display
- View run history in the Run History tab
- Clean up stuck runs if needed

The cataloger tracks changes between runs, detecting new, updated, and deleted items.
""")

st.markdown("""
### Step 2: Browse & Document
**Page: Catalog**

Navigate and document your data assets:
- **Browse tab**: Explore the hierarchy (Server > Database > Schema > Table > Column)
- **Batch Edit tab**: Quickly edit descriptions for multiple tables and columns
- **Cleanup tab**: Remove stale objects that no longer exist in the source database

Click the edit icon next to any item to add or update its description.
""")

st.markdown("""
### Step 3: Bulk Enrich with AI
**Page: Bulk Operations**

Generate descriptions at scale using AI:
- **Export tab**: Download tables/columns as CSV for external processing
- **AI Generate tab**: Use built-in AI (Claude or Ollama) to auto-generate descriptions
- **Import tab**: Upload enriched CSV to update the catalog

Filter by server, database, schema, and whether items already have descriptions.
""")

st.markdown("""
### Step 4: Index for Search
**Page: Index**

Enable semantic search by syncing your catalog to a vector database:
- Click "Sync Now" to index all catalog items with descriptions
- The index stores qualified names, object types, data types, and descriptions
- Required before using the Ask page

Re-sync whenever you add or update descriptions.
""")

st.markdown("""
### Step 5: Ask Questions
**Page: Ask**

Chat with your data catalog using natural language:
- Ask questions like "Which tables contain customer information?"
- Find data assets by meaning, not just keywords
- Get contextual answers based on your catalog descriptions

Requires a configured AI model (Claude or Ollama) and a synced vector index.
""")

st.divider()

# Detailed documentation
st.header("Page Documentation")

# Run Cataloger
with st.expander("Run Cataloger", expanded=False):
    st.markdown("""
    ### Purpose
    Extract database metadata and store it in the catalog.

    ### Supported Databases
    - **PostgreSQL**: Full support including schemas, tables, views, columns

    ### How to Use
    1. Select a database source from the dropdown (configured in `.env`)
    2. Click "Run Cataloger" to start extraction
    3. Monitor progress in real-time
    4. View results in the Run History tab

    ### What Gets Extracted
    - All schemas (excluding system schemas)
    - All tables and views
    - All columns with data types and nullability
    - View definitions (DDL)

    ### Run History Tab
    - View all previous cataloger runs
    - See counts of created, updated, and deleted items
    - Clean up stuck runs that didn't complete properly

    ### Tips
    - The cataloger preserves existing descriptions
    - Re-running detects changes (new, updated, deleted objects)
    - Deleted objects are soft-deleted and can be cleaned up in the Catalog page
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

    **Import Modes:**
    - **Add only**: Only add descriptions where none exists (never overwrites)
    - **Add & Update**: Add new and update existing descriptions (default)
    - **Overwrite all**: Same as above, plus allows clearing with `[CLEAR]`

    **Process:**
    1. Select the appropriate **Import Mode**
    2. **Upload** your CSV file with the `new_description` column filled
    3. **Dry Run**: Preview changes (shows old vs new, skipped items, change types)
    4. **Import Now**: Apply the descriptions to the catalog

    **Special Values:**
    - Empty `new_description`: Row is skipped, existing description preserved
    - `[CLEAR]`: Removes the description (only in "Overwrite all" mode)
    - Same as current: Automatically skipped (no unnecessary database updates)

    ### AI Prompt Example

    When sending the CSV to an AI, you can use a prompt like:

    > "I have a CSV export from my data catalog. Please analyze the qualified_name and data_type columns
    > and generate clear, concise descriptions for each item. Fill in the new_description column.
    > Focus on business meaning, not just technical details. Keep descriptions under 200 characters."

    ### Tips
    - Always do a **Dry Run** before importing to preview changes
    - Export small batches for better AI results
    - Review AI-generated descriptions before importing
    - Imported descriptions are marked as 'ai_generated' in the status field
    - Use "Add only" mode to safely add AI descriptions without overwriting manual edits
    """)

# Index
with st.expander("Index", expanded=False):
    st.markdown("""
    ### Purpose
    Sync catalog descriptions to a vector database (ChromaDB) for semantic search.

    ### How It Works
    The index stores each catalog item as a vector embedding:
    - **Qualified name**: The full path (server/database/schema/table/column)
    - **Object type**: table, view, column, etc.
    - **Data type**: For columns
    - **Description**: The main searchable content

    ### Actions
    - **Sync Now**: Update the index with current catalog descriptions
    - **Clear Index**: Remove all items from the index

    ### When to Sync
    - After adding or updating descriptions in the Catalog
    - After importing descriptions via Bulk Operations
    - After running the cataloger (if descriptions were preserved)

    ### Tips
    - Only items with descriptions are indexed
    - Sync is incremental - only changed items are updated
    - The index is stored locally in the `chroma_db` folder
    """)

# Ask
with st.expander("Ask", expanded=False):
    st.markdown("""
    ### Purpose
    Chat with your data catalog using natural language queries.

    ### Prerequisites
    1. **Vector index**: Sync your catalog on the Index page first
    2. **AI model**: Configure Claude (Anthropic) or Ollama in your `.env` file

    ### How It Works
    1. You type a question about your data
    2. The system searches the vector index for relevant catalog items
    3. The AI uses those items as context to answer your question
    4. You see the answer plus the source catalog items

    ### Example Questions
    - "Which tables store customer data?"
    - "What columns contain email addresses?"
    - "Where is order information stored?"
    - "What does the users table contain?"

    ### AI Configuration
    Add one of these to your `.env` file:

    **Claude (Anthropic):**
    ```
    ANTHROPIC_API_KEY=sk-ant-...
    ```

    **Ollama (Local):**
    ```
    OLLAMA_HOST=http://localhost:11434
    ```

    ### Tips
    - More detailed descriptions lead to better answers
    - The AI can only find items that are in the vector index
    - Re-sync the index after updating descriptions
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

st.caption("DataNavigator - A data catalog tool for PostgreSQL databases")
