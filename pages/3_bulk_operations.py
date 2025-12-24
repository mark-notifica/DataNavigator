"""
Bulk Operations Page - Export and Import Catalog Descriptions
Export to CSV for AI enrichment, import back with generated descriptions.
"""

import streamlit as st
from catalog_export import export_for_description, import_descriptions
from storage import get_catalog_servers, get_catalog_databases

st.set_page_config(
    page_title="Bulk Operations - DataNavigator",
    page_icon="🔄",
    layout="wide"
)

st.title("🔄 Bulk Operations")
st.markdown("Export catalog items for AI description generation or manual editing, then import the enriched descriptions back.")

# === SHARED SIDEBAR NAVIGATION ===
st.sidebar.header("Navigation")

servers = get_catalog_servers()

if not servers:
    st.sidebar.warning("No servers in catalog")
    selected_server = None
    selected_database = None
else:
    # Server selection (with "All" option)
    server_options = {"All servers": None}
    for s in servers:
        label = s['name'] if not s['alias'] else f"{s['name']} ({s['alias']})"
        server_options[label] = s['name']

    selected_server_display = st.sidebar.selectbox("Server", list(server_options.keys()))
    selected_server = server_options[selected_server_display]

    # Database selection
    if selected_server:
        databases = get_catalog_databases(selected_server)
        if databases:
            database_options = {"All databases": None}
            database_options.update({d['name']: d['name'] for d in databases})
            selected_database = st.sidebar.selectbox("Database", list(database_options.keys()))
            if selected_database == "All databases":
                selected_database = None
        else:
            selected_database = None
            st.sidebar.warning("No databases found")
    else:
        selected_database = None

# Main tabs
tab1, tab2 = st.tabs(["📥 Export", "📤 Import"])

# === EXPORT TAB ===
with tab1:
    st.header("Export for AI Description Generation")
    st.markdown("""
    Export catalog items to CSV format. You can then:
    1. Send the CSV to an AI to generate descriptions
    2. Fill in the `new_description` column manually or with AI
    3. Import the CSV back using the Import tab
    """)

    st.divider()

    # Show current filter
    if selected_server and selected_database:
        st.info(f"Exporting from: **{selected_server} / {selected_database}**")
    elif selected_server:
        st.info(f"Exporting from: **{selected_server}** (all databases)")
    else:
        st.info("Exporting from: **All servers**")

    # Options
    col1, col2 = st.columns(2)

    with col1:
        include_described = st.checkbox(
            "Include already described items",
            value=True,
            help="If unchecked, only exports items without descriptions"
        )
        include_ddl = st.checkbox(
            "Include View DDL",
            value=True,
            help="Include SQL definition for views - helpful for AI to understand context"
        )

    with col2:
        object_types = st.multiselect(
            "Object types to export",
            ["DB_SERVER", "DB_DATABASE", "DB_SCHEMA", "DB_TABLE", "DB_VIEW", "DB_COLUMN"],
            default=["DB_SERVER", "DB_DATABASE", "DB_SCHEMA", "DB_TABLE", "DB_VIEW", "DB_COLUMN"],
            help="Select which catalog object types to include"
        )

    st.divider()

    # Export button
    if st.button("🚀 Generate Export", type="primary", use_container_width=True):
        if not object_types:
            st.error("Please select at least one object type")
        else:
            with st.spinner("Generating export..."):
                try:
                    csv_content = export_for_description(
                        server_name=selected_server,
                        database_name=selected_database,
                        include_described=include_described,
                        object_types=object_types,
                        include_ddl=include_ddl
                    )

                    row_count = csv_content.count('\n') - 1

                    if row_count == 0:
                        st.warning("No items found matching the selected filters")
                    else:
                        st.success(f"✅ Exported {row_count} items")

                        # Generate filename
                        filename_parts = ["catalog_export"]
                        if selected_server:
                            filename_parts.append(selected_server)
                        if selected_database:
                            filename_parts.append(selected_database)
                        filename = "_".join(filename_parts) + ".csv"

                        # Download button
                        st.download_button(
                            label="📥 Download CSV File",
                            data=csv_content,
                            file_name=filename,
                            mime="text/csv",
                            use_container_width=True
                        )

                        # Preview
                        st.subheader("Preview (first 1000 characters)")
                        st.code(csv_content[:1000], language="csv")

                        if len(csv_content) > 1000:
                            st.info("... (truncated for display)")

                except Exception as e:
                    st.error(f"Error during export: {e}")

# === IMPORT TAB ===
with tab2:
    st.header("Import Descriptions from CSV")
    st.markdown("""
    Upload a CSV file with the `new_description` column filled in.
    - Use **Dry Run** first to preview what will be changed
    - Then use **Import Now** to apply the changes
    """)

    st.divider()

    # Import mode selection
    import_mode = st.radio(
        "Import Mode",
        options=['add_and_update', 'add_only', 'overwrite_all'],
        format_func=lambda x: {
            'add_only': 'Add only - Only add where no description exists',
            'add_and_update': 'Add & Update - Add new and update existing (default)',
            'overwrite_all': 'Overwrite all - Including clearing with [CLEAR]'
        }[x],
        horizontal=True,
        help="Controls how existing descriptions are handled"
    )

    st.divider()

    # File uploader
    uploaded_file = st.file_uploader(
        "Upload CSV file with descriptions",
        type=['csv'],
        help="Upload the CSV file with the 'new_description' column filled in"
    )

    if uploaded_file:
        try:
            csv_content = uploaded_file.read().decode('utf-8')

            st.success(f"File loaded: {uploaded_file.name}")

            # Preview uploaded content
            with st.expander("Preview uploaded file"):
                st.code(csv_content[:1000], language="csv")
                if len(csv_content) > 1000:
                    st.info("... (truncated for display)")

            st.divider()

            # Action buttons
            col1, col2 = st.columns(2)

            with col1:
                if st.button("Dry Run (Preview)", use_container_width=True):
                    with st.spinner("Running dry run..."):
                        try:
                            results = import_descriptions(csv_content, dry_run=True, mode=import_mode)

                            st.subheader("Dry Run Results")

                            # Summary metrics
                            metric_cols = st.columns(4)
                            with metric_cols[0]:
                                st.metric("Add", results['added'])
                            with metric_cols[1]:
                                st.metric("Update", results['updated'])
                            with metric_cols[2]:
                                st.metric("Clear", results['cleared'])
                            with metric_cols[3]:
                                st.metric("Errors", len(results['errors']))

                            # Skipped details
                            total_skipped = results['skipped_empty'] + results['skipped_unchanged'] + results['skipped_mode']
                            if total_skipped > 0:
                                with st.expander(f"Skipped: {total_skipped} items"):
                                    if results['skipped_empty']:
                                        st.write(f"- Empty new_description: {results['skipped_empty']}")
                                    if results['skipped_unchanged']:
                                        st.write(f"- Unchanged (same as current): {results['skipped_unchanged']}")
                                    if results['skipped_mode']:
                                        st.write(f"- Blocked by mode: {results['skipped_mode']}")

                            # Changes preview
                            if results['changes']:
                                with st.expander(f"Preview changes ({len(results['changes'])} items)", expanded=True):
                                    for change in results['changes'][:20]:
                                        change_icon = {'add': '+', 'update': '~', 'clear': '-'}[change['type']]
                                        st.markdown(f"**[{change_icon}]** `{change['qualified_name']}`")
                                        if change['old']:
                                            st.caption(f"  Old: {change['old']}")
                                        st.caption(f"  New: {change['new'] if change['new'] else '(cleared)'}")
                                    if len(results['changes']) > 20:
                                        st.info(f"... and {len(results['changes']) - 20} more changes")

                            if results['errors']:
                                st.error("**Errors found:**")
                                for error in results['errors'][:10]:
                                    st.text(f"- {error}")

                            total_changes = results['added'] + results['updated'] + results['cleared']
                            if total_changes > 0:
                                st.info("Validation passed! You can now use 'Import Now' to apply changes.")
                            else:
                                st.warning("No items to update. Check that 'new_description' column is filled.")

                        except Exception as e:
                            st.error(f"Error during dry run: {e}")

            with col2:
                if st.button("Import Now", type="primary", use_container_width=True):
                    with st.spinner("Importing descriptions..."):
                        try:
                            results = import_descriptions(csv_content, dry_run=False, mode=import_mode)

                            st.subheader("Import Results")

                            # Summary metrics
                            metric_cols = st.columns(4)
                            with metric_cols[0]:
                                st.metric("Added", results['added'])
                            with metric_cols[1]:
                                st.metric("Updated", results['updated'])
                            with metric_cols[2]:
                                st.metric("Cleared", results['cleared'])
                            with metric_cols[3]:
                                st.metric("Errors", len(results['errors']))

                            total_changes = results['added'] + results['updated'] + results['cleared']
                            if total_changes > 0:
                                st.success(f"Successfully processed {total_changes} descriptions!")

                            # Skipped summary
                            total_skipped = results['skipped_empty'] + results['skipped_unchanged'] + results['skipped_mode']
                            if total_skipped > 0:
                                skip_parts = []
                                if results['skipped_empty']:
                                    skip_parts.append(f"{results['skipped_empty']} empty")
                                if results['skipped_unchanged']:
                                    skip_parts.append(f"{results['skipped_unchanged']} unchanged")
                                if results['skipped_mode']:
                                    skip_parts.append(f"{results['skipped_mode']} blocked by mode")
                                st.info(f"Skipped: {', '.join(skip_parts)}")

                            if results['errors']:
                                st.error("**Errors encountered:**")
                                for error in results['errors'][:10]:
                                    st.text(f"- {error}")

                        except Exception as e:
                            st.error(f"Error during import: {e}")

        except Exception as e:
            st.error(f"Error reading file: {e}")
    else:
        st.info("Upload a CSV file to begin import")

# Help section
st.divider()
with st.expander("Help"):
    st.markdown("""
### CSV Format
The CSV file uses semicolon (`;`) as delimiter and has these columns:
- `node_id` - Unique identifier (don't modify)
- `object_type` - Type of object (DB_TABLE, DB_VIEW, DB_COLUMN, etc.)
- `qualified_name` - Full path name
- `data_type` - Data type for columns
- `view_definition` - SQL definition for views (for AI context, ignored on import)
- `current_description` - Existing description (for reference)
- `new_description` - **Fill this column** with your descriptions

### View DDL in Export
When "Include View DDL" is checked, the export includes the SQL definition of views.
This helps AI understand what each view does, making it easier to generate accurate descriptions.
The `view_definition` column is ignored during import - only `new_description` is processed.

### Import Modes
- **Add only**: Only adds descriptions where none exists, never overwrites
- **Add & Update**: Adds new descriptions and updates existing ones (default)
- **Overwrite all**: Same as Add & Update, but also allows clearing with `[CLEAR]`

### Special Values
- Empty `new_description`: Row is skipped (existing description preserved)
- `[CLEAR]`: Removes the description (only works in "Overwrite all" mode)
- Same as current: Automatically skipped (no unnecessary updates)
""")
