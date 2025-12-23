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
st.markdown("Export catalog items for AI description generation, then import the enriched descriptions back.")

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

    # Filters
    col1, col2 = st.columns(2)

    with col1:
        servers = get_catalog_servers()
        server_options = ["All servers"] + [s['name'] for s in servers]
        selected_server = st.selectbox("🖥️ Server", server_options)

    with col2:
        if selected_server != "All servers":
            databases = get_catalog_databases(selected_server)
            database_options = ["All databases"] + [d['name'] for d in databases]
            selected_database = st.selectbox("🗄️ Database", database_options)
        else:
            selected_database = "All databases"
            st.selectbox("🗄️ Database", ["All databases"], disabled=True)

    # Options
    col3, col4 = st.columns(2)

    with col3:
        include_described = st.checkbox(
            "Include already described items",
            value=False,
            help="If unchecked, only exports items without descriptions"
        )

    with col4:
        object_types = st.multiselect(
            "Object types to export",
            ["DB_SERVER", "DB_DATABASE", "DB_SCHEMA", "DB_TABLE", "DB_VIEW", "DB_COLUMN"],
            default=["DB_TABLE", "DB_VIEW", "DB_COLUMN"],
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
                        server_name=None if selected_server == "All servers" else selected_server,
                        database_name=None if selected_database == "All databases" else selected_database,
                        include_described=include_described,
                        object_types=object_types
                    )

                    row_count = csv_content.count('\n') - 1

                    if row_count == 0:
                        st.warning("No items found matching the selected filters")
                    else:
                        st.success(f"✅ Exported {row_count} items")

                        # Generate filename
                        filename_parts = ["catalog_export"]
                        if selected_server != "All servers":
                            filename_parts.append(selected_server)
                        if selected_database != "All databases":
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

# Footer
st.divider()
st.markdown("""
### CSV Format
The CSV file uses semicolon (`;`) as delimiter and has these columns:
- `node_id` - Unique identifier (don't modify)
- `object_type` - Type of object (DB_TABLE, DB_VIEW, DB_COLUMN, etc.)
- `qualified_name` - Full path name
- `data_type` - Data type for columns
- `current_description` - Existing description (for reference)
- `new_description` - **Fill this column** with your descriptions

### Import Modes
- **Add only**: Only adds descriptions where none exists, never overwrites
- **Add & Update**: Adds new descriptions and updates existing ones (default)
- **Overwrite all**: Same as Add & Update, but also allows clearing with `[CLEAR]`

### Special Values
- Empty `new_description`: Row is skipped (existing description preserved)
- `[CLEAR]`: Removes the description (only works in "Overwrite all" mode)
- Same as current: Automatically skipped (no unnecessary updates)
""")
