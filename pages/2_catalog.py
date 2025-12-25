"""
Catalog Browser - View and edit catalog descriptions.
"""

import streamlit as st
import pandas as pd
from storage import (
    get_catalog_servers,
    get_catalog_databases,
    get_catalog_schemas,
    get_catalog_tables_for_database,
    get_catalog_columns,
    get_table_node_id,
    get_column_node_id,
    update_node_description,
    get_stale_nodes,
    delete_nodes,
    permanently_delete_nodes
)

st.set_page_config(
    page_title="Catalog - DataNavigator",
    page_icon="📚",
    layout="wide"
)

st.title("📚 Catalog")
st.markdown("Browse and edit catalog descriptions")

# === SHARED SIDEBAR NAVIGATION ===
st.sidebar.header("Navigation")

servers = get_catalog_servers()

if not servers:
    st.sidebar.warning("No servers in catalog")
    selected_server = None
    selected_server_display = None
    selected_database = None
    databases = []
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
        else:
            selected_database = None
            st.sidebar.warning("No databases found")
    else:
        databases = []
        selected_database = None
        st.sidebar.info("Select a server to see databases")

# Main tabs
tab_browse, tab_batch, tab_cleanup = st.tabs(["Browse", "📝 Batch Edit", "🧹 Cleanup"])

# === BATCH EDIT TAB ===
with tab_batch:
    st.header("Batch Edit Descriptions")
    st.markdown("Quickly edit descriptions for multiple tables and columns.")

    if not servers:
        st.warning("No servers in catalog. Run extraction first.")
    elif not selected_server:
        st.warning("Select a server in the sidebar.")
    elif not selected_database:
        st.warning("Select a database in the sidebar.")
    else:
        # Get all tables for this database
        all_tables = get_catalog_tables_for_database(selected_server, selected_database)

        if not all_tables:
            st.warning("No tables found.")
        else:
            # Schema filter
            all_schemas = sorted(list(set(t['schema'] for t in all_tables)))

            col1, col2 = st.columns([1, 2])
            with col1:
                batch_schema = st.selectbox(
                    "Schema",
                    ["All schemas"] + all_schemas,
                    key="batch_schema"
                )

            with col2:
                batch_filter = st.text_input("Filter by name", "", key="batch_filter")

            # Filter tables
            filtered_tables = all_tables
            if batch_schema != "All schemas":
                filtered_tables = [t for t in filtered_tables if t['schema'] == batch_schema]
            if batch_filter:
                filtered_tables = [t for t in filtered_tables if batch_filter.lower() in t['table'].lower()]

            # Object type selection
            batch_type = st.radio(
                "Edit",
                ["Tables", "Columns"],
                horizontal=True,
                key="batch_type"
            )

            st.divider()

            if batch_type == "Tables":
                # === BATCH EDIT TABLES ===
                st.subheader(f"Tables ({len(filtered_tables)})")

                if not filtered_tables:
                    st.info("No tables match the filter.")
                else:
                    # Initialize session state for pending changes
                    if 'batch_table_changes' not in st.session_state:
                        st.session_state['batch_table_changes'] = {}

                    # Show tables in editable format
                    for table_info in filtered_tables[:50]:  # Limit to 50 for performance
                        table_key = f"{table_info['schema']}.{table_info['table']}"
                        node_id = get_table_node_id(table_info['schema'], table_info['table'])

                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.markdown(f"**{table_info['table']}**")
                            st.caption(table_info['schema'])
                        with col2:
                            current_desc = table_info.get('description') or ''
                            new_desc = st.text_input(
                                "Description",
                                value=current_desc,
                                key=f"batch_table_{table_key}",
                                label_visibility="collapsed"
                            )
                            # Track changes
                            if new_desc != current_desc:
                                st.session_state['batch_table_changes'][node_id] = {
                                    'name': table_key,
                                    'old': current_desc,
                                    'new': new_desc
                                }
                            elif node_id in st.session_state['batch_table_changes']:
                                del st.session_state['batch_table_changes'][node_id]

                    if len(filtered_tables) > 50:
                        st.info(f"Showing first 50 of {len(filtered_tables)} tables. Use filter to narrow down.")

                    # Save button
                    changes = st.session_state.get('batch_table_changes', {})
                    if changes:
                        st.divider()
                        st.warning(f"**{len(changes)} unsaved changes**")

                        if st.button("💾 Save All Table Changes", type="primary"):
                            saved = 0
                            for node_id, change in changes.items():
                                try:
                                    update_node_description(node_id, change['new'])
                                    saved += 1
                                except Exception as e:
                                    st.error(f"Error saving {change['name']}: {e}")
                            if saved > 0:
                                st.success(f"Saved {saved} descriptions!")
                                st.session_state['batch_table_changes'] = {}
                                st.rerun()

            else:
                # === BATCH EDIT COLUMNS ===
                # Select a table first
                table_options = [f"{t['schema']}.{t['table']}" for t in filtered_tables]

                if not table_options:
                    st.info("No tables match the filter.")
                else:
                    selected_table = st.selectbox(
                        "Select table to edit columns",
                        table_options,
                        key="batch_column_table"
                    )

                    schema, table = selected_table.split('.', 1)
                    columns = get_catalog_columns(schema, table, selected_server, selected_database)

                    st.subheader(f"Columns in {selected_table} ({len(columns)})")

                    if not columns:
                        st.info("No columns found.")
                    else:
                        # Initialize session state for pending changes
                        if 'batch_column_changes' not in st.session_state:
                            st.session_state['batch_column_changes'] = {}

                        # Column filter
                        col_filter = st.text_input("Filter columns", "", key="col_filter")

                        filtered_cols = columns
                        if col_filter:
                            filtered_cols = [c for c in columns if col_filter.lower() in c['column'].lower()]

                        # Show columns in editable format
                        for col_info in filtered_cols:
                            col_name = col_info['column']
                            node_id = col_info.get('node_id') or get_column_node_id(schema, table, col_name)

                            if not node_id:
                                continue  # Skip columns without node_id

                            col_key = f"{schema}.{table}.{col_name}"

                            col1, col2, col3 = st.columns([1, 0.5, 2.5])
                            with col1:
                                st.markdown(f"**{col_name}**")
                            with col2:
                                st.caption(col_info.get('type', ''))
                            with col3:
                                current_desc = col_info.get('description') or ''
                                new_desc = st.text_input(
                                    "Description",
                                    value=current_desc,
                                    key=f"batch_col_{node_id}",  # Use node_id for unique key
                                    label_visibility="collapsed"
                                )
                                # Track changes
                                if new_desc != current_desc:
                                    st.session_state['batch_column_changes'][node_id] = {
                                        'name': col_key,
                                        'old': current_desc,
                                        'new': new_desc
                                    }
                                elif node_id in st.session_state['batch_column_changes']:
                                    del st.session_state['batch_column_changes'][node_id]

                        # Save button
                        changes = st.session_state.get('batch_column_changes', {})
                        if changes:
                            st.divider()
                            st.warning(f"**{len(changes)} unsaved changes**")

                            if st.button("💾 Save All Column Changes", type="primary"):
                                saved = 0
                                for node_id, change in changes.items():
                                    try:
                                        update_node_description(node_id, change['new'])
                                        saved += 1
                                    except Exception as e:
                                        st.error(f"Error saving {change['name']}: {e}")
                                if saved > 0:
                                    st.success(f"Saved {saved} descriptions!")
                                    st.session_state['batch_column_changes'] = {}
                                    st.rerun()

                        # Navigation buttons for quick table switching
                        st.divider()
                        current_idx = table_options.index(selected_table)

                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col1:
                            if current_idx > 0:
                                if st.button("⬅️ Previous table"):
                                    st.session_state['batch_column_table'] = table_options[current_idx - 1]
                                    st.rerun()
                        with col2:
                            st.caption(f"Table {current_idx + 1} of {len(table_options)}")
                        with col3:
                            if current_idx < len(table_options) - 1:
                                if st.button("Next table ➡️"):
                                    st.session_state['batch_column_table'] = table_options[current_idx + 1]
                                    st.rerun()


# === CLEANUP TAB ===
with tab_cleanup:
    st.header("Catalog Cleanup")
    st.markdown("Review and remove nodes that are no longer present in the source database.")

    if not servers:
        st.warning("No servers in catalog. Run extraction first.")
    else:
        # Show current filter
        if selected_server and selected_database:
            st.info(f"Showing stale nodes for: **{selected_server_display} / {selected_database}**")
        elif selected_server:
            st.info(f"Showing stale nodes for: **{selected_server_display}** (all databases)")
        else:
            st.info("Showing stale nodes for: **All servers**")

        # Get stale nodes for selected server/database
        stale_nodes = get_stale_nodes(selected_server, selected_database)

        if not stale_nodes:
            st.success("No stale nodes found! Your catalog is up to date.")
        else:
            st.warning(f"Found {len(stale_nodes)} stale nodes that were not seen in the latest catalog run.")

            # Group by source
            by_source = {}
            for node in stale_nodes:
                source = node['source_label'] or 'Unknown'
                if source not in by_source:
                    by_source[source] = []
                by_source[source].append(node)

            # Display by source
            for source, nodes in by_source.items():
                with st.expander(f"{source} ({len(nodes)} stale nodes)", expanded=True):
                    # Group by object type
                    by_type = {}
                    for node in nodes:
                        otype = node['object_type']
                        if otype not in by_type:
                            by_type[otype] = []
                        by_type[otype].append(node)

                    for otype, type_nodes in sorted(by_type.items()):
                        st.markdown(f"**{otype}** ({len(type_nodes)})")

                        # Show nodes in a table
                        table_data = []
                        for node in type_nodes[:50]:
                            table_data.append({
                                'Name': node['name'],
                                'Last Seen': node['last_seen_at'].strftime('%Y-%m-%d') if node['last_seen_at'] else '-',
                                'Run': node['last_seen_run_id']
                            })
                        st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)

                        if len(type_nodes) > 50:
                            st.info(f"... and {len(type_nodes) - 50} more {otype} nodes")

                    st.divider()

                    # Actions for this source
                    node_ids = [n['node_id'] for n in nodes]
                    source_key = source.replace('/', '_')

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button(f"Soft-delete {len(nodes)} nodes", key=f"soft_{source_key}"):
                            deleted = delete_nodes(node_ids)
                            st.success(f"Soft-deleted {deleted} nodes")
                            st.rerun()

                    with col2:
                        with st.popover("Permanently delete"):
                            st.warning("This will permanently remove these nodes!")
                            if st.button("Confirm deletion", key=f"confirm_{source_key}"):
                                deleted = permanently_delete_nodes(node_ids)
                                st.success(f"Deleted {deleted} nodes")
                                st.rerun()

        # Help
        with st.expander("Help"):
            st.markdown("""
            ### What are stale nodes?

            Stale nodes are catalog entries that were not found during the most recent
            cataloger run for their source database. This usually means the object was
            dropped, renamed, or removed from the source.

            ### Soft-delete vs Permanent delete

            - **Soft-delete**: Marks nodes as deleted. They won't appear in the catalog
              but can be restored if needed.

            - **Permanent delete**: Completely removes nodes from the database.
              This cannot be undone!
            """)

# === BROWSE TAB ===
with tab_browse:
    if not servers:
        st.warning("No servers in catalog. Run extraction first.")
    elif not selected_server:
        st.warning("Select a server in the sidebar to browse tables.")
    elif not selected_database:
        st.warning("Select a database in the sidebar to browse tables.")
    else:
        try:
            st.sidebar.divider()

            # === TABLE SELECTION (Browse-specific) ===
            tables = get_catalog_tables_for_database(selected_server, selected_database)

            if not tables:
                st.warning("No tables in catalog for this database.")
                st.stop()

            # Schema filter
            all_schemas = sorted(list(set(t['schema'] for t in tables)))
            schema_options = ["All schemas"] + all_schemas
            selected_schema = st.sidebar.selectbox("Schema", schema_options)

            if selected_schema != "All schemas":
                tables = [t for t in tables if t['schema'] == selected_schema]

            # Search filter
            search = st.sidebar.text_input("🔍 Filter tables", "")

            if search:
                tables = [t for t in tables if search.lower() in t['table'].lower()
                          or search.lower() in t['schema'].lower()]

            if not tables:
                st.sidebar.warning("No tables match filter")
                st.stop()

            table_options = [f"{t['schema']}.{t['table']}" for t in tables]
            selected = st.sidebar.selectbox("Table", table_options)

            # Parse selected table
            schema, table = selected.split('.')

            # Find table info
            table_info = next(
                (t for t in tables if t['schema'] == schema and t['table'] == table),
                None
            )

            # Main content
            st.header(f"{selected_server_display} / {selected_database}")

            # === SERVER DESCRIPTION EDITING ===
            server_info = next((s for s in servers if s['name'] == selected_server), None)
            server_node_id = server_info['node_id'] if server_info else None
            current_server_desc = server_info['description'] if server_info else ''

            with st.expander("Server Description"):
                new_server_desc = st.text_area(
                    "Description",
                    value=current_server_desc,
                    key=f"server_desc_{selected_server}",
                    height=68
                )
                if st.button("Save Server Description"):
                    if server_node_id:
                        update_node_description(server_node_id, new_server_desc)
                        st.success("Saved!")
                        st.rerun()

            # === DATABASE DESCRIPTION EDITING ===
            db_info = next((d for d in databases if d['name'] == selected_database), None)
            db_node_id = db_info['node_id'] if db_info else None
            current_db_desc = db_info['description'] if db_info else ''

            with st.expander("Database Description"):
                new_db_desc = st.text_area(
                    "Description",
                    value=current_db_desc,
                    key=f"db_desc_{selected_database}",
                    height=68
                )
                if st.button("Save Database Description"):
                    if db_node_id:
                        update_node_description(db_node_id, new_db_desc)
                        st.success("Saved!")
                        st.rerun()

            # === SCHEMA DESCRIPTION EDITING ===
            # Only show if a specific schema is selected
            if selected_schema != "All schemas":
                schemas = get_catalog_schemas(selected_server, selected_database)
                schema_info = next((s for s in schemas if s['name'] == selected_schema), None)
                schema_node_id = schema_info['node_id'] if schema_info else None
                current_schema_desc = schema_info['description'] if schema_info else ''

                with st.expander(f"Schema Description: {selected_schema}"):
                    new_schema_desc = st.text_area(
                        "Description",
                        value=current_schema_desc,
                        key=f"schema_desc_{selected_schema}",
                        height=68
                    )
                    if st.button("Save Schema Description"):
                        if schema_node_id:
                            update_node_description(schema_node_id, new_schema_desc)
                            st.success("Saved!")
                            st.rerun()

            st.subheader(f"Table: {selected}")

            # === TABLE DESCRIPTION EDITING ===
            table_node_id = get_table_node_id(schema, table)
            current_table_desc = table_info['description'] if table_info else ''

            new_table_desc = st.text_area(
                "Table Description",
                value=current_table_desc,
                key=f"table_desc_{schema}_{table}",
                height=80
            )

            if st.button("Save Table Description"):
                if table_node_id:
                    update_node_description(table_node_id, new_table_desc)
                    st.success("Saved!")
                    st.rerun()
                else:
                    st.error("Table not found in catalog")

            st.divider()

            # === COLUMNS SECTION ===
            st.subheader("Columns")

            columns = get_catalog_columns(schema, table, selected_server, selected_database)

            df = pd.DataFrame(columns)
            df = df[['column', 'type', 'nullable', 'description']]  # Reorder and select columns
            df.columns = ['Column', 'Type', 'Nullable', 'Description']
            st.dataframe(df, use_container_width=True)

            st.info(f"{len(columns)} columns")

            # === COLUMN DESCRIPTION EDITING ===
            st.divider()
            st.subheader("Edit Column Descriptions")

            column_names = [c['column'] for c in columns]
            selected_column = st.selectbox("Select column to edit", column_names)

            col_info = next(
                (c for c in columns if c['column'] == selected_column),
                None
            )
            current_col_desc = col_info['description'] if col_info else ''

            new_col_desc = st.text_area(
                f"Description for `{selected_column}`",
                value=current_col_desc,
                key=f"col_desc_{schema}_{table}_{selected_column}",
                height=80
            )

            if st.button("Save Column Description"):
                col_node_id = get_column_node_id(schema, table, selected_column)
                if col_node_id:
                    update_node_description(col_node_id, new_col_desc)
                    st.success("Saved!")
                    st.rerun()
                else:
                    st.error("Column not found in catalog")

        except Exception as e:
            st.error(f"Error: {e}")
            st.info("Check database connection or run catalog extraction first")
