"""
Bulk Operations Page - Export and Import Catalog Descriptions
Export to CSV for AI enrichment, import back with generated descriptions.
Also provides direct AI generation and interactive enrichment.
"""

import os

from dotenv import load_dotenv
import streamlit as st

from catalog_export import export_for_description, import_descriptions
from storage import get_catalog_servers, get_catalog_databases, get_catalog_schemas
from ai_enrichment import (
    get_items_for_enrichment,
    generate_description,
    save_description
)

load_dotenv()

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

    # Schema selection (only if a specific database is selected)
    selected_schema = None
    if selected_server and selected_database:
        schemas = get_catalog_schemas(selected_server, selected_database)
        if schemas:
            schema_options = {"All schemas": None}
            schema_options.update({s['name']: s['name'] for s in schemas})
            selected_schema = st.sidebar.selectbox("Schema", list(schema_options.keys()))
            if selected_schema == "All schemas":
                selected_schema = None

# Check for AI availability
anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
ollama_host = os.environ.get('OLLAMA_HOST', '')
ai_available = bool(anthropic_key or ollama_host)

# Determine model choice
if anthropic_key:
    ai_model = "claude"
elif ollama_host:
    ai_model = "ollama"
else:
    ai_model = None

# Main tabs
tab1, tab2, tab3 = st.tabs(["📥 Export", "📤 Import", "🤖 AI Generate"])

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
    if selected_server and selected_database and selected_schema:
        st.info(f"Exporting from: **{selected_server} / {selected_database} / {selected_schema}**")
    elif selected_server and selected_database:
        st.info(f"Exporting from: **{selected_server} / {selected_database}** (all schemas)")
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
                        schema_name=selected_schema,
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
                        if selected_schema:
                            filename_parts.append(selected_schema)
                        filename = "_".join(filename_parts) + ".csv"

                        # Download button - encode with BOM for Excel compatibility
                        csv_bytes = ('\ufeff' + csv_content).encode('utf-8')
                        st.download_button(
                            label="📥 Download CSV File",
                            data=csv_bytes,
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

    # Export help
    st.divider()
    with st.expander("Help: CSV Export"):
        st.markdown("""
**CSV Format** - The exported file uses semicolon (`;`) as delimiter:
- `node_id` - Unique identifier (don't modify)
- `object_type` - Type: DB_TABLE, DB_VIEW, DB_COLUMN, etc.
- `qualified_name` - Full path (server/database/schema/name)
- `data_type` - Column data type (for columns only)
- `view_definition_1/2/3` - View SQL split across 3 columns (long views need multiple cells)
- `current_description` - Existing description
- `new_description` - **Fill this column** with your descriptions

**Workflow:**
1. Export to CSV
2. Open in Excel or send to AI
3. Fill the `new_description` column
4. Import back using the Import tab
""")

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

    # Import help
    st.divider()
    with st.expander("Help: CSV Import"):
        st.markdown("""
**Import Modes:**
- **Add only** - Only fills in items that have no description yet
- **Add & Update** - Adds new descriptions and updates existing ones (most common)
- **Overwrite all** - Same as Add & Update, but also processes `[CLEAR]` values

**Special Values in `new_description`:**
- Empty cell → skipped (no change)
- `[CLEAR]` → removes existing description (only in "Overwrite all" mode)

**Workflow:**
1. Upload your CSV file (must have `node_id` and `new_description` columns)
2. Run **Dry Run** first to preview changes
3. Review the summary (adds, updates, clears, errors)
4. Click **Import Now** to apply changes

**Tips:**
- Always do a dry run first to catch errors
- The `node_id` column must match exactly - don't modify it
- CSV should use semicolon (`;`) as delimiter
""")

# === AI GENERATE TAB ===
with tab3:
    st.header("AI Description Generation")

    if not ai_available:
        st.warning("No AI model configured. Add ANTHROPIC_API_KEY or OLLAMA_HOST to your .env file.")
    else:
        st.markdown(f"Generate descriptions automatically using **{'Claude' if ai_model == 'claude' else 'Ollama'}**.")

        # Show current filter
        filter_desc = []
        if selected_server:
            filter_desc.append(selected_server)
        if selected_database:
            filter_desc.append(selected_database)
        if selected_schema:
            filter_desc.append(selected_schema)

        if filter_desc:
            st.info(f"Scope: **{' / '.join(filter_desc)}**")
        else:
            st.info("Scope: **All servers** (select a server/database in sidebar to narrow down)")

        st.divider()

        # Options
        col1, col2 = st.columns(2)
        with col1:
            ai_object_types = st.multiselect(
                "Object types",
                ["DB_TABLE", "DB_VIEW", "DB_COLUMN"],
                default=["DB_TABLE", "DB_VIEW"],
                key="ai_object_types"
            )
            batch_size = st.slider("Batch size", 5, 50, 10, help="Number of items to process at once")

        with col2:
            include_described_ai = st.checkbox(
                "Include already described",
                value=False,
                help="Re-generate descriptions for items that already have one"
            )

        # Reference context section
        st.divider()
        st.subheader("Reference Context (Optional)")
        st.markdown("Add extra information to help AI generate better descriptions.")

        col1, col2 = st.columns(2)

        with col1:
            # File upload
            uploaded_ref = st.file_uploader(
                "Upload reference file",
                type=['txt', 'csv', 'md'],
                key="ref_file",
                help="Upload a text file with reference information"
            )
            if uploaded_ref:
                try:
                    file_content = uploaded_ref.read().decode('utf-8')
                    st.session_state['reference_context'] = file_content
                    st.success(f"Loaded {len(file_content)} characters")
                except Exception as e:
                    st.error(f"Error reading file: {e}")

        with col2:
            # Manual text input
            manual_context = st.text_area(
                "Or enter text manually",
                value=st.session_state.get('manual_reference', ''),
                height=120,
                key="manual_ref_input",
                help="Paste or type reference information"
            )
            if manual_context:
                st.session_state['reference_context'] = manual_context
                st.session_state['manual_reference'] = manual_context

        # Show preview if context exists
        reference_context = st.session_state.get('reference_context')
        if reference_context:
            with st.expander("Preview reference context"):
                st.text(reference_context[:2000])
                if len(reference_context) > 2000:
                    st.caption(f"... ({len(reference_context)} total characters)")
            if st.button("Clear context", key="clear_ref"):
                st.session_state['reference_context'] = None
                st.session_state['manual_reference'] = ''
                st.rerun()

        st.divider()

        # Get items to process
        if st.button("🔍 Find Items to Describe", use_container_width=True):
            with st.spinner("Loading items..."):
                items = get_items_for_enrichment(
                    server_name=selected_server,
                    database_name=selected_database,
                    schema_name=selected_schema,
                    object_types=ai_object_types,
                    include_described=include_described_ai,
                    limit=batch_size
                )
                st.session_state['ai_items'] = items

        # Show items if loaded
        if 'ai_items' in st.session_state and st.session_state['ai_items']:
            items = st.session_state['ai_items']
            st.success(f"Found {len(items)} items to process")

            # Initialize results storage
            if 'ai_results' not in st.session_state:
                st.session_state['ai_results'] = {}

            # Generate button
            if st.button("🤖 Generate Descriptions", type="primary", use_container_width=True):
                progress = st.progress(0)
                status = st.empty()

                # Get reference context from session state
                ref_ctx = st.session_state.get('reference_context')

                for i, item in enumerate(items):
                    status.text(f"Processing: {item['name']} ({item['object_type']})")

                    try:
                        description = generate_description(
                            item,
                            model=ai_model,
                            reference_context=ref_ctx
                        )
                        st.session_state['ai_results'][item['node_id']] = {
                            'item': item,
                            'description': description,
                            'status': 'pending'
                        }
                    except Exception as e:
                        st.session_state['ai_results'][item['node_id']] = {
                            'item': item,
                            'description': f"Error: {e}",
                            'status': 'error'
                        }

                    progress.progress((i + 1) / len(items))

                status.text("Done!")

            # Show results for review
            if st.session_state.get('ai_results'):
                st.subheader("Review Generated Descriptions")
                st.markdown("Review and save each description, or edit before saving.")

                for node_id, result in list(st.session_state['ai_results'].items()):
                    if result['status'] == 'saved':
                        continue

                    item = result['item']
                    with st.expander(f"**{item['name']}** ({item['object_type'].replace('DB_', '')})", expanded=True):
                        st.caption(item['qualified_name'])

                        if item.get('description'):
                            st.markdown(f"**Current:** {item['description']}")

                        # Editable description
                        new_desc = st.text_area(
                            "Generated description",
                            value=result['description'],
                            key=f"desc_{node_id}",
                            height=100
                        )

                        col1, col2, col3 = st.columns([1, 1, 2])
                        with col1:
                            if st.button("✅ Save", key=f"save_{node_id}"):
                                try:
                                    save_description(node_id, new_desc, source='ai')
                                    st.session_state['ai_results'][node_id]['status'] = 'saved'
                                    st.success("Saved!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        with col2:
                            if st.button("❌ Skip", key=f"skip_{node_id}"):
                                del st.session_state['ai_results'][node_id]
                                st.rerun()

                # Summary
                saved = sum(1 for r in st.session_state['ai_results'].values() if r['status'] == 'saved')
                pending = sum(1 for r in st.session_state['ai_results'].values() if r['status'] == 'pending')
                if saved > 0 or pending > 0:
                    st.divider()
                    st.markdown(f"**Progress:** {saved} saved, {pending} pending review")

                    if pending == 0 and saved > 0:
                        if st.button("🔄 Load Next Batch"):
                            st.session_state['ai_results'] = {}
                            del st.session_state['ai_items']
                            st.rerun()

        elif 'ai_items' in st.session_state:
            st.info("No items found matching the criteria. Try different filters or include already described items.")

    # AI Generate help
    st.divider()
    with st.expander("Help: AI Generate"):
        st.markdown("""
**Batch Generation Workflow:**
1. Select object types and batch size
2. Click **Find Items to Describe** to load items needing descriptions
3. (Optional) Add reference context for better AI output
4. Click **Generate Descriptions** to run AI on all items
5. Review each generated description
6. **Save** to keep, **Skip** to discard, or edit before saving

**Reference Context:**
Provide extra information to help AI generate better descriptions:
- **Upload file** - Upload a text/CSV file with reference data
- **Manual text** - Paste or type reference information directly

The AI will use this context when generating descriptions, making them more accurate and domain-specific.

**Tips:**
- Start with small batches (5-10 items) to verify quality
- Use reference context for domain-specific terminology
- Edit generated descriptions before saving if needed
- Items are saved individually - you can stop anytime
""")

