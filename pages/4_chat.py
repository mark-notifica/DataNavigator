"""
Chat - Search and explore the catalog using natural language.
"""

import streamlit as st
from vector_store import search, sync_to_chroma, get_stats

st.set_page_config(
    page_title="Chat - DataNavigator",
    page_icon="💬",
    layout="wide"
)

st.title("💬 Catalog Chat")
st.markdown("Search your data catalog using natural language")

st.divider()

# Sidebar with vector store controls
with st.sidebar:
    st.header("Vector Store")

    stats = get_stats()
    st.metric("Indexed Items", stats['total_vectors'])

    if st.button("Sync to Vector Store", use_container_width=True):
        with st.spinner("Syncing catalog to vector store..."):
            result = sync_to_chroma(only_with_descriptions=True)
            st.success(result['message'])
            st.rerun()

    st.divider()

    st.subheader("Search Options")

    object_type_filter = st.multiselect(
        "Filter by type",
        ["DB_SERVER", "DB_DATABASE", "DB_SCHEMA", "DB_TABLE", "DB_VIEW", "DB_COLUMN"],
        default=[],
        help="Leave empty to search all types"
    )

    num_results = st.slider("Number of results", 5, 50, 10)

# Main search interface
if stats['total_vectors'] == 0:
    st.warning("No items in vector store yet. Click 'Sync to Vector Store' to index your catalog descriptions.")
else:
    # Search input
    query = st.text_input(
        "What are you looking for?",
        placeholder="e.g., 'customer information', 'order status', 'price calculations'",
        key="search_query"
    )

    if query:
        with st.spinner("Searching..."):
            results = search(
                query,
                n_results=num_results,
                object_types=object_type_filter if object_type_filter else None
            )

        if results:
            st.subheader(f"Found {len(results)} results")

            for i, result in enumerate(results):
                score = result.get('score')
                score_pct = f"{score * 100:.0f}%" if score else "N/A"

                obj_type = result['object_type'].replace('DB_', '').lower()
                icon = {
                    'server': '🖥️',
                    'database': '🗄️',
                    'schema': '📁',
                    'table': '📋',
                    'view': '👁️',
                    'column': '📊'
                }.get(obj_type, '📄')

                with st.container():
                    col1, col2 = st.columns([1, 9])

                    with col1:
                        st.markdown(f"### {icon}")
                        st.caption(f"{score_pct}")

                    with col2:
                        st.markdown(f"**{result['qualified_name']}**")

                        if result.get('data_type'):
                            st.caption(f"Type: `{result['data_type']}`")

                        if result.get('description'):
                            st.markdown(result['description'])
                        else:
                            st.caption("_No description_")

                    st.divider()
        else:
            st.info("No results found. Try different search terms.")

# Example queries
with st.expander("Example searches"):
    st.markdown("""
    Try searching for:
    - **Business concepts**: "customer data", "order information", "product catalog"
    - **Technical terms**: "primary key", "timestamp", "status code"
    - **Data types**: "text fields", "numeric values", "date columns"

    The search uses semantic similarity, so you don't need exact matches.
    """)

# Footer
st.divider()
st.caption("Tip: Add descriptions to your catalog items for better search results. Use Bulk Operations to generate descriptions with AI.")
