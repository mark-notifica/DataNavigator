"""
Index - Manage the vector index for semantic search.
Sync catalog descriptions to ChromaDB for AI-powered search.
"""

import streamlit as st
from vector_store import sync_to_chroma, get_stats, clear

st.set_page_config(
    page_title="Index - DataNavigator",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Vector Index")
st.markdown("Manage the vector index used for semantic search and AI chat")

st.divider()

# Current status
stats = get_stats()

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Indexed Items", stats['total_vectors'])

with col2:
    st.metric("Collection", stats['collection_name'])

with col3:
    st.metric("Storage", "Local (ChromaDB)")

st.divider()

# Sync section
st.header("Sync Catalog to Index")

st.markdown("""
Synchronize catalog descriptions to the vector index. This enables semantic search
and allows the AI chat to find relevant catalog items based on meaning, not just keywords.

**What gets indexed:**
- Qualified name (path): `server/database/schema/table/column`
- Object type: table, view, column, etc.
- Data type: for columns
- Description: the main content for search
""")

col1, col2 = st.columns(2)

with col1:
    include_all = st.checkbox(
        "Include items without descriptions",
        value=False,
        help="If checked, also indexes items that don't have descriptions yet"
    )

with col2:
    pass  # Reserved for future options

if st.button("Sync Now", type="primary", use_container_width=True):
    with st.spinner("Syncing catalog to vector index..."):
        result = sync_to_chroma(only_with_descriptions=not include_all)
        st.success(result['message'])
        st.rerun()

st.divider()

# Test search section
st.header("Test Search")
st.markdown("Test the vector index with a sample query to verify it's working.")

query = st.text_input(
    "Test query",
    placeholder="e.g., 'customer data', 'order status', 'price calculations'"
)

if query:
    from vector_store import search

    with st.spinner("Searching..."):
        results = search(query, n_results=5)

    if results:
        st.subheader(f"Top {len(results)} results")

        for result in results:
            score = result.get('score')
            score_pct = f"{score * 100:.0f}%" if score else "N/A"

            obj_type = result['object_type'].replace('DB_', '').lower()

            st.markdown(f"**{result['qualified_name']}** ({obj_type}) - {score_pct} match")
            if result.get('description'):
                st.caption(result['description'][:200])
            st.divider()
    else:
        st.info("No results found. Try different search terms or sync more items.")

st.divider()

# Maintenance section
st.header("Maintenance")

with st.expander("Clear Index"):
    st.warning("This will delete all vectors from the index. You'll need to sync again.")

    if st.button("Clear Vector Index", type="secondary"):
        result = clear()
        st.success(result['message'])
        st.rerun()

# Info section
with st.expander("About Vector Search"):
    st.markdown("""
    ### How it works

    1. **Embedding**: Each catalog item's text (name, type, description) is converted
       into a numerical vector using a language model (all-MiniLM-L6-v2).

    2. **Storage**: Vectors are stored in ChromaDB, a local vector database.

    3. **Search**: When you search, your query is also converted to a vector,
       and we find catalog items with similar vectors (semantic similarity).

    ### Why use vector search?

    - **Semantic matching**: Find "customer info" even if description says "client data"
    - **Natural language**: Ask questions in plain English
    - **Context-aware**: Understands meaning, not just keywords

    ### Embedding Model

    Currently using: **all-MiniLM-L6-v2**
    - Runs locally (no API needed)
    - 384-dimensional vectors
    - Good balance of quality and speed
    """)
