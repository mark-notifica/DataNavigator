"""
Vector store for catalog search using ChromaDB.
Stores embeddings of catalog descriptions for semantic search.
"""

import os
import chromadb
from chromadb.utils import embedding_functions
from connection_db_postgres import get_catalog_connection


# ChromaDB persistent storage location
CHROMA_PATH = os.path.join(os.path.dirname(__file__), "chroma_db")

# Use default embedding function (all-MiniLM-L6-v2)
# This runs locally, no API key needed
DEFAULT_EMBEDDING_FUNCTION = embedding_functions.DefaultEmbeddingFunction()


def get_chroma_client():
    """Get ChromaDB client with persistent storage."""
    return chromadb.PersistentClient(path=CHROMA_PATH)


def get_collection():
    """Get or create the catalog collection."""
    client = get_chroma_client()
    return client.get_or_create_collection(
        name="catalog_nodes",
        embedding_function=DEFAULT_EMBEDDING_FUNCTION,
        metadata={"description": "Data catalog node descriptions"}
    )


def build_document_text(node):
    """
    Build the text to embed for a node.
    Combines qualified_name, object_type, data_type, and description.
    """
    parts = []

    # Object type in readable form
    obj_type = node['object_type'].replace('DB_', '').lower()
    parts.append(f"Type: {obj_type}")

    # Qualified name (path)
    parts.append(f"Path: {node['qualified_name']}")

    # Data type for columns
    if node.get('data_type'):
        parts.append(f"Data type: {node['data_type']}")

    # Description (main content)
    if node.get('description'):
        parts.append(f"Description: {node['description']}")

    return "\n".join(parts)


def sync_to_chroma(only_with_descriptions=True):
    """
    Sync catalog nodes to ChromaDB.

    Args:
        only_with_descriptions: If True, only sync nodes that have descriptions

    Returns:
        Dict with sync statistics
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    # Get nodes to sync
    query = """
        SELECT
            n.node_id,
            n.object_type_code,
            n.qualified_name,
            n.description,
            CASE
                WHEN n.object_type_code = 'DB_COLUMN' THEN c.data_type
                ELSE NULL
            END as data_type
        FROM catalog.nodes n
        LEFT JOIN catalog.node_column c ON n.node_id = c.node_id
        WHERE n.deleted_at IS NULL
    """

    if only_with_descriptions:
        query += " AND n.description IS NOT NULL AND n.description != ''"

    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        return {'synced': 0, 'message': 'No nodes to sync'}

    # Prepare data for ChromaDB
    ids = []
    documents = []
    metadatas = []

    for row in rows:
        node_id, obj_type, qual_name, description, data_type = row

        node = {
            'node_id': node_id,
            'object_type': obj_type,
            'qualified_name': qual_name,
            'description': description or '',
            'data_type': data_type
        }

        ids.append(str(node_id))
        documents.append(build_document_text(node))
        metadatas.append({
            'node_id': node_id,
            'object_type': obj_type,
            'qualified_name': qual_name,
            'data_type': data_type or '',
            'description': description or ''
        })

    # Upsert to ChromaDB
    collection = get_collection()
    collection.upsert(
        ids=ids,
        documents=documents,
        metadatas=metadatas
    )

    return {
        'synced': len(ids),
        'message': f'Synced {len(ids)} nodes to vector store'
    }


def search(query, n_results=10, object_types=None):
    """
    Search the catalog using semantic similarity.

    Args:
        query: Search query text
        n_results: Number of results to return
        object_types: Optional list of object types to filter
                     e.g. ['DB_TABLE', 'DB_COLUMN']

    Returns:
        List of matching nodes with scores
    """
    collection = get_collection()

    # Build where filter
    where = None
    if object_types:
        where = {"object_type": {"$in": object_types}}

    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where,
        include=["metadatas", "distances", "documents"]
    )

    # Format results
    matches = []
    if results['ids'] and results['ids'][0]:
        for i, node_id in enumerate(results['ids'][0]):
            metadata = results['metadatas'][0][i]
            distance = results['distances'][0][i] if results['distances'] else None

            matches.append({
                'node_id': int(node_id),
                'object_type': metadata.get('object_type'),
                'qualified_name': metadata.get('qualified_name'),
                'description': metadata.get('description'),
                'data_type': metadata.get('data_type'),
                'score': 1 - distance if distance else None  # Convert distance to similarity
            })

    return matches


def get_stats():
    """Get vector store statistics."""
    collection = get_collection()
    count = collection.count()

    return {
        'total_vectors': count,
        'collection_name': 'catalog_nodes',
        'storage_path': CHROMA_PATH
    }


def clear():
    """Clear all vectors from the store."""
    client = get_chroma_client()
    client.delete_collection("catalog_nodes")
    return {'message': 'Vector store cleared'}


# === CLI ===

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Vector store operations')
    parser.add_argument('command', choices=['sync', 'search', 'stats', 'clear'],
                       help='Command to run')
    parser.add_argument('--query', '-q', help='Search query')
    parser.add_argument('--limit', '-n', type=int, default=10,
                       help='Number of results')
    parser.add_argument('--all', action='store_true',
                       help='Sync all nodes, not just those with descriptions')

    args = parser.parse_args()

    if args.command == 'sync':
        result = sync_to_chroma(only_with_descriptions=not args.all)
        print(result['message'])

    elif args.command == 'search':
        if not args.query:
            print("Error: --query required for search")
        else:
            results = search(args.query, n_results=args.limit)
            print(f"Found {len(results)} results:\n")
            for r in results:
                score = f"{r['score']:.3f}" if r['score'] else "N/A"
                print(f"[{score}] {r['qualified_name']}")
                if r['description']:
                    print(f"        {r['description'][:80]}")
                print()

    elif args.command == 'stats':
        stats = get_stats()
        print(f"Total vectors: {stats['total_vectors']}")
        print(f"Collection: {stats['collection_name']}")
        print(f"Storage: {stats['storage_path']}")

    elif args.command == 'clear':
        result = clear()
        print(result['message'])
