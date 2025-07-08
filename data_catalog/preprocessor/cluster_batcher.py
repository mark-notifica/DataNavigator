import networkx as nx

def generate_clusters_for_tables(graph: nx.Graph) -> list[dict]:
    """
    Genereert clusters van tabellen op basis van verbonden componenten.
    Als de graph gericht is, wordt eerst een ongerichte versie gebruikt.

    Returns:
    - Lijst van clusters als dicts met een cluster_id en bijbehorende tabellen
    """
    if graph.is_directed():
        undirected = graph.to_undirected()
    else:
        undirected = graph

    clusters = []
    for i, component in enumerate(nx.connected_components(undirected), start=1):
        clusters.append({
            "cluster_id": f"cluster_{i}",
            "tables": sorted(component)
        })

    return clusters