import networkx as nx
from ai_analyzer.preprocessor.graph_builder import build_fk_graph

def run_clustering(fk_relations):
    G = build_fk_graph(fk_relations, directed=False)
    clusters = list(nx.connected_components(G))
    cluster_map = {}
    for i, cluster in enumerate(clusters):
        for table in cluster:
            cluster_map[table] = f"cluster_{i+1}"
    return cluster_map
