import networkx as nx
from data_catalog.ai_analyzer.preprocessor.graph_builder import build_fk_graph

def compute_centrality_scores(fk_relations: list[dict]) -> dict:
    """
    Berekent centrale scores op basis van een foreign key-graaf.

    Parameters:
    - fk_relations: lijst van dicts met 'from_table' en 'to_table'

    Returns:
    - dict met in_degree, out_degree, pagerank, betweenness
    """
    G = build_fk_graph(fk_relations, directed=True)

    return {
        "in_degree": dict(G.in_degree()),
        "out_degree": dict(G.out_degree()),
        "pagerank": nx.pagerank(G),
        "betweenness": nx.betweenness_centrality(G)
    }