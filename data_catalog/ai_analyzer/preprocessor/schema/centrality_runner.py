import networkx as nx
from ai_analyzer.preprocessor.graph_builder import build_fk_graph

def run_centrality_analysis(fk_relations):
    G = build_fk_graph(fk_relations, directed=True)
    return {
        "in_degree": dict(G.in_degree()),
        "out_degree": dict(G.out_degree()),
        "pagerank": nx.pagerank(G),
        "betweenness": nx.betweenness_centrality(G)
    }
