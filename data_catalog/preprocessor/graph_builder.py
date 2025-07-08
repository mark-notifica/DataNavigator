import networkx as nx

def build_fk_graph(fk_relations, directed=True):
    """
    Bouwt een graph-structuur van foreign key-relaties.

    Parameters:
    - fk_relations: lijst van dictionaries met 'from_table' en 'to_table'
    - directed: of de graaf gericht moet zijn (True = DiGraph)

    Returns:
    - NetworkX Graph of DiGraph object
    """
    G = nx.DiGraph() if directed else nx.Graph()
    for fk in fk_relations:
        G.add_edge(fk["from_table"], fk["to_table"])
    return G
