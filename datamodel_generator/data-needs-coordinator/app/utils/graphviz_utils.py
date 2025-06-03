# graphviz.py
import graphviz
from typing import Dict, List, Tuple

def create_erd_diagram(entities_data: Dict[str, List[str]], relationships: List[Tuple[str, str, str]] = None) -> graphviz.Digraph:
    """
    Create ERD diagram using GraphViz
    
    Args:
        entities_data: Dictionary of entity names and their attributes
        relationships: List of tuples (entity1, entity2, relationship_type)
    """
    dot = graphviz.Digraph()
    dot.attr(rankdir='LR')
    dot.attr('node', shape='box')
    
    # Add entities
    for entity, attrs in entities_data.items():
        label = f"{entity}\n" + "\n".join(attrs)
        dot.node(entity.lower(), label)
    
    # Add relationships
    if relationships:
        for entity1, entity2, rel_type in relationships:
            dot.edge(entity1.lower(), entity2.lower(), rel_type)
    
    return dot

def parse_relationship_notation(input_str: str) -> Tuple[Dict[str, List[str]], List[Tuple[str, str, str]]]:
    """
    Parse input string into entities and relationships
    Example: "Student[1--*]Course" -> ("Student", "Course", "1:N")
    """
    entities_data = {}
    relationships = []
    
    # Split by comma
    entity_pairs = [pair.strip() for pair in input_str.split(",")]
    
    for pair in entity_pairs:
        if "[" in pair and "]" in pair:
            # Parse relationship notation
            parts = pair.split("[")
            entity1 = parts[0].strip()
            rel_and_entity2 = parts[1].split("]")
            rel_type = rel_and_entity2[0].replace("--*", ":N").replace("--1", ":1")
            entity2 = rel_and_entity2[1].strip()
            
            # Add entities with default attributes
            entities_data[entity1] = ["id", "naam"]
            entities_data[entity2] = ["id", "naam"]
            
            # Add relationship
            relationships.append((entity1, entity2, rel_type))
        else:
            # Single entity without relationship
            entity = pair.strip()
            entities_data[entity] = ["id", "naam"]
    
    return entities_data, relationships