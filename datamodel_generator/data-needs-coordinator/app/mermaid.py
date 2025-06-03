import streamlit.components.v1 as components
import uuid

# def render_er_diagram(mermaid_syntax: str):
#     """Render Mermaid ERD diagram in Streamlit."""
#     mermaid_html = f"""
#     <div class="mermaid">
#     {mermaid_syntax}
#     </div>
#     <script type="module">
#       import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
#       mermaid.initialize({{ startOnLoad: true }});
#     </script>
#     """
#     components.html(mermaid_html, height=600, scrolling=True)

def render_er_diagram(mermaid_code: str):
    unique_id = f"mermaid-{uuid.uuid4().hex}"
    mermaid_html = f"""
    <div class="mermaid" id="{unique_id}">
    {mermaid_code}
    </div>
    <script type="module">
      import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
      mermaid.initialize({{ startOnLoad: true }});
    </script>
    """
    components.html(mermaid_html, height=600, scrolling=True)

def parse_relationship(rel_string):
    """Convert relationship string to Mermaid syntax."""
    relationship_map = {
        "1:1": "||--||",    # één-op-één
        "1:N": "||--o{",    # één-op-veel
        "N:1": "}o--||",    # veel-op-één
        "N:M": "}o--o{",    # veel-op-veel
    }
    return relationship_map.get(rel_string, "||--o{")  # Default to één-op-veel

def generate_mermaid_diagram(entities_data: dict, relationships: list = None) -> str:
    """Generate Mermaid ERD diagram with entities and relationships."""
    mermaid_code = """
    erDiagram"""
    
    # Add entities with attributes
    for entity, attrs in entities_data.items():
        mermaid_code += f"""
        {entity} {{"""
        for attr in attrs:
            if attr == "id":
                mermaid_code += f"""
            int {attr}"""
            elif any(date_word in attr for date_word in ["datum", "date", "tijd", "time"]):
                mermaid_code += f"""
            date {attr}"""
            else:
                mermaid_code += f"""
            string {attr}"""
        mermaid_code += """
        }"""
    
    # Add relationships
    if relationships:
        for entity1, entity2, rel_type in relationships:
            rel_syntax = parse_relationship(rel_type)
            mermaid_code += f"""
        {entity1} {rel_syntax} {entity2} : "relates" """
    
    return mermaid_code

def parse_relationship_notation(entities_str: str, relationships_str: str = None) -> tuple:
    """Parse input strings into entities and relationships data."""
    # Parse entities
    entities = [e.strip() for e in entities_str.split(",") if e.strip()]
    entities_data = {entity: ["id", "naam"] for entity in entities}
    
    # Parse relationships if provided
    relationships = []
    if relationships_str:
        try:
            relationships = eval(relationships_str)
        except:
            pass
            
    return entities_data, relationships