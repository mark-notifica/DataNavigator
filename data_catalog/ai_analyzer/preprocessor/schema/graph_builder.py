import networkx as nx
from ai_analyzer.utils.ai_config import table_is_allowed_by_config
from rapidfuzz.fuzz import ratio



def fetch_column_classifications(db_cursor) -> dict:
    """
    Haalt AI-classificaties op per column_id uit de catalogus.

    Returns:
        Dict[int, str] → {column_id: classification}
    """
    query = """
        SELECT
            column_id,
            summary_json ->> 'classification' AS classification
        FROM catalog.catalog_ai_analysis_results
        WHERE analysis_type = 'column_classification'
          AND status = 'ok'
          AND column_id IS NOT NULL
    """
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    return {
        int(row["column_id"]): row["classification"]
        for row in rows if row["classification"] is not None
    }

def generate_graph_relationships(
    db_cursor,
    classification_map: dict,
    ai_config: dict = None,
    alias_map: dict = None,  # bijv. {"klant_id": ["customer_id", "client_id"]}
    matching_mode: str = "combined"  # "exact", "fuzzy", "alias", "combined"
) -> list:
    query = """
    SELECT
        column_name,
        table_ids,
        column_ids,
        schema_names,
        table_names
    FROM catalog.catalog_column_occurrences_mv
    """
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    relationships = []

    for row in rows:
        column_name = row["column_name"]
        table_ids = row["table_ids"].split(";")
        column_ids = row["column_ids"].split(";")
        table_names = row.get("table_names", "").split(";")
        schema_names = row.get("schema_names", "").split(";")

        all_tables = [
            {
                "table_id": int(table_ids[i]),
                "column_id": int(column_ids[i]),
                "schema_name": schema_names[i] if i < len(schema_names) else "public",
                "table_name": table_names[i] if i < len(table_names) else f"table_{table_ids[i]}",
                "column_name": column_name
            }
            for i in range(len(table_ids))
        ]

        if ai_config:
            all_tables = [t for t in all_tables if table_is_allowed_by_config(t, ai_config)]

        if len(all_tables) <= 1:
            continue

        for i in range(len(all_tables)):
            for j in range(len(all_tables)):
                if i == j:
                    continue

                src = all_tables[i]
                tgt = all_tables[j]

                # Matching logica
                names_match = src["column_name"] == tgt["column_name"]
                alias_match = alias_map and (
                    src["column_name"] in alias_map.get(tgt["column_name"], []) or
                    tgt["column_name"] in alias_map.get(src["column_name"], [])
                )

                matched = False
                if matching_mode == "exact" and names_match:
                    matched = True
                elif matching_mode == "alias" and alias_match:
                    matched = True
                elif matching_mode == "combined" and (names_match or alias_match):
                    matched = True

                if not matched:
                    continue

                src_cls = classification_map.get(src["column_id"])
                tgt_cls = classification_map.get(tgt["column_id"])

                if src_cls in {"TIMESTAMP", "ATTRIBUTE"} or tgt_cls in {"TIMESTAMP", "ATTRIBUTE"}:
                    continue

                # Nieuwe classificatie-logica
                if {src_cls, tgt_cls} <= {"PRIMARY_KEY", "FOREIGN_KEY", "IDENTIFIER"}:
                    confidence = 0.95
                    rel_type = "fk_semantic"
                elif src_cls == tgt_cls:
                    confidence = 0.75
                    rel_type = "semantic_match"
                else:
                    confidence = 0.6
                    rel_type = "name_match"

                description = f"Relatie via kolommen '{src['column_name']}' ↔ '{tgt['column_name']}'"

                relationships.append({
                    "source_table_id": src["table_id"],
                    "target_table_id": tgt["table_id"],
                    "source_column_id": src["column_id"],
                    "target_column_id": tgt["column_id"],
                    "column_name": src["column_name"],
                    "relationship_type": rel_type,
                    "confidence_score": confidence,
                    "description": description,
                    "source": f"graph_builder:{rel_type}",
                    "schema_name": src["schema_name"],
                    "database_name": ai_config.get("filter_database_name") if ai_config else "UNKNOWN",
                    "server_name": ai_config.get("filter_server_name") if ai_config else "UNKNOWN"
                })

    return relationships

def build_fk_graph(fk_relations, directed=True):
    """
    Bouwt een graph-structuur van table-naar-table relaties uit catalog_table_relationships.

    Parameters:
    - fk_relations: lijst van dicts met 'source_table_id' en 'target_table_id'
    - directed: of de graaf gericht moet zijn (True = DiGraph)

    Returns:
    - NetworkX Graph of DiGraph object
    """
    G = nx.DiGraph() if directed else nx.Graph()

    for rel in fk_relations:
        src = rel["source_table_id"]
        tgt = rel["target_table_id"]
        weight = rel.get("confidence_score", 1.0)
        G.add_edge(src, tgt, weight=weight, type=rel.get("relationship_type", "unknown"))

    return G

def main_graph_build(db_cursor, server_name, database_name, schema_name, schema_preprocessor_run_id=None, ai_config=None):
    deactivate_old_relationships(db_cursor, server_name, database_name, schema_name)

    classification_map = fetch_column_classifications(db_cursor)
    relations = generate_graph_relationships(db_cursor, classification_map, ai_config=ai_config)

    insert_relationships(db_cursor, relations)

def insert_relationships(db_cursor, relationships: list):
    """
    Slaat relaties op in catalog.catalog_table_relationships via UPSERT.
    """

    insert_query = """
    INSERT INTO catalog.catalog_table_relationships (
        server_name,
        database_name,
        schema_name,
        source_table_id,
        source_column_id,
        target_table_id,
        target_column_id,
        column_name,
        relationship_type,
        confidence_score,
        description,
        is_current,
        source,
        date_created,
        date_updated
    ) VALUES (
        %(server_name)s,
        %(database_name)s,
        %(schema_name)s,
        %(source_table_id)s,
        %(source_column_id)s,
        %(target_table_id)s,
        %(target_column_id)s,
        %(column_name)s,
        %(relationship_type)s,
        %(confidence_score)s,
        %(description)s,
        TRUE,
        %(source)s,
        now(),
        now()
    )
    ON CONFLICT (source_table_id, target_table_id, column_name)
    DO UPDATE SET
        confidence_score = EXCLUDED.confidence_score,
        relationship_type = EXCLUDED.relationship_type,
        description = EXCLUDED.description,
        is_current = TRUE,
        date_updated = now()
    """

    for rel in relationships:
        row = {
            "server_name": rel.get("server_name", "UNKNOWN"),
            "database_name": rel.get("database_name", "UNKNOWN"),
            "schema_name": rel.get("schema_name", "UNKNOWN"),
            "source_table_id": rel["source_table_id"],
            "source_column_id": rel.get("source_column_id"),
            "target_table_id": rel["target_table_id"],
            "target_column_id": rel.get("target_column_id"),
            "column_name": rel.get("column_name"),
            "relationship_type": rel.get("relationship_type", "unknown"),
            "confidence_score": rel.get("confidence_score", 1.0),
            "description": rel.get("description", f"Relatie via kolom '{rel.get('column_name', '?')}'"),
            "source": rel.get("source", "graph_builder")
        }
        db_cursor.execute(insert_query, row)

def deactivate_old_relationships(db_cursor, server_name, database_name, schema_name):
    """
    Zet bestaande relaties inactief (is_current = FALSE) voor een specifieke combinatie.
    Dit gebeurt voorafgaand aan het inladen van nieuwe relaties uit graph_builder.
    """

    update_query = """
    UPDATE catalog.catalog_table_relationships
    SET is_current = FALSE,
        date_updated = now()
    WHERE server_name = %(server_name)s
      AND database_name = %(database_name)s
      AND schema_name = %(schema_name)s
      AND is_current = TRUE
    """

    db_cursor.execute(update_query, {
        "server_name": server_name,
        "database_name": database_name,
        "schema_name": schema_name
    })