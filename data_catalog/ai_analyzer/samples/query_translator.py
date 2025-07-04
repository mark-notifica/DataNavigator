from typing import Optional


def map_connection_type_to_engine_type(connection_type: str) -> str:
    """
    Zet de connection_type uit de catalogus om naar een engine_type voor querygeneratie.
    """
    type_map = {
        "PostgreSQL": "postgresql",
        "Azure SQL Server": "sqlserver",
        "SQL Server": "sqlserver",
        "MSSQL": "sqlserver",
        "Power BI Semantic Model": "pbism"  # niet ondersteund voor SQL queries
    }
    engine = type_map.get(connection_type)
    if not engine:
        raise ValueError(f"Connection type '{connection_type}' wordt niet ondersteund voor querygeneratie.")
    return engine


def build_select_sample_query(
    schema: str,
    table: str,
    limit: int,
    engine_type: str,
    random: bool = False
) -> str:
    """
    Genereert een SQL-statement voor het ophalen van sample data.
    """
    if engine_type.lower() == "postgresql":
        order_clause = "ORDER BY RANDOM()" if random else ""
    elif engine_type.lower() in ("sqlserver", "mssql"):
        order_clause = "ORDER BY NEWID()" if random else ""
    else:
        raise ValueError(f"Onbekende of niet-ondersteunde engine: {engine_type}")

    return f"""
SELECT *
FROM "{schema}"."{table}"
{order_clause}
LIMIT {limit}
    """.strip()


def build_table_description_query(schema: str, table: str, engine_type: str, random: bool = False) -> str:
    return build_select_sample_query(schema, table, limit=50, engine_type=engine_type, random=random)


def build_column_classification_query(schema: str, table: str, engine_type: str, random: bool = False) -> str:
    return build_select_sample_query(schema, table, limit=200, engine_type=engine_type, random=random)


def build_data_quality_query(schema: str, table: str, engine_type: str, random: bool = False) -> str:
    return build_select_sample_query(schema, table, limit=500, engine_type=engine_type, random=random)


def build_data_presence_query(schema: str, table: str, engine_type: str, random: bool = False) -> str:
    return build_select_sample_query(schema, table, limit=10, engine_type=engine_type, random=random)


def build_view_definition_query(table_metadata: dict) -> Optional[str]:
    """
    View-definitie komt uit de catalogus (niet dynamisch gegenereerd).
    """
    return table_metadata.get("definition")


def get_query_for_analysis_type(
    analysis_type: str,
    schema: str,
    table: str,
    engine_type: str,
    metadata: Optional[dict] = None,
    random: bool = False
) -> Optional[str]:
    """
    Routed per analysis_type naar juiste querygenerator.
    """
    if analysis_type == "table_description":
        return build_table_description_query(schema, table, engine_type, random)
    elif analysis_type == "column_classification":
        return build_column_classification_query(schema, table, engine_type, random)
    elif analysis_type == "data_quality_check":
        return build_data_quality_query(schema, table, engine_type, random)
    elif analysis_type == "data_presence_analysis":
        return build_data_presence_query(schema, table, engine_type, random)
    elif analysis_type == "view_definition_analysis":
        return build_view_definition_query(metadata or {})
    else:
        raise ValueError(f"Onbekend analysis_type: {analysis_type}")