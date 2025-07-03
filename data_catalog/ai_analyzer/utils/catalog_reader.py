import re
from typing import List, Dict, Optional
from data_catalog.connection_handler import get_catalog_connection


def get_metadata_with_ids(table: dict) -> List[Dict[str, str]]:
    """
    Haalt kolomnamen, datatypes en column_id op uit de catalogus voor een opgegeven tabel.
    :param table: Dictionary met server_name, database_name, schema_name, table_name
    :return: Lijst van dicts met keys: 'column_id', 'name', 'type'
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT col.id AS column_id, col.column_name, col.data_type
                FROM catalog.catalog_columns col
                JOIN catalog.catalog_tables t ON col.table_id = t.id
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
                  AND s.schema_name = %s
                  AND t.table_name = %s
                ORDER BY col.ordinal_position
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"]
            ))
            rows = cur.fetchall()
            return [{"column_id": r[0], "name": r[1], "type": r[2]} for r in rows]
    finally:
        conn.close()


def get_tables_for_pattern_with_ids(
    server_name: str,
    database_name: str,
    schema_name: str,
    prefix: str
) -> List[dict]:
    """
    Zoekt tabellen in catalogus op basis van prefix (ILIKE) binnen één schema,
    inclusief database_id, schema_id en table_id.
    :return: Lijst dicts met keys:
      server_name, database_name, database_id,
      schema_name, schema_id,
      table_name, table_id
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.server_name, d.database_name, d.id AS database_id,
                       s.schema_name, s.id AS schema_id,
                       t.table_name, t.id AS table_id
                FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
                  AND s.schema_name = %s
                  AND t.table_name ILIKE %s
            """, (server_name, database_name, schema_name, prefix + '%'))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def get_view_definition_with_ids(table: dict) -> Optional[str]:
    """
    Haalt de opgeslagen viewdefinitie op uit catalog_view_definitions,
    werkt op basis van server/database/schema/table met IDs.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT v.definition
                FROM catalog.catalog_view_definitions v
                JOIN catalog.catalog_tables t ON v.table_id = t.id
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
                  AND s.schema_name = %s
                  AND t.table_name = %s
                LIMIT 1
            """, (
                table["server_name"],
                table["database_name"],
                table["schema_name"],
                table["table_name"]
            ))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def get_catalog_tables_for_server_with_ids(server_name: str) -> List[dict]:
    """
    Haalt alle tabellen op voor een server, inclusief alle IDs.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.server_name, d.database_name, d.id AS database_id,
                       s.schema_name, s.id AS schema_id,
                       t.table_name, t.id AS table_id
                FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
            """, (server_name,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def get_ai_analysis_results(run_id: int, server_name: str, database_name: str,
                            schema_name: str, table_name: Optional[str] = None) -> Dict[str, dict]:
    """
    Haalt AI-analyseresultaten op uit catalog_ai_analysis_results
    voor een specifieke run en tabel.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT analysis_type, result_json
                FROM catalog.catalog_ai_analysis_results
                WHERE run_id = %s
                  AND server_name = %s
                  AND database_name = %s
                  AND schema_name = %s
            """
            params = [run_id, server_name, database_name, schema_name]

            if table_name is None:
                query += " AND table_name IS NULL"
            else:
                query += " AND table_name = %s"
                params.append(table_name)

            cur.execute(query, params)
            rows = cur.fetchall()
            return {row[0]: row[1] for row in rows} if rows else {}
    finally:
        conn.close()


def get_full_table_metadata(run_id: int, table: dict) -> dict:
    """
    Haalt technische metadata en AI-analyseresultaten op voor een tabel.
    """
    technical = get_metadata_with_ids(table)
    ai_results = get_ai_analysis_results(
        run_id,
        table["server_name"],
        table["database_name"],
        table["schema_name"],
        table["table_name"]
    )
    return {
        "technical_metadata": technical,
        "ai_results": ai_results
    }


def _matches_pattern(name: str, pattern: Optional[str]) -> bool:
    """
    Controleert of 'name' matcht met één van de comma-separated wildcard patronen in 'pattern'.
    '*' vertaald naar regex '.*' voor case-insensitive match.
    """
    if not pattern:
        return True
    patterns = [p.strip() for p in pattern.split(",") if p.strip()]
    for pat in patterns:
        regex_pat = re.escape(pat).replace("\\*", ".*")
        if re.fullmatch(regex_pat, name, flags=re.IGNORECASE):
            return True
    return False


def get_filtered_tables_with_ids(server_name: str,
                                 database_name: Optional[str] = None,
                                 schema_pattern: Optional[str] = None,
                                 table_pattern: Optional[str] = None) -> List[dict]:
    """
    Haalt tabellen uit catalogus met optionele filters en wildcards,
    inclusief bijbehorende IDs.
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT d.server_name, d.database_name, d.id AS database_id,
                       s.schema_name, s.id AS schema_id,
                       t.table_name, t.id AS table_id
                FROM catalog.catalog_tables t
                JOIN catalog.catalog_schemas s ON t.schema_id = s.id
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
            """
            params = [server_name]

            if database_name:
                query += " AND d.database_name = %s"
                params.append(database_name)

            cur.execute(query, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            tables = [dict(zip(cols, row)) for row in rows]

            filtered = []
            for t in tables:
                if not _matches_pattern(t["schema_name"], schema_pattern):
                    continue
                if not _matches_pattern(t["table_name"], table_pattern):
                    continue
                filtered.append(t)

            return filtered
    finally:
        conn.close()

def get_schemas_for_database_with_ids(
    server_name: str,
    database_name: str,
    schema_pattern: Optional[str] = None
) -> List[dict]:
    """
    Haalt schemas op voor een gegeven server en database,
    inclusief schema_id, met optionele filtering op schema naam.

    :return: Lijst dicts met keys: server_name, database_name, database_id,
             schema_name, schema_id
    """
    conn = get_catalog_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT d.server_name, d.database_name, d.id AS database_id,
                       s.schema_name, s.id AS schema_id
                FROM catalog.catalog_schemas s
                JOIN catalog.catalog_databases d ON s.database_id = d.id
                WHERE d.server_name = %s
                  AND d.database_name = %s
            """
            params = [server_name, database_name]

            cur.execute(query, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            schemas = [dict(zip(cols, row)) for row in rows]

            if not schema_pattern:
                return schemas

            # Filter op schema patroon
            def _matches_pattern(name: str, pattern: Optional[str]) -> bool:
                if not pattern:
                    return True
                patterns = [p.strip() for p in pattern.split(",") if p.strip()]
                for pat in patterns:
                    regex_pat = re.escape(pat).replace("\\*", ".*")
                    if re.fullmatch(regex_pat, name, flags=re.IGNORECASE):
                        return True
                return False

            filtered = [s for s in schemas if _matches_pattern(s["schema_name"], schema_pattern)]
            return filtered
    finally:
        conn.close()