from data_catalog.connection_handler import get_connection_by_server_name, connect_to_source_database, build_sqlalchemy_engine
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_sample_data(table: dict, sample_size: int = 50) -> pd.DataFrame:
    """
    Haalt een sample van rijen op uit de brontabel.

    :param table: Dict met server_name, database_name, schema_name, table_name
    :param sample_size: Aantal rijen om op te halen
    :return: DataFrame met sample data
    """
    conn_info = get_connection_by_server_name(table["server_name"])
    conn_info["database_name"] = table["database_name"]  # overschrijf indien nodig
    query = f'SELECT * FROM "{table["schema_name"]}"."{table["table_name"]}" LIMIT {sample_size}'
    
    logger.debug(f"Querying sample data from {table['table_name']} on {table['server_name']}")
    
    with connect_to_source_database(conn_info) as conn:
        return pd.read_sql(query, conn)

def get_row_count(table: dict) -> int:
    """
    Telt het aantal rijen in de brontabel.

    :param table: Dict met server_name, database_name, schema_name, table_name
    :return: Aantal rijen
    """
    conn_info = get_connection_by_server_name(table["server_name"])
    conn_info["database_name"] = table["database_name"]
    query = f'SELECT COUNT(*) FROM "{table["schema_name"]}"."{table["table_name"]}"'
    
    with connect_to_source_database(conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

def get_column_distinct_values(table: dict, column_name: str, limit: int = 10) -> list:
    """
    Haalt een lijst van unieke waarden op voor een kolom in de brontabel.

    :param table: Dict met server_name, database_name, schema_name, table_name
    :param column_name: Kolomnaam waarvoor je unieke waarden wilt
    :param limit: Aantal unieke waarden om op te halen
    :return: Lijst met unieke waarden
    """
    conn_info = get_connection_by_server_name(table["server_name"])
    conn_info["database_name"] = table["database_name"]
    query = f'''
        SELECT DISTINCT "{column_name}"
        FROM "{table["schema_name"]}"."{table["table_name"]}"
        WHERE "{column_name}" IS NOT NULL
        LIMIT {limit}
    '''
    
    with connect_to_source_database(conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

def get_engine_for_table(table: dict):
    """
    Bepaalt de juiste SQLAlchemy engine op basis van server_name via config.connections.
    """
    conn_info = get_connection_by_server_name(table["server_name"])
    if not conn_info:
        raise ValueError(f"Geen connectie gevonden voor server: {table['server_name']}")
    return build_sqlalchemy_engine(conn_info)


def get_engine_for_schema(server_name: str, database_name: str):
    """
    Bouwt een SQLAlchemy-engine voor een specifieke server en database,
    op basis van de config.connections.
    """
    conn_info = get_connection_by_server_name(server_name)
    conn_info["database_name"] = database_name
    return build_sqlalchemy_engine(conn_info)