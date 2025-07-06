import re
import logging
from typing import Optional
from data_catalog.connection_handler import (
    get_main_connector_by_id,
    get_ai_config_by_id,
    connect_to_source_database,
    build_sqlalchemy_engine
)
import pandas as pd

logger = logging.getLogger(__name__)

def execute_sample_query(table: dict, query: str) -> pd.DataFrame:
    """
    Voert een SQL-query uit op de brontabel. Wordt o.a. gebruikt voor het ophalen van sample data.
    
    Vereist dat 'table' minimaal bevat:
    - main_connector_id
    - database_name

    :param table: dictionary met connectie- en tabelgegevens
    :param query: uit te voeren SQL-query
    :return: pandas DataFrame met resultaten (kan leeg zijn bij fout)
    """
    try:
        main_conn = get_main_connector_by_id(table["main_connector_id"])
        conn_info = main_conn.copy()
        conn_info["database_name"] = table["database_name"]

        with connect_to_source_database(conn_info, database_name=conn_info["database_name"]) as conn:
            df = pd.read_sql(query, conn)
            return df

    except Exception as e:
        logger.warning(f"Fout bij uitvoeren sample query op tabel {table.get('table_name')}: {e}")
        return pd.DataFrame()

def matches_filter(name: str, filter_str: Optional[str]) -> bool:
    """
    Checkt of 'name' overeenkomt met één van de comma-separated filter patronen.
    Wildcards '*' in filter_str worden vertaald naar regex '.*' voor contains match.
    Als filter_str None of leeg is, altijd True (geen filter).

    Voorbeeld:
    filter_str = 'sales,hr,*log*,*temp' betekent match als naam 'sales', 'hr', iets met 'log' of 'temp' bevat.
    """
    if not filter_str:
        return True  # geen filter betekent altijd match

    patterns = [pat.strip() for pat in filter_str.split(",") if pat.strip()]
    for pat in patterns:
        # Escape regex speciaaltekens behalve '*', vervang '*' door '.*'
        regex_pat = re.escape(pat).replace("\\*", ".*")
        if re.fullmatch(regex_pat, name, flags=re.IGNORECASE):
            return True
    return False


def get_sample_data(table: dict, sample_size: int = 50) -> pd.DataFrame:
    """
    Haalt een sample van rijen op uit de brontabel, rekening houdend met AI-config filters.
    
    Vereist keys in 'table':
      - main_connector_id: int
      - ai_config_id: int
      - database_name: str (verplicht)
      - schema_name: str (optioneel, default 'public')
      - table_name: str (verplicht)
    """
    try:
        main_conn = get_main_connector_by_id(table["main_connector_id"])
        ai_config = get_ai_config_by_id(table["ai_config_id"])

        database = table.get("database_name")
        if not database:
            logger.warning("Geen database_name opgegeven, sample data overslaan.")
            return pd.DataFrame()

        schema = table.get("schema_name") or "public"
        table_name = table.get("table_name")
        if not table_name:
            logger.warning("Geen table_name opgegeven, sample data overslaan.")
            return pd.DataFrame()

        if not matches_filter(schema, ai_config.get("ai_schema_filter") if ai_config else None):
            logger.debug(f"Schema {schema} valt buiten AI schema filter")
            return pd.DataFrame()

        if not matches_filter(table_name, ai_config.get("ai_table_filter") if ai_config else None):
            logger.debug(f"Tabel {table_name} valt buiten AI table filter")
            return pd.DataFrame()

        query = (
            f'SELECT * FROM "{schema}"."{table_name}" '
            f'LIMIT {sample_size}'
        )
        logger.debug(f"Query sample data: {query} op connector ID {table['main_connector_id']}")

        conn_info = main_conn.copy()
        conn_info["database_name"] = database

        with connect_to_source_database(conn_info) as conn:
            return pd.read_sql(query, conn)

    except Exception as e:
        logger.warning(f"Sample data ophalen mislukt voor table {table.get('table_name')}: {e}")
        return pd.DataFrame()


def get_row_count(table: dict) -> int:
    """
    Telt het aantal rijen in de brontabel, rekening houdend met AI-config filters.
    """
    try:
        main_conn = get_main_connector_by_id(table["main_connector_id"])
        ai_config = get_ai_config_by_id(table["ai_config_id"])

        database = table.get("database_name")
        if not database:
            logger.warning("Geen database_name opgegeven, row count overslaan.")
            return 0

        schema = table.get("schema_name") or "public"
        table_name = table.get("table_name")
        if not table_name:
            logger.warning("Geen table_name opgegeven, row count overslaan.")
            return 0

        if not matches_filter(schema, ai_config.get("ai_schema_filter") if ai_config else None):
            logger.debug(f"Schema {schema} valt buiten AI schema filter")
            return 0

        if not matches_filter(table_name, ai_config.get("ai_table_filter") if ai_config else None):
            logger.debug(f"Tabel {table_name} valt buiten AI table filter")
            return 0

        query = (
            f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
        )
        logger.debug(f"Query row count: {query} op connector ID {table['main_connector_id']}")

        conn_info = main_conn.copy()
        conn_info["database_name"] = database

        with connect_to_source_database(conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchone()[0]

    except Exception as e:
        logger.warning(f"Row count ophalen mislukt voor table {table.get('table_name')}: {e}")
        return 0


def get_column_distinct_values(table: dict, column_name: str, limit: int = 10) -> list:
    """
    Haalt een lijst van unieke waarden op voor een kolom in de brontabel, rekening houdend met AI-config filters.
    """
    try:
        main_conn = get_main_connector_by_id(table["main_connector_id"])
        ai_config = get_ai_config_by_id(table["ai_config_id"])

        database = table.get("database_name")
        if not database:
            logger.warning("Geen database_name opgegeven, distinct values overslaan.")
            return []

        schema = table.get("schema_name") or "public"
        table_name = table.get("table_name")
        if not table_name:
            logger.warning("Geen table_name opgegeven, distinct values overslaan.")
            return []

        if not matches_filter(schema, ai_config.get("ai_schema_filter") if ai_config else None):
            logger.debug(f"Schema {schema} valt buiten AI schema filter")
            return []

        if not matches_filter(table_name, ai_config.get("ai_table_filter") if ai_config else None):
            logger.debug(f"Tabel {table_name} valt buiten AI table filter")
            return []

        query = (
            f'SELECT DISTINCT "{column_name}" '
            f'FROM "{schema}"."{table_name}" '
            f'WHERE "{column_name}" IS NOT NULL '
            f'LIMIT {limit}'
        )
        logger.debug(f"Query distinct values: {query} op connector ID {table['main_connector_id']}")

        conn_info = main_conn.copy()
        conn_info["database_name"] = database

        with connect_to_source_database(conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]

    except Exception as e:
        logger.warning(f"Distinct values ophalen mislukt voor kolom {column_name} in table {table.get('table_name')}: {e}")
        return []


def get_engine_for_table(table: dict):
    """
    Bepaalt SQLAlchemy engine obv main connector + database.
    """
    try:
        main_conn = get_main_connector_by_id(table["main_connector_id"])
        database = table.get("database_name")
        if not database:
            raise ValueError("database_name is verplicht voor get_engine_for_table")
        return build_sqlalchemy_engine(main_conn, database_name=database)
    except Exception as e:
        logger.warning(f"Engine ophalen mislukt voor table {table.get('table_name')}: {e}")
        return None


def get_engine_for_schema(main_connector_id: int, database_name: str):
    """
    Bepaalt SQLAlchemy engine voor schema op basis van main connector ID en database.
    """
    try:
        main_conn = get_main_connector_by_id(main_connector_id)
        return build_sqlalchemy_engine(main_conn, database_name=database_name)
    except Exception as e:
        logger.warning(f"Engine ophalen mislukt voor schema in database {database_name}: {e}")
        return None
    
def get_sample_data_for_table_description(table: dict) -> pd.DataFrame:
    """
    Sample data voor analyse van tabelbeschrijving (klein fragment).
    """
    return get_sample_data(table, sample_size=50)


def get_sample_data_for_column_classification(table: dict) -> pd.DataFrame:
    """
    Sample data voor kolomclassificatie (iets ruimer).
    """
    return get_sample_data(table, sample_size=200)


def get_sample_data_for_data_quality(table: dict) -> pd.DataFrame:
    """
    Sample data voor datakwaliteitsanalyse (groter bereik).
    """
    return get_sample_data(table, sample_size=500)


def get_sample_data_for_data_presence(table: dict) -> pd.DataFrame:
    """
    Sample data voor aanwezigheid/actualiteitsanalyse (eventueel leeg is al informatie).
    """
    return get_sample_data(table, sample_size=10)