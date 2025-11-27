import pandas as pd
from ai_analyzer.samples.query_translator import get_query_for_analysis_type
from ai_analyzer.samples.sample_data_reader import execute_sample_query
from data_catalog.connection_handler import get_main_connector_by_id
import logging
from ai_analyzer.samples.query_translator import map_connection_type_to_engine_type

def validate_table_metadata(table: dict, required_fields: list = None):
    """
    Valideert of verplichte velden aanwezig zijn in het table-dict.
    """
    if required_fields is None:
        required_fields = [
            "main_connector_id",
            "database_name",
            "schema_name",
            "table_name",
            "ai_config_id"
        ]

    missing = [field for field in required_fields if field not in table or not table[field]]
    if missing:
        raise ValueError(f"[VALIDATIE] Ontbrekende verplichte velden in table: {missing}")

def get_engine_type(table: dict) -> str:
    """
    Bepaalt het engine_type op basis van main_connector_id in de tabel.
    Als engine_type ontbreekt, probeer af te leiden uit connection_type.
    """
    main_conn = get_main_connector_by_id(table["main_connector_id"])
    engine = main_conn.get("engine_type")
    if not engine:
        conn_type = main_conn.get("connection_type")
        if not conn_type:
            raise ValueError(f"[FOUT] Geen engine_type of connection_type gevonden voor connector ID {table['main_connector_id']}")
        engine = map_connection_type_to_engine_type(conn_type)
    return engine.lower()


def fetch_sample_data(table: dict, analysis_type: str, random: bool = False) -> pd.DataFrame:
    """
    Algemene functie voor ophalen van sample data gebaseerd op analysis_type.

    Bouwt SQL-query en voert deze uit via sample_data_reader.execute_sample_query.
    """
    try:
        # ✅ Valideer verplichte metadata
        validate_table_metadata(table)

        # 🔎 Bepaal engine
        engine_type = get_engine_type(table)
        schema = table.get("schema_name") or "public"
        table_name = table["table_name"]

        # 🧱 Genereer SQL-query
        query = get_query_for_analysis_type(
            analysis_type=analysis_type,
            schema=schema,
            table=table_name,
            engine_type=engine_type,
            metadata=table,
            random=random
        )

        # ❌ Ongeldige query?    
        if not query or not isinstance(query, str) or query.strip() == "":
            logging.warning(f"[INVALID QUERY] Lege of ongeldige query gegenereerd voor {table_name} ({analysis_type})")
            return pd.DataFrame()

        logging.debug(f"[QUERY] Voor {table_name}: {query.strip()}")

        # ▶️ Voer query uit
        return execute_sample_query(table, query)

    except Exception as e:
        logging.warning(f"[ERROR] Sample ophalen mislukt voor {table.get('table_name')}: {e}")
        logging.exception("Stacktrace:")
        return pd.DataFrame()

def get_sample_data_for_base_table_analysis(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="base_table_analysis")


def get_sample_data_for_table_description(table: dict) -> pd.DataFrame:
    """Alias voor legacy tests.

    Table description gebruikt dezelfde sampler als base table analysis.
    """
    return get_sample_data_for_base_table_analysis(table)


def get_sample_data_for_column_classification(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="column_classification")


def get_sample_data_for_data_quality(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="data_quality_check")


def get_sample_data_for_data_presence(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="data_presence_analysis")
