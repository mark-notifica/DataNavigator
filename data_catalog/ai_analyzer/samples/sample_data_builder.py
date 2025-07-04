import pandas as pd
from ai_analyzer.samples.query_translator import get_query_for_analysis_type
from data_catalog.connection_handler import get_main_connector_by_id
import logging


def get_engine_type(table: dict) -> str:
    """
    Bepaalt het engine_type op basis van main_connector_id in de tabel.
    """
    main_conn = get_main_connector_by_id(table["main_connector_id"])
    return main_conn.get("engine_type", "").lower()


def fetch_sample_data(table: dict, analysis_type: str, random: bool = False) -> pd.DataFrame:
    """
    Algemene functie voor ophalen van sample data gebaseerd op analysis_type.
    """
    engine_type = get_engine_type(table)
    schema = table.get("schema_name") or "public"
    table_name = table["table_name"]

    query = get_query_for_analysis_type(
        analysis_type=analysis_type,
        schema=schema,
        table=table_name,
        engine_type=engine_type,
        random=random
    )
    if not query:
        return pd.DataFrame()

def get_sample_data_for_table_description(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="table_description")


def get_sample_data_for_column_classification(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="column_classification")


def get_sample_data_for_data_quality(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="data_quality_check")


def get_sample_data_for_data_presence(table: dict) -> pd.DataFrame:
    return fetch_sample_data(table, analysis_type="data_presence_analysis")