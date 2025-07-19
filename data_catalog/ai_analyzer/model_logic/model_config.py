import logging
from ai_analyzer.analysis.analysis_matrix import ANALYSIS_TYPES


import logging
from ai_analyzer.analysis.analysis_matrix import ANALYSIS_TYPES, SCHEMA_ANALYSIS_TYPES
from data_catalog.connection_handler import get_catalog_connection
import psycopg2.extras

def get_model_config(
    analysis_type: str,
    ai_config: dict | None = None,
    dw_connection_config_id: int | None = None
) -> tuple[str, float, int, str]:
    """
    Bepaalt welk model, temperatuur en max_tokens gebruikt moeten worden voor een analyse.
    Prioriteit:
    1. Instellingen in dw_ai_model_config (mits dw_connection_config_id gegeven en use_for_ai = true)
    2. Overrides in ai_config
    3. Defaults uit analysis_matrix
    4. Fallback

    :return: tuple (model, temperature, max_tokens, model_config_source)
    """
    # 1. Load default from matrix
    base_config = ANALYSIS_TYPES.get(analysis_type) or SCHEMA_ANALYSIS_TYPES.get(analysis_type) or {}
    model = base_config.get("default_model", "gpt-4")
    temperature = base_config.get("temperature", 0.7)
    max_tokens = base_config.get("max_tokens", 1000)
    model_config_source = "analysis_type_default"

    # 2. Check dw_ai_model_config in DB
    if dw_connection_config_id:
        conn = get_catalog_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT model, temperature, max_tokens
                    FROM config.dw_ai_model_config
                    WHERE dw_connection_config_id = %s
                      AND analysis_type = %s
                    LIMIT 1
                """, (dw_connection_config_id, analysis_type))
                row = cur.fetchone()
                if row:
                    model = row["model"] or model
                    temperature = row["temperature"] if row["temperature"] is not None else temperature
                    max_tokens = row["max_tokens"] if row["max_tokens"] is not None else max_tokens
                    model_config_source = "dw_ai_model_config"
        finally:
            conn.close()

    # 3. Override via ai_config als opgegeven
    if ai_config:
        overridden = False

        if ai_config.get("model") and ai_config["model"] != model:
            logging.info(f"[OVERRIDE] Model overschreven door ai_config: {ai_config['model']} (was: {model})")
            model = ai_config["model"]
            overridden = True

        if ai_config.get("temperature") is not None and ai_config["temperature"] != temperature:
            logging.info(f"[OVERRIDE] Temperature overschreven door ai_config: {ai_config['temperature']} (was: {temperature})")
            temperature = ai_config["temperature"]
            overridden = True

        if ai_config.get("max_tokens") is not None and ai_config["max_tokens"] != max_tokens:
            logging.info(f"[OVERRIDE] Max tokens overschreven door ai_config: {ai_config['max_tokens']} (was: {max_tokens})")
            max_tokens = ai_config["max_tokens"]
            overridden = True

        if overridden:
            model_config_source = "ai_config"

    # 4. Fallback
    if not model:
        model = "gpt-4"
        model_config_source = "fallback"

    return model, temperature, max_tokens, model_config_source

