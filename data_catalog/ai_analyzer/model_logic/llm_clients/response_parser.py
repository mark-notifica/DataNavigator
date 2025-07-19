import logging
from .openai_parsing import (
    parse_openai_json_block,
    parse_column_classification_response
)

def parse_model_response(raw_response: str, analysis_type: str) -> object | None:
    """
    Parseert de AI-output op basis van het analysis_type.
    Centrale dispatcher die specifieke parsinglogica aanroept.

    :param raw_response: plain tekstresponse van model
    :param analysis_type: analysis_type die werd uitgevoerd
    :return: Geparsete data (meestal dict of str), of None bij fout
    """
    if not raw_response:
        logging.warning("[PARSER] Lege response ontvangen.")
        return None

    try:
        match analysis_type:
            case "column_classification":
                return parse_column_classification_response(raw_response)
            case "data_quality_check":
                return parse_openai_json_block(raw_response)
            case "data_presence_analysis":
                return parse_openai_json_block(raw_response)
            case "schema_summary" | "schema_recommendations" | "all_in_one":
                return parse_openai_json_block(raw_response)
            case _:  # fallback
                logging.info(f"[PARSER] Geen specifieke parser voor {analysis_type}, gebruik standaard JSON-parser.")
                return parse_openai_json_block(raw_response)

    except Exception as e:
        logging.error(f"[PARSER] Fout bij parsen van {analysis_type}: {e}")
        return None
