import json
import ast
import re
import logging
from typing import Optional, Dict

def parse_openai_json_block(raw_response: str) -> dict | None:
    """
    Probeert een JSON-object te extraheren uit een OpenAI-response
    die mogelijk omgeven is door markdown-opmaak (zoals ```json ... ```).
    Retourneert None bij parsing errors.
    """
    if not raw_response:
        return None

    try:
        # Stap 1: verwijder codeblock-markeringen zoals ```json of ```
        cleaned = re.sub(r"^```(?:json)?", "", raw_response.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"```$", "", cleaned.strip())

        # Stap 2: probeer als JSON te parsen
        return json.loads(cleaned)

    except json.JSONDecodeError as je:
        logging.warning(f"[PARSER] JSON decode fout: {je}. Probeer fallback.")
        try:
            # Fallback: gebruik ast.literal_eval voor Python-style dicts (enkele quotes etc.)
            return ast.literal_eval(cleaned)
        except Exception as ae:
            logging.error(f"[PARSER] Fallback ast-eval faalt ook: {ae}")
            return None

def parse_column_classification_response(raw_response: str) -> dict[str, str] | None:
    """
    Parseert een AI-response voor column_classification.
    Verwacht een JSON-achtig object met kolomnamen als keys en classificatielabels als strings.
    """

    if not raw_response:
        return None

    try:
        # Strip markdown-codeblock met of zonder json en met of zonder \n
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw_response.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned.strip())

        # JSON laden
        parsed = json.loads(cleaned)

    except json.JSONDecodeError as je:
        logging.warning(f"[PARSER] JSON decode fout: {je}. Probeer fallback.")
        try:
            parsed = ast.literal_eval(cleaned)
        except Exception as ae:
            logging.error(f"[PARSER] Fallback parsing column_classification faalt ook: {ae}")
            return None

    # Validatie
    if not isinstance(parsed, dict):
        logging.warning("[PARSER] Verwacht dict maar kreeg iets anders.")
        return None

    invalid_items = {k: v for k, v in parsed.items() if not isinstance(k, str) or not isinstance(v, str)}
    if invalid_items:
        logging.warning(f"[PARSER] Ongeldige items gevonden in parsed response: {invalid_items}")
        return None

    return parsed

def extract_text_from_response(response: dict) -> str:
    try:
        return response["choices"][0]["message"]["content"]
    except Exception as e:
        logging.warning(f"[PARSER] Kan tekst niet extraheren: {e}")
        return ""