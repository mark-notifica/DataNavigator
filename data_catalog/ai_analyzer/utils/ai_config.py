import re
from typing import Optional

def matches_filter(
    name: str,
    filter_str: Optional[str],
    case_sensitive: bool = False
) -> bool:
    """
    Checkt of 'name' overeenkomt met een van de comma-separated patronen in 'filter_str'.
    Wildcards (*) worden vertaald naar regex '.*'.

    :param name: De naam om te vergelijken (schema, tabel, kolom, etc.)
    :param filter_str: De filterstring, comma-separated met wildcards (bijv. '*log*,sales,temp*')
    :param case_sensitive: Of hoofdlettergevoelig vergeleken moet worden
    :return: True als er een match is, anders False
    """
    if not filter_str:
        return True

    patterns = [pat.strip() for pat in filter_str.split(",") if pat.strip()]
    flags = 0 if case_sensitive else re.IGNORECASE

    for pat in patterns:
        regex_pat = re.escape(pat).replace("\\*", ".*")
        if re.fullmatch(regex_pat, name, flags=flags):
            return True
    return False


def table_is_allowed_by_config(
    table: dict,
    ai_config: dict
) -> bool:
    """
    Controleert of een tabel is toegestaan volgens de schema- en tabel-filters in ai_config.

    Verwacht in 'table':
        - schema_name
        - table_name

    En in 'ai_config':
        - ai_schema_filter (optioneel)
        - ai_table_filter (optioneel)

    :return: True als toegestaan, anders False
    """
    schema = table.get("schema_name") or "public"
    table_name = table.get("table_name")

    schema_filter = ai_config.get("ai_schema_filter")
    table_filter = ai_config.get("ai_table_filter")

    if not matches_filter(schema, schema_filter):
        return False
    if not matches_filter(table_name, table_filter):
        return False
    return True

def resolve_ai_config_and_connection(ai_config_id: int):
    """
    Haalt AI-config + bijbehorende connectie op uit configuratie.
    Raise bij fouten.

    Returns:
        ai_config (dict), connection_info (dict)
    """
    from ..config import get_ai_config_by_id, get_specific_connection

    ai_config = get_ai_config_by_id(ai_config_id)
    if not ai_config:
        raise ValueError(f"Geen AI-config gevonden voor id={ai_config_id}")

    try:
        connection = get_specific_connection(ai_config["connection_id"])
    except Exception as e:
        raise ConnectionError(f"Kan geen connectie ophalen uit ai_config: {e}")

    return ai_config, connection