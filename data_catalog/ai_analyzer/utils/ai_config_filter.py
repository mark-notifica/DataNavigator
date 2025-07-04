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