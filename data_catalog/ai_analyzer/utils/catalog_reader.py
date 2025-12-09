"""
Compatibility shim for legacy imports used in tests.

Old path expected by tests:
    ai_analyzer.utils.catalog_reader

This module re-exports selected functions from
    ai_analyzer.catalog_access.catalog_reader
so existing tests continue to work.
"""

from ai_analyzer.catalog_access.catalog_reader import (
    get_tables_for_pattern_with_ids,
    get_metadata_with_ids,
    get_view_definition_with_ids,
    get_filtered_tables_with_ids as _real_filtered_tables,
)

def get_filtered_tables_with_ids(*args, **kwargs):  # pragma: no cover - thin alias
    """Compat alias.

    Geeft door naar de echte implementatie zodat patchen via deze module werkt.
    """
    return _real_filtered_tables(*args, **kwargs)


__all__ = [
    "get_tables_for_pattern_with_ids",
    "get_filtered_tables_with_ids",
    "get_metadata_with_ids",
    "get_view_definition_with_ids",
]
