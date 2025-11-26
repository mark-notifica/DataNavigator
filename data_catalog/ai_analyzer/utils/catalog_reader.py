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
)

__all__ = [
    "get_tables_for_pattern_with_ids",
    "get_metadata_with_ids",
    "get_view_definition_with_ids",
]
