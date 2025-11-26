"""
Compatibility shim for legacy imports used in tests:
    ai_analyzer.utils.file_writer.store_analysis_result_to_file

Provides a thin wrapper that writes a JSON file to a local directory.
If your project has a canonical writer elsewhere, you can swap the implementation
to delegate there. For tests, this simple implementation is sufficient and patchable.
"""
from __future__ import annotations

import json
import os
from typing import Dict, Any


def store_analysis_result_to_file(name: str, result_json: Dict[str, Any], output_dir: str | None = None) -> str:
    """
    Store analysis output as a JSON file and return the file path.

    Parameters:
        name: Base name of the file (without extension)
        result_json: The JSON-serializable content to write
        output_dir: Target directory; defaults to env var AI_ANALYZER_OUTPUT_DIR
                    falling back to tests/output
    """
    out_dir = (
        output_dir
        or os.getenv("AI_ANALYZER_OUTPUT_DIR")
        or os.path.join("data_catalog", "logfiles", "ai_analyzer")
    )

    os.makedirs(out_dir, exist_ok=True)

    filepath = os.path.join(out_dir, f"{name}_analysis.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result_json, f, indent=2, ensure_ascii=False)

    return filepath


__all__ = ["store_analysis_result_to_file"]
