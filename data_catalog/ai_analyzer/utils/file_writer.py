import json
import os

def store_analysis_result_to_file(name: str, result_json: dict, output_dir="./tests/output"):
    """
    Slaat analyse op als lokaal JSON-bestand (voor debugging of dry-run)
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Kon output directory niet aanmaken: {output_dir}") from e

    if not os.path.isdir(output_dir):
        raise AssertionError(f"{output_dir} bestaat niet!")

    filename = f"{name}_analysis.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(result_json, f, indent=2)

    print(f"[DEBUG] filepath = {filepath}")  