import json
import os

def store_analysis_result_to_file(name: str, result_json: dict, output_dir="./tests/output") -> str:
    """
    Slaat analyse op als lokaal JSON-bestand en retourneert het pad (voor dry-run logging).
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
    return filepath