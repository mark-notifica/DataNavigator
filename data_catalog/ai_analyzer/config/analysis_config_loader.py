import yaml
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "analysis_config.yaml"

def load_analysis_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def merge_analysis_configs(yaml_config: dict, matrix_config: dict) -> dict:
    """
    Combineert YAML-config met matrixconfig. YAML overrulet alleen als de waarde expliciet is gezet.
    """
    merged = {}

    for name, matrix_entry in matrix_config.items():
        yaml_entry = yaml_config.get(name, {})

        merged[name] = {
            **matrix_entry,
            **{k: v for k, v in yaml_entry.items() if v is not None}
        }

    return merged