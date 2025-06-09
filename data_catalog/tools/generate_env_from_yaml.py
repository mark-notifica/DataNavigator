import yaml
from pathlib import Path
import os
import re

# Invoerbestanden
db_config_path = Path("data_catalog/db_config.yaml")
server_config_path = Path("data_catalog/servers_config.yaml")
output_path = Path(".env.generated")
ENV_PATTERN = re.compile(r'^\${(.+)}$')

def _resolve_env(value):
    if isinstance(value, str):
        m = ENV_PATTERN.match(value)
        if m:
            return os.getenv(m.group(1), value)
    return value

lines = []

# Verwerk db_config.yaml
if db_config_path.exists():
    with db_config_path.open() as f:
        db_conf = yaml.safe_load(f)
    lines.extend([
        f"DB_HOST={_resolve_env(db_conf.get('host', ''))}",
        f"DB_PORT={_resolve_env(db_conf.get('port', ''))}",
        f"DB_NAME={_resolve_env(db_conf.get('database', ''))}",
        f"DB_USER={_resolve_env(db_conf.get('user', ''))}",
        f"DB_PASSWORD={_resolve_env(db_conf.get('password', ''))}",
        ""
    ])

# Verwerk servers_config.yaml
if server_config_path.exists():
    with server_config_path.open() as f:
        servers_conf = yaml.safe_load(f)

    for idx, server in enumerate(servers_conf.get("servers", []), start=1):
        lines.extend([
            f"SERVER{idx}_NAME={server.get('name', '')}",
            f"SERVER{idx}_HOST={server.get('host', '')}",
            f"SERVER{idx}_PORT={server.get('port', '')}",
            ""
        ])

# Schrijf output naar .env-bestand
output_path.write_text("\n".join(lines))
print(f".env-bestand gegenereerd als: {output_path.resolve()}")
