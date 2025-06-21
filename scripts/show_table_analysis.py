
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from data_catalog.ai_analyzer.query_analysis import get_table_analysis
import json

# 👇 Vul hier de gegevens in van de tabel die je wil ophalen
server_name = "VPS1"
database_name = "ENK_DEV1"
schema_name = "stage"
table_name = "ods_alias_werken"

# Ophalen
result = get_table_analysis(server_name, database_name, schema_name, table_name)

# Weergave
if result:
    print("🔍 Analyse gevonden:\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))
else:
    print("⚠️ Geen analyse gevonden voor deze tabel.")