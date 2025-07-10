# Column Occurrence Analyzer
# --------------------------
# Deze preprocessor telt hoe vaak een kolomnaam voorkomt binnen een database/schema.
# Berekent ook in hoeveel tabellen een kolom voorkomt (table_count).
#
# ➕ Potentiële toegevoegde waarde:
# 1. AI Column Analyzer:
#    - Frequent voorkomende kolommen (zoals 'id', 'user_id', 'project_id') kunnen wijzen op foreign keys.
#    - Samen met uniqueness-profielen vormt dit input voor key-classificatie.
#
# 2. Graph Builder:
#    - Veelvoorkomende kolommen kunnen als verbindende 'anchors' gebruikt worden in de graph.
#    - Bijv. 'project_id' in 20 tabellen → potentiële koppelkans.
#
# 3. Standaardisatie-inzicht:
#    - Laat zien hoe kolomnamen consistent (of inconsistent) zijn toegepast binnen een datawarehouse.
#
# ⚠️ Beperkingen & risico’s:
# - Kolomnamen zijn geen garantie voor semantische gelijkheid!
#   'id' in de ene tabel kan iets totaal anders betekenen dan in een andere.
# - Cross-database/schemagebruik kan verwarrend of zelfs misleidend zijn.
# - Naming-inconsistentie ('project_id' vs. 'proj_id') beperkt effectiviteit van deze analyse.
# - Kolommen kunnen vaak voorkomen zonder unieke waarden → geen sleutel!
#
# 🎯 Aanbevolen gebruik:
# - Combineer occurrence-analyse altijd met:
#   • Uniqueness-ratio uit column_profile
#   • AI-classificatie op basis van context + voorbeelden
#   • Filters per schema/databron om betekenis te waarborgen
#
# ❗ Gebruik dit dus *niet* als beslissende factor voor key-herkenning,
# maar als verkennende indicator of ondersteunend signaal.
#
# 📌 Tip:
# Aanroepen van de databasefunctie kan met:
# SELECT *
# FROM catalog.fn_filtered_column_occurrences(
#     p_database_name => 'ENK_DEV1',
#     p_schema_name   => 'stg',
#     p_table_like_pattern => 'ods_alias_%'
# );
