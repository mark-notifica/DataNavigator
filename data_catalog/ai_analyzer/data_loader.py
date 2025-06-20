def get_enk_tables():
    # Simulatie: normaal haal je dit uit je catalog_table
    return [{"server_name": "ENK", "database_name": "enkdb", "schema_name": "public", "table_name": "enk_klanten"}]

def get_metadata(table):
    return [
        {"name": "klant_id", "type": "int"},
        {"name": "naam", "type": "varchar"},
        {"name": "postcode", "type": "varchar"}
    ]

def get_sample_data(table, metadata):
    return [
        {"klant_id": 1, "naam": "Jansen", "postcode": "1234AB"},
        {"klant_id": 2, "naam": "De Vries", "postcode": "5678CD"}
    ]