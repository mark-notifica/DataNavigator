from data_catalog.database_server_cataloger import get_source_connections, connect_to_source_database

def get_connection_by_server_name(server_name: str):
    """Zoekt connectie-informatie op uit config.connections o.b.v. server_name"""
    all_connections = get_source_connections()
    for conn in all_connections:
        if conn["name"] == server_name:
            return conn
    raise ValueError(f"Geen connectie gevonden voor server_name '{server_name}'")