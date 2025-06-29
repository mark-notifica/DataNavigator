def ensure_single_schema_across_tables(tables: list[dict]) -> tuple:
    """Controleer of alle tabellen binnen dezelfde server/database/schema vallen."""
    if not tables:
        raise ValueError("Geen tabellen opgegeven.")

    first = tables[0]
    server = first.get("server_name")
    database = first.get("database_name")
    schema = first.get("schema_name")

    for t in tables:
        if (
            t.get("server_name") != server
            or t.get("database_name") != database
            or t.get("schema_name") != schema
        ):
            raise ValueError("Niet alle tabellen vallen binnen dezelfde server/database/schema.")

    return server, database, schema