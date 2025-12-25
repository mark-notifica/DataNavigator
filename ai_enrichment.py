"""
AI-powered description enrichment for catalog items.
Supports batch generation and interactive enrichment.
"""

import os
from typing import Generator, Optional
from connection_db_postgres import get_catalog_connection


# === Reference Context Support ===

def load_reference_from_file(filepath: str) -> str:
    """Load reference context from a text file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def load_reference_from_table(table_qualified_name: str, name_column: str = 'code',
                               desc_column: str = 'omschrijving', limit: int = 500) -> str:
    """
    Load reference context from a database table (e.g., at_stent).

    Args:
        table_qualified_name: Full path like 'VPS3/1190/prepare/at_stent'
        name_column: Column containing the object name/code
        desc_column: Column containing the description
        limit: Max rows to load

    Returns:
        Formatted string with name: description pairs
    """
    # Parse qualified name to get server/database/schema/table
    parts = table_qualified_name.split('/')
    if len(parts) < 4:
        raise ValueError(f"Invalid qualified name: {table_qualified_name}")

    server_name = parts[0]
    database_name = parts[1]
    schema_name = parts[2]
    table_name = parts[3]

    # Get server connection info
    conn = get_catalog_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT s.host, s.port, d.database_name
        FROM catalog.node_server s
        JOIN catalog.nodes sn ON s.node_id = sn.node_id
        JOIN catalog.node_database d ON d.server_node_id = s.node_id
        JOIN catalog.nodes dn ON d.node_id = dn.node_id
        WHERE sn.name = %s AND d.database_name = %s
    """, (server_name, database_name))

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        raise ValueError(f"Server/database not found: {server_name}/{database_name}")

    host, port, db_name = row

    # Connect to source database and query the reference table
    import psycopg2
    source_conn = psycopg2.connect(
        host=host,
        port=port,
        database=db_name,
        user=os.environ.get('SOURCE_DB_USER', 'postgres'),
        password=os.environ.get('SOURCE_DB_PASSWORD', '')
    )
    source_cursor = source_conn.cursor()

    try:
        query = f"""
            SELECT {name_column}, {desc_column}
            FROM {schema_name}.{table_name}
            WHERE {desc_column} IS NOT NULL AND {desc_column} != ''
            LIMIT %s
        """
        source_cursor.execute(query, (limit,))
        rows = source_cursor.fetchall()
    finally:
        source_cursor.close()
        source_conn.close()

    # Format as reference text
    lines = []
    for name, desc in rows:
        if name and desc:
            lines.append(f"- {name}: {desc}")

    return "\n".join(lines)


def get_reference_tables(server_name: str = None, database_name: str = None) -> list:
    """
    Get list of potential reference tables (tables with 'stent', 'meta', 'dict' in name).
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    query = """
        SELECT DISTINCT n.qualified_name, n.name
        FROM catalog.nodes n
        WHERE n.object_type_code = 'DB_TABLE'
          AND n.deleted_at IS NULL
          AND (n.name ILIKE '%stent%' OR n.name ILIKE '%meta%' OR n.name ILIKE '%dict%')
    """
    params = []

    if server_name:
        query += " AND n.qualified_name LIKE %s"
        params.append(f"{server_name}/%")

    if database_name:
        query += " AND n.qualified_name LIKE %s"
        params.append(f"%/{database_name}/%")

    query += " ORDER BY n.qualified_name"

    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [{'qualified_name': r[0], 'name': r[1]} for r in rows]


def get_items_for_enrichment(server_name=None, database_name=None, schema_name=None,
                              object_types=None, include_described=False, limit=50):
    """
    Get catalog items that need descriptions.

    Returns list of dicts with item details including DDL for views.
    """
    if object_types is None:
        object_types = ['DB_TABLE', 'DB_VIEW', 'DB_COLUMN']

    conn = get_catalog_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            n.node_id,
            n.object_type_code,
            n.qualified_name,
            n.name,
            n.description,
            CASE
                WHEN n.object_type_code = 'DB_COLUMN' THEN c.data_type
                ELSE NULL
            END as data_type,
            CASE
                WHEN n.object_type_code IN ('DB_VIEW', 'DB_TABLE') THEN t.view_definition
                ELSE NULL
            END as view_definition
        FROM catalog.nodes n
        LEFT JOIN catalog.node_column c ON n.node_id = c.node_id
        LEFT JOIN catalog.node_table t ON n.node_id = t.node_id
        WHERE n.object_type_code = ANY(%s)
          AND n.deleted_at IS NULL
    """
    params = [object_types]

    if not include_described:
        query += " AND (n.description IS NULL OR n.description = '')"

    if server_name:
        query += " AND n.qualified_name LIKE %s"
        params.append(f"{server_name}/%")

    if database_name:
        query += " AND n.qualified_name LIKE %s"
        params.append(f"%/{database_name}/%")

    if schema_name:
        query += " AND n.qualified_name LIKE %s"
        params.append(f"%/{schema_name}/%")

    query += " ORDER BY n.qualified_name LIMIT %s"
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    items = []
    for row in rows:
        items.append({
            'node_id': row[0],
            'object_type': row[1],
            'qualified_name': row[2],
            'name': row[3],
            'description': row[4] or '',
            'data_type': row[5] or '',
            'view_definition': row[6] or ''
        })

    return items


def get_item_context(node_id: int) -> dict:
    """
    Get full context for a single item including parent/child relationships.
    """
    conn = get_catalog_connection()
    cursor = conn.cursor()

    # Get the item itself
    cursor.execute("""
        SELECT
            n.node_id,
            n.object_type_code,
            n.qualified_name,
            n.name,
            n.description,
            CASE WHEN n.object_type_code = 'DB_COLUMN' THEN c.data_type ELSE NULL END,
            CASE WHEN n.object_type_code IN ('DB_VIEW', 'DB_TABLE') THEN t.view_definition ELSE NULL END
        FROM catalog.nodes n
        LEFT JOIN catalog.node_column c ON n.node_id = c.node_id
        LEFT JOIN catalog.node_table t ON n.node_id = t.node_id
        WHERE n.node_id = %s
    """, (node_id,))

    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        return None

    item = {
        'node_id': row[0],
        'object_type': row[1],
        'qualified_name': row[2],
        'name': row[3],
        'description': row[4] or '',
        'data_type': row[5] or '',
        'view_definition': row[6] or '',
        'columns': [],
        'parent_description': ''
    }

    # If it's a table/view, get columns
    if item['object_type'] in ('DB_TABLE', 'DB_VIEW'):
        cursor.execute("""
            SELECT c.column_name, c.data_type, cn.description
            FROM catalog.node_column c
            JOIN catalog.nodes cn ON c.node_id = cn.node_id
            WHERE c.table_node_id = %s AND cn.deleted_at IS NULL
            ORDER BY c.column_name
        """, (node_id,))

        for col_row in cursor.fetchall():
            item['columns'].append({
                'name': col_row[0],
                'type': col_row[1],
                'description': col_row[2] or ''
            })

    # If it's a column, get parent table description
    if item['object_type'] == 'DB_COLUMN':
        cursor.execute("""
            SELECT pn.description, pn.qualified_name
            FROM catalog.node_column c
            JOIN catalog.nodes pn ON c.table_node_id = pn.node_id
            WHERE c.node_id = %s
        """, (node_id,))
        parent = cursor.fetchone()
        if parent:
            item['parent_description'] = parent[0] or ''
            item['parent_name'] = parent[1] or ''

    cursor.close()
    conn.close()

    return item


def save_description(node_id: int, description: str, source: str = 'ai') -> bool:
    """Save a description to a catalog node."""
    conn = get_catalog_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE catalog.nodes
            SET description = %s,
                description_source = %s,
                description_updated_at = NOW(),
                updated_at = NOW()
            WHERE node_id = %s
        """, (description, source, node_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def build_prompt_for_item(item: dict, reference_context: str = None) -> str:
    """Build a prompt for generating a description for a single item."""
    obj_type = item['object_type'].replace('DB_', '').lower()

    prompt_parts = [
        f"Generate a concise, clear description for this database {obj_type}.",
        f"\n**Name:** {item['name']}",
        f"**Full path:** {item['qualified_name']}"
    ]

    if item.get('data_type'):
        prompt_parts.append(f"**Data type:** {item['data_type']}")

    if item.get('view_definition'):
        ddl = item['view_definition'][:8000]  # Limit DDL size
        prompt_parts.append(f"\n**SQL Definition:**\n```sql\n{ddl}\n```")

    if item.get('columns'):
        col_list = []
        for col in item['columns'][:30]:  # Limit columns
            col_str = f"  - {col['name']} ({col['type']})"
            if col.get('description'):
                col_str += f": {col['description']}"
            col_list.append(col_str)
        prompt_parts.append(f"\n**Columns:**\n" + "\n".join(col_list))

    if item.get('parent_description'):
        prompt_parts.append(f"\n**Parent table description:** {item['parent_description']}")

    # Add reference context if provided
    if reference_context:
        prompt_parts.append(f"\n**Reference information from ERP system:**\n{reference_context[:6000]}")

    if item.get('description'):
        prompt_parts.append(f"\n**Current description:** {item['description']}")
        prompt_parts.append("\nImprove or confirm this description.")
    else:
        prompt_parts.append("\nWrite a description that explains what this contains and its purpose.")

    prompt_parts.append("\n\nRespond with ONLY the description text, no explanations or formatting.")

    return "\n".join(prompt_parts)


def generate_description(item: dict, model: str = "claude", reference_context: str = None) -> str:
    """
    Generate a description for a catalog item using AI.

    Args:
        item: Dict with item details from get_items_for_enrichment
        model: "claude" or "ollama"
        reference_context: Optional reference text (from file or reference table)

    Returns:
        Generated description string
    """
    prompt = build_prompt_for_item(item, reference_context=reference_context)

    system = """Je bent een data catalog documentatie expert. Je schrijft duidelijke,
beknopte beschrijvingen voor database objecten (tabellen, views, kolommen) in het Nederlands.

Richtlijnen:
- Wees beknopt (1-3 zinnen voor de meeste items)
- Focus op WAT het object bevat en WAAROM het bestaat
- Gebruik zakelijke/ERP terminologie waar mogelijk
- Voor views, leg uit wat de view berekent of presenteert
- Voor kolommen, leg uit welke waarden worden opgeslagen
- Herhaal niet de objectnaam in de beschrijving
- Gebruik geen zinnen als "Deze tabel bevat..." - beschrijf gewoon de inhoud
- Schrijf altijd in het Nederlands"""

    if model == "claude":
        return _call_claude(system, prompt)
    elif model == "ollama":
        return _call_ollama(system, prompt)
    else:
        raise ValueError(f"Unknown model: {model}")


def generate_description_stream(item: dict, model: str = "claude") -> Generator[str, None, None]:
    """
    Generate a description with streaming output.

    Yields chunks of text as they're generated.
    """
    prompt = build_prompt_for_item(item)

    system = """Je bent een data catalog documentatie expert. Je schrijft duidelijke,
beknopte beschrijvingen voor database objecten (tabellen, views, kolommen) in het Nederlands.

Richtlijnen:
- Wees beknopt (1-3 zinnen voor de meeste items)
- Focus op WAT het object bevat en WAAROM het bestaat
- Gebruik zakelijke/ERP terminologie waar mogelijk
- Voor views, leg uit wat de view berekent of presenteert
- Voor kolommen, leg uit welke waarden worden opgeslagen
- Herhaal niet de objectnaam in de beschrijving
- Gebruik geen zinnen als "Deze tabel bevat..." - beschrijf gewoon de inhoud
- Schrijf altijd in het Nederlands"""

    if model == "claude":
        yield from _call_claude_stream(system, prompt)
    elif model == "ollama":
        yield from _call_ollama_stream(system, prompt)
    else:
        raise ValueError(f"Unknown model: {model}")


def chat_about_item(item: dict, user_message: str, chat_history: list, model: str = "claude") -> str:
    """
    Have a conversation about a catalog item to refine its description.

    Args:
        item: Dict with item details
        user_message: The user's message
        chat_history: List of {"role": "user"|"assistant", "content": str}
        model: "claude" or "ollama"

    Returns:
        Assistant response
    """
    obj_type = item['object_type'].replace('DB_', '').lower()

    # Build context about the item
    context_parts = [
        f"You are helping document a database {obj_type}.",
        f"\n**Item:** {item['qualified_name']}",
        f"**Name:** {item['name']}"
    ]

    if item.get('data_type'):
        context_parts.append(f"**Data type:** {item['data_type']}")

    if item.get('view_definition'):
        ddl = item['view_definition'][:6000]
        context_parts.append(f"\n**SQL Definition:**\n```sql\n{ddl}\n```")

    if item.get('columns'):
        col_list = [f"  - {c['name']} ({c['type']})" for c in item['columns'][:20]]
        context_parts.append(f"\n**Columns:**\n" + "\n".join(col_list))

    if item.get('description'):
        context_parts.append(f"\n**Current description:** {item['description']}")

    context = "\n".join(context_parts)

    system = f"""Je bent een data catalog documentatie assistent die helpt bij het schrijven van beschrijvingen in het Nederlands.

{context}

Help de gebruiker dit object te begrijpen en schrijf een goede beschrijving. Wanneer de gebruiker tevreden is,
kan hij vragen om de beschrijving te "finaliseren" - antwoord dan met alleen de uiteindelijke beschrijvingstekst
voorafgegaan door "FINAL:" op een eigen regel."""

    # Build messages
    messages = []
    for msg in chat_history:
        messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})

    if model == "claude":
        return _call_claude_chat(system, messages)
    elif model == "ollama":
        return _call_ollama_chat(system, messages)
    else:
        raise ValueError(f"Unknown model: {model}")


# === LLM Backends ===

def _call_claude(system: str, user: str) -> str:
    """Call Claude API (non-streaming)."""
    try:
        import anthropic
    except ImportError:
        raise ImportError("The 'anthropic' module is not available. Please use Ollama instead or install anthropic with: pip install anthropic")

    client = anthropic.Anthropic()

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        system=system,
        messages=[{"role": "user", "content": user}]
    )

    return message.content[0].text.strip()


def _call_claude_stream(system: str, user: str) -> Generator[str, None, None]:
    """Call Claude API with streaming."""
    try:
        import anthropic
    except ImportError:
        raise ImportError("The 'anthropic' module is not available. Please use Ollama instead or install anthropic with: pip install anthropic")

    client = anthropic.Anthropic()

    with client.messages.stream(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        system=system,
        messages=[{"role": "user", "content": user}]
    ) as stream:
        for text in stream.text_stream:
            yield text


def _call_claude_chat(system: str, messages: list) -> str:
    """Call Claude API for multi-turn chat."""
    try:
        import anthropic
    except ImportError:
        raise ImportError("The 'anthropic' module is not available. Please use Ollama instead or install anthropic with: pip install anthropic")

    client = anthropic.Anthropic()

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=system,
        messages=messages
    )

    return message.content[0].text.strip()


def _call_ollama(system: str, user: str) -> str:
    """Call Ollama (non-streaming)."""
    from ollama import Client

    host = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
    model = os.environ.get('OLLAMA_MODEL', 'mistral:instruct')

    client = Client(host=host)

    response = client.chat(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ]
    )

    return response.get("message", {}).get("content", "").strip()


def _call_ollama_stream(system: str, user: str) -> Generator[str, None, None]:
    """Call Ollama with streaming."""
    from ollama import Client

    host = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
    model = os.environ.get('OLLAMA_MODEL', 'mistral:instruct')

    client = Client(host=host)

    stream = client.chat(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        stream=True
    )

    for chunk in stream:
        if chunk.get("message", {}).get("content"):
            yield chunk["message"]["content"]


def _call_ollama_chat(system: str, messages: list) -> str:
    """Call Ollama for multi-turn chat."""
    from ollama import Client

    host = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
    model = os.environ.get('OLLAMA_MODEL', 'mistral:instruct')

    client = Client(host=host)

    full_messages = [{"role": "system", "content": system}] + messages

    response = client.chat(model=model, messages=full_messages)

    return response.get("message", {}).get("content", "").strip()
