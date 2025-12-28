# DataNavigator

A data catalog tool for documenting and exploring your database metadata. Extract schemas, tables, and columns from PostgreSQL databases, add descriptions manually or with AI assistance, and search your catalog using natural language.

## Features

- **Metadata Extraction**: Automatically catalog tables, views, columns, and their properties from PostgreSQL
- **Description Management**: Add and edit descriptions for all catalog items
- **AI-Powered Enrichment**: Generate descriptions using Claude (Anthropic) or Mistral (Ollama)
- **Semantic Search**: Find data assets by meaning using ChromaDB vector indexing
- **AI Chat**: Ask questions about your catalog in natural language
- **Bulk Operations**: Export/import descriptions via CSV for external processing
- **Run Tracking**: Monitor cataloger runs with progress tracking and history

## Workflow

### 1. Extract Metadata (Run Cataloger)
Connect to your databases and extract metadata into the catalog:
- Select a database source (configured in `.env`)
- Run the cataloger to extract schemas, tables, views, and columns
- Monitor progress in real-time
- View run history and clean up stuck runs

### 2. Browse & Document (Catalog)
Navigate and document your data assets:
- Browse hierarchy: Server > Database > Schema > Table > Column
- Edit descriptions inline with the pencil icon
- Use **Batch Edit** tab for bulk description updates
- Use **Cleanup** tab to remove stale/deleted objects

### 3. Bulk Enrich with AI (Bulk Operations)
Generate descriptions at scale:
- **Export**: Download tables/columns as CSV (with or without existing descriptions)
- **AI Generate**: Use Claude or Ollama to auto-generate descriptions
- **Import**: Upload enriched CSV to update catalog descriptions
- Filter by server, database, schema, and description status

### 4. Index for Search (Index)
Enable semantic search capabilities:
- Sync catalog descriptions to ChromaDB vector store
- View index statistics and status
- Clear and rebuild index as needed

### 5. Ask Questions (Ask)
Chat with your catalog:
- Ask natural language questions about your data
- Find relevant tables and columns by meaning
- Get contextual answers based on catalog descriptions

## Setup

### Prerequisites
- Python 3.10+
- PostgreSQL (for the catalog database)
- Access to source databases (PostgreSQL or SQL Server)

### Installation

```bash
# Clone the repository
git clone https://github.com/mark-notifica/DataNavigator.git
cd DataNavigator

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Copy the example environment file and configure your settings:

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:

```env
# Catalog database (PostgreSQL) - where metadata is stored
CATALOG_DB_HOST=localhost
CATALOG_DB_PORT=5432
CATALOG_DB_NAME=catalog
CATALOG_DB_USER=postgres
CATALOG_DB_PASSWORD=your_password

# Source databases to catalog (comma-separated list)
# Format: alias:type:host:port:database:user:password
DB_SOURCES=mydb:postgresql:localhost:5432:myapp:user:pass

# AI Configuration (optional - for description generation)
ANTHROPIC_API_KEY=sk-ant-...      # For Claude
OLLAMA_HOST=http://localhost:11434 # For local Ollama/Mistral
```

### Database Setup

Initialize the catalog database schema:

```bash
python -c "from storage import init_catalog_db; init_catalog_db()"
```

### Running the Application

```bash
# Development
streamlit run app.py

# Production (as Windows service)
# See start_streamlit.bat and use NSSM to install as service
```

The app runs on port 8501 by default (configured in `.streamlit/config.toml`).

## Project Structure

```
DataNavigator/
├── app.py                    # Home page
├── pages/
│   ├── 1_run_cataloger.py   # Metadata extraction
│   ├── 2_catalog.py         # Browse and edit catalog
│   ├── 3_bulk_operations.py # Export/import/AI enrichment
│   ├── 4_index.py           # Vector index management
│   ├── 5_ask.py             # AI chat interface
│   └── 99_help.py           # Help documentation
├── storage.py               # Catalog database operations
├── catalog_export.py        # CSV export/import
├── ai_enrichment.py         # AI description generation
├── vector_store.py          # ChromaDB operations
├── extract_*.py             # Database extractors
├── connection_db_*.py       # Database connections
├── .streamlit/
│   └── config.toml          # Streamlit configuration
├── start_streamlit.bat      # Windows startup script
└── requirements.txt         # Python dependencies
```

## Supported Databases

**Source databases** (for metadata extraction):
- PostgreSQL

**Catalog database** (for storing metadata):
- PostgreSQL

## AI Integration

DataNavigator supports two AI backends for description generation:

### Claude (Anthropic)
- Set `ANTHROPIC_API_KEY` in `.env`
- Uses Claude for high-quality description generation

### Ollama (Local)
- Install Ollama and pull a model (e.g., `ollama pull mistral`)
- Set `OLLAMA_HOST=http://localhost:11434` in `.env`
- Runs locally with no API costs

## Roadmap

Planned features for future releases:

- **Power BI TMDL support** - Catalog Power BI semantic models from TMDL files (investigate: parse into nodes vs. chunk whole files)
- **SQL Server support** - Metadata extraction from SQL Server databases
- **ETL script cataloging** - Extract and document ETL scripts and data pipelines (with chunking for long scripts)
- **DDL vectorization** - Investigate benefits of embedding view/procedure definitions for semantic search
- **Data lineage** - Track relationships and dependencies between tables
- **Scheduled cataloging** - Automatic periodic extraction runs
- **Incremental vector sync** - Only re-embed changed descriptions for faster indexing
- **Description approval workflow** - Review and approve AI-generated descriptions
- **API access** - REST API for programmatic catalog access

## License

MIT
