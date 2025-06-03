# DataNavigator

De DataNavigator combineert:
- **Uniforme Datamodel Generator**: voor het automatisch genereren van consistente SQL-datamodellen
- **Data Catalogus**: voor het inzichtelijk maken van beschikbare databronnen en kolommen

🎯 Doel: inzicht bieden in wat er is en wat nodig is, vanuit zowel brondata als informatiebehoefte.

## Structuur
- `datamodel_generator/` – logica voor het genereren van modellen
- `data_catalog/` – extractie en opslag van metadata

## Data Catalog Extractor

A version-aware PostgreSQL metadata extraction tool that catalogs schemas, tables, and columns from remote PostgreSQL servers into a central metadata repository. Built for use across multiple environments (e.g. VPS1, VPS3) and stores results in a central catalog (e.g. on VPS2).

---

### 🔧 Features

* Extracts metadata from any number of PostgreSQL databases
* Tracks current and previous versions with `curr_id`, `date_created`, `date_updated`, and `date_deleted`
* Differentiates between updated and deleted records:
  * If metadata changes: **old row is marked inactive**, new version inserted
  * If metadata disappears: **old row is marked as deleted**
* CLI filters for selecting specific server or database
* Auto-generates summary after execution
* Logs to rotating timestamped files **and** the console
* Designed to support later external descriptions, UI browsing, and reporting

---

### 🛠 Requirements

* Python 3.10+
* PostgreSQL 13+
* Python packages:

  ```bash
  pip install psycopg2-binary pyyaml
  ```
* Permissions:

  * The user connecting to remote PostgreSQL DBs must have read access to `information_schema`
  * The catalog DB user must have `SELECT`, `INSERT`, `UPDATE` on the `metadata` schema tables and sequences

---

### 🗂 File Structure

```
data-catalog/
├── extract_metadata.py       # Main runner script
├── extract_metadata_updated.py # Extended versioning + deletion logic
├── servers_config.yaml       # Host/credential config
├── catalog_extraction_*.log  # Generated log files
└── README.md                 # This file
```

---

## ▶️ Usage

```bash
python extract_metadata.py \
  --server 10.3.152.2 \
  --dbname ENK_DEV1
```

#### Optional CLI Arguments

| Flag       | Description                                |
| ---------- | ------------------------------------------ |
| `--server` | Filter to a specific PostgreSQL host       |
| `--dbname` | Filter to a specific database on that host |

---

### 📝 Configuration (`servers_config.yaml`)

```yaml
servers:
  - name: VPS1
    host: 10.3.152.2
    user: postgres
    password: your_password

  - name: VPS3
    host: 10.3.152.9
    user: postgres
    password: your_password

catalog_db:
  host: localhost
  dbname: data_catalog
  user: catalog_user
  password: catalog_password
```

---

### 🧠 Versioning Logic

Each metadata object (database, schema, table, column) is versioned with:

* `curr_id = 'Y'` → Current active version
* `curr_id = 'N'` → Replaced or deleted
* `date_updated` → When a **change** was detected
* `date_deleted` → When an object is **no longer present** in source

Tracked by compound uniqueness (no DB constraints needed):

- `catalog_databases`: `server_address + database_name`
- `catalog_schemas`: `server_address + database_name + schema_name`
- `catalog_tables`: `server_address + database_name + schema_name + table_name`
- `catalog_columns`: `server_address + database_name + schema_name + table_name + column_name`

> ℹ️ Note: Since new versions are inserted when changes are detected, an object may appear multiple times with `curr_id = 'N'` in historical form, and once with `curr_id = 'Y'` if still active.

---

### ✅ Sample Output

```bash
Summary:
Databases Added: 1
Schemas Added: 6
Tables Added: 393
Tables Updated: 2
Columns Added: 4279
Columns Updated: 3
Tables Deleted: 1
Columns Deleted: 17
```

Logs are saved to:

```
catalog_extraction_20250507_103000.log
```
---
## 2. Descriptions and Annotation

To support documentation and reporting, optional external descriptions can be linked to catalog entries.

Each catalog level (database, schema, table, column) has a corresponding `_descriptions` table:

- `catalog_database_descriptions`
- `catalog_schema_descriptions`
- `catalog_table_descriptions`
- `catalog_column_descriptions`

Each table includes:

* Descriptive fields like `description_short`, `description_long`
* Administrative metadata: `author_created`, `author_updated`, `date_updated`

### Linking Logic

Descriptions are linked using the compound uniqueness of the corresponding catalog objects:

- `server_address + database_name`
- `server_address + database_name + schema_name`
- `server_address + database_name + schema_name + table_name`
- `server_address + database_name + schema_name + table_name + column_name`

This allows description rows to stay attached to the most recent version (`curr_id = 'Y'`) of a catalog entry.

Descriptions do **not** use foreign keys and are preserved across version changes.

---


## 🔄 3. Next Features (Optional)

* External reference tables for descriptions
* HTML/React UI to browse the catalog
* JSON/CSV export

