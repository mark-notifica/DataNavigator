import psycopg2
from datetime import datetime
import os
import argparse
import logging
from pathlib import Path
import sys
import re

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def load_db_config(path="servers_config.yaml"):
    import yaml
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config["catalog_db"]

def extract_dax_expression(lines, start_idx):
    expr_lines = []
    in_backticks = False
    i = start_idx
    logging.debug(f"Starting extract_dax_expression at line {start_idx}")
    while i < len(lines):
        line = lines[i].rstrip()
        logging.debug(f"Line {i}: {line}")
        # Start of triple backtick block
        if not in_backticks and line.strip().startswith("```"):
            in_backticks = True
            logging.debug(f"Entering backtick block at line {i}")
            i += 1
            continue
        # End of triple backtick block
        if in_backticks and line.strip().startswith("```"):
            logging.debug(f"Exiting backtick block at line {i}")
            i += 1  # <-- Advance index past closing ```
            break
        # End of measure properties (formatString, displayFolder, etc.)
        if not in_backticks and re.match(r"\s*(formatString|displayFolder|annotation|lineageTag):", line):
            logging.debug(f"Encountered property line at {i}: {line}")
            break
        expr_lines.append(line.strip())
        i += 1
    logging.debug(f"Extracted DAX expression (lines {start_idx}-{i}):\n{expr_lines}")
    return "\n".join(expr_lines).strip(), i

def parse_custom_tables_format(tables_dir):
    logging.info(f"Parsing tables in directory: {tables_dir}")
    tables = []
    for file in os.listdir(tables_dir):
        if not file.endswith(".tmdl"):
            continue
        table = {"columns": [], "measures": []}
        with open(os.path.join(tables_dir, file), "r", encoding="utf-8") as f:
            lines = f.readlines()
        current_table = None
        current_column = None
        current_measure = None
        mode = None
        i = 0
        while i < len(lines):
            line = lines[i].rstrip()

            if line.startswith("table "):
                if current_column:
                    table["columns"].append(current_column)
                    current_column = None
                if current_measure:
                    table["measures"].append(current_measure)
                    current_measure = None
                current_table = line.split(" ", 1)[1].strip()
                table["name"] = current_table
                mode = None
                i += 1
                continue

            elif line.startswith("\tcolumn "):
                if current_column:
                    table["columns"].append(current_column)
                current_column = {"name": line.split(" ", 1)[1].strip()}
                mode = "column"
                i += 1
                continue

            elif line.startswith("\tmeasure "):
                if current_measure:
                    table["measures"].append(current_measure)

                measure_def = line.split(" ", 1)[1].strip()
                if "=" in measure_def:
                    name_part, expr_part = measure_def.split("=", 1)
                    measure_name = name_part.strip().strip("'\"`")
                    dax_expr = expr_part.strip().strip("`'''").strip()
                else:
                    measure_name = measure_def.strip().strip("'\"`")
                    dax_expr = ""

                current_measure = {"name": measure_name}
                mode = "measure"
                i += 1

                # DAX expression
                if dax_expr:
                    current_measure["expression"] = dax_expr
                else:
                    dax_lines = []
                    while i < len(lines):
                        next_line = lines[i].rstrip()

                        # Lege regels zijn toegestaan, maar worden mee opgenomen
                        if next_line.strip() == "":
                            dax_lines.append("")
                            i += 1
                        elif next_line.startswith("\t\t\t"):
                            dax_lines.append(next_line.strip())
                            i += 1
                        else:
                            break  # Niet-DAX, dus mogelijk metadata

                    current_measure["expression"] = "\n".join(dax_lines).strip()


                # ✅ Deze blokken zijn nu *buiten* if/else → metadata wordt altijd gelezen
                while i < len(lines) and lines[i].strip() == "":
                    i += 1

                while i < len(lines):
                    meta_line = lines[i].rstrip()
                    meta_stripped = meta_line.lstrip()
                    logging.debug(f"Metadata parsing at line {i}: '{meta_line}' (stripped: '{meta_stripped}')")
                    if meta_stripped.startswith("formatString:"):
                        current_measure["formatString"] = meta_stripped.split(":", 1)[1].strip()
                    elif meta_stripped.startswith("displayFolder:"):
                        current_measure["displayFolder"] = meta_stripped.split(":", 1)[1].strip()
                    elif meta_stripped.startswith("lineageTag:"):
                        current_measure["lineageTag"] = meta_stripped.split(":", 1)[1].strip()
                    elif meta_stripped.startswith("annotation"):
                        pass  # ignore
                    elif meta_stripped == "isHidden":
                        current_measure["isHidden"] = True
                    elif meta_stripped == "isPrivate":
                        current_measure["isPrivate"] = True
                    elif meta_stripped == "isAvailableInMDX":
                        current_measure["isAvailableInMDX"] = True
                    else:
                        break
                    i += 1

                # table["measures"].append(current_measure)
                continue

            elif mode == "column" and line.startswith("\t\t") and current_column is not None:
                key_val = line.strip().split(":", 1)
                if len(key_val) == 2:
                    k, v = key_val[0].strip(), key_val[1].strip()
                    current_column[k] = v
                i += 1
                continue

            i += 1

        if current_column:
            table["columns"].append(current_column)
        tables.append(table)

    logging.info(f"Finished parsing tables in directory: {tables_dir}")
    return tables


def parse_relationships(path):
    relationships = []
    if not os.path.isfile(path):
        return relationships
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    current = {}
    for line in lines:
        line = line.strip()
        if line.startswith("relationship "):
            if current:
                relationships.append(current)
            current = {"id": line.split(" ", 1)[1].strip(), "isActive": True}
        elif line.startswith("isActive:"):
            current["isActive"] = line.split(":", 1)[1].strip().lower() == "true"
        elif line.startswith("fromColumn:"):
            from_full = line.split(":", 1)[1].strip()
            table, column = from_full.split(".", 1)
            current["fromTable"] = table.strip("'")
            current["fromColumn"] = column
        elif line.startswith("toColumn:"):
            to_full = line.split(":", 1)[1].strip()
            table, column = to_full.split(".", 1)
            current["toTable"] = table.strip("'")
            current["toColumn"] = column
    if current:
        relationships.append(current)
    return relationships

def insert_model(cur, name, filename):
    cur.execute("""
        INSERT INTO metadata.semantic_models (model_name, file_name, date_imported)
        VALUES (%s, %s, %s) RETURNING id
    """, (name, filename, datetime.now()))
    return cur.fetchone()[0]

def insert_tables(cur, model_id, tables):
    table_ids = {}
    for t in tables:
        cur.execute("""
            INSERT INTO metadata.semantic_tables (model_id, table_name)
            VALUES (%s, %s) RETURNING id
        """, (
            model_id,
            t.get("name")
        ))
        table_ids[t["name"]] = cur.fetchone()[0]
    return table_ids

def insert_columns(cur, table_ids, tables):
    for t in tables:
        for c in t.get("columns", []):
            cur.execute("""
                INSERT INTO metadata.semantic_columns (
                    semantic_table_id, column_name, data_type
                )
                VALUES (%s, %s, %s)
            """, (
                table_ids[t["name"]],
                c.get("name"),
                c.get("dataType")
            ))

def insert_measures(cur, table_ids, tables):
    for t in tables:
        for m in t.get("measures", []):
            print(m)
            cur.execute("""
                INSERT INTO metadata.semantic_measures (
                    semantic_table_id
                    , measure_name
                    , dax_expression
                    , format_string
                    , display_folder
                    , lineage_tag
                    , is_hidden
                    , is_private
                    , is_available_in_mdx
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                table_ids[t["name"]],
                m.get("name"),
                m.get("expression"),
                m.get("formatString"),
                m.get("displayFolder"),
                m.get("lineageTag"),
                m.get("isHidden", False),
                m.get("isPrivate", False),
                m.get("isAvailableInMDX", False)
            ))

def insert_relationships(cur, model_id, relationships):
    for r in relationships:
        cur.execute("""
            INSERT INTO metadata.semantic_relationships (
                model_id, from_table, from_column, to_table, to_column, is_active
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            model_id,
            r["fromTable"],
            r["fromColumn"],
            r["toTable"],
            r["toColumn"],
            r["isActive"]
        ))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='Path to extracted Power BI project folder (e.g. SSM_project)')
    parser.add_argument('--config', default='servers_config.yaml', help='YAML config file for DB connection')
    args = parser.parse_args()

    if not os.path.isdir(args.project):
        logger.error(f"❌ Project folder not found: {args.project}")
        sys.exit(1)

    if not os.path.isfile(args.config):
        logger.error(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)

    with open(args.config, "r", encoding="utf-8") as f:
        try:
            import yaml
            config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"❌ Failed to read YAML config: {e}")
            sys.exit(1)

    try:
        conn = psycopg2.connect(**config["catalog_db"])
        conn.close()
        logger.info("✅ Database connection successful")
    except Exception as e:
        logger.error(f"❌ Failed to connect to database: {e}")
        sys.exit(1)

    project_path = Path(args.project)
    definition_path = project_path / "SSM.SemanticModel" / "definition"
    model_name = "SSM.SemanticModel"
    tables_dir = definition_path / "tables"
    relationships_path = definition_path / "relationships.tmdl"

    tables = parse_custom_tables_format(tables_dir)
    relationships = parse_relationships(relationships_path)

    db_config = load_db_config(args.config)
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    try:
        model_id = insert_model(cur, model_name, str(definition_path))
        table_ids = insert_tables(cur, model_id, tables)
        insert_columns(cur, table_ids, tables)
        insert_measures(cur, table_ids, tables)
        insert_relationships(cur, model_id, relationships)

        conn.commit()
        logger.info(f"✅ Model '{model_name}' successfully imported (ID: {model_id})")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Import error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
