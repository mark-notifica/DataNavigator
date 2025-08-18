import psycopg2
from datetime import datetime
import os
import argparse
import logging
from pathlib import Path
import sys
import re
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection config (same as database cataloger)
CATALOG_DB_CONFIG = {
    'host': os.getenv('NAV_DB_HOST'),
    'port': os.getenv('NAV_DB_PORT'),
    'database': os.getenv('NAV_DB_NAME'),  
    'user': os.getenv('NAV_DB_USER'),
    'password': os.getenv('NAV_DB_PASSWORD')
}

# Get logger (will be configured later by setup_logging_with_run_id)
logger = logging.getLogger(__name__)

ENV_PATTERN = re.compile(r'^\${(.+)}$')

def _resolve_env(obj):
    if isinstance(obj, dict):
        return {k: _resolve_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env(v) for v in obj]
    if isinstance(obj, str):
        m = ENV_PATTERN.match(obj)
        if m:
            return os.getenv(m.group(1), obj)
    return obj

def setup_logging_with_run_id(catalog_run_id=None):
    """Setup logging with optional run ID in filename (same as database cataloger)"""
    # Create logfiles directory structure
    script_dir = Path(__file__).parent  # data_catalog directory
    log_dir = script_dir / 'logfiles' / 'powerbi_semanticmodel'
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Include run ID in log filename if available
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if catalog_run_id:
        log_filename = log_dir / f"powerbi_catalog_{timestamp}_run_{catalog_run_id}.log"
    else:
        log_filename = log_dir / f"powerbi_catalog_{timestamp}.log"
    
    # Setup new handlers
    from logging.handlers import RotatingFileHandler
    log_handler = RotatingFileHandler(str(log_filename), maxBytes=5*1024*1024, backupCount=5)
    console_handler = logging.StreamHandler()
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[log_handler, console_handler],
        force=True
    )
    
    # Return path relative to PROJECT ROOT
    project_root = script_dir.parent
    relative_path = log_filename.relative_to(project_root)
    
    return str(relative_path)

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

def extract_m_code_from_tmdl(file_path):
    """Extract M-code partition information from .tmdl file"""
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    partitions = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for partition definition
        if line.startswith("partition ") and " = m" in line:
            partition_name = line.split(" = m")[0].replace("partition ", "").strip()
            
            partition_info = {
                "name": partition_name,
                "mode": None,
                "query_group": None,
                "m_expression": ""
            }
            
            i += 1
            
            # Parse partition metadata
            while i < len(lines) and not lines[i].strip().startswith("source ="):
                line = lines[i].strip()
                
                if line.startswith("mode:"):
                    partition_info["mode"] = line.split(":", 1)[1].strip()
                elif line.startswith("queryGroup:"):
                    # Remove quotes from queryGroup
                    query_group = line.split(":", 1)[1].strip().strip("'\"")
                    partition_info["query_group"] = query_group
                
                i += 1
            
            # Extract M expression (starts after "source =")
            if i < len(lines) and lines[i].strip().startswith("source ="):
                i += 1  # Skip "source =" line
                
                m_lines = []
                indent_level = None
                
                while i < len(lines):
                    line = lines[i].rstrip()
                    
                    # Determine initial indentation
                    if indent_level is None and line.strip():
                        indent_level = len(line) - len(line.lstrip())
                    
                    # Check if we've reached the end of the M expression
                    if line.strip() and len(line) - len(line.lstrip()) <= indent_level - 1:
                        if not line.strip().startswith("let") and not line.strip().startswith("in"):
                            break
                    
                    # Add line to M expression (remove base indentation)
                    if line.strip():
                        if len(line) >= indent_level:
                            m_lines.append(line[indent_level:])
                        else:
                            m_lines.append(line.strip())
                    else:
                        m_lines.append("")
                    
                    i += 1
                
                partition_info["m_expression"] = "\n".join(m_lines).strip()
            
            partitions.append(partition_info)
        else:
            i += 1
    
    return partitions

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

def insert_model(cur, name, filename, catalog_run_id=None):
    cur.execute("""
        INSERT INTO catalog.pbi_models (model_name, file_name, date_imported, catalog_run_id)
        VALUES (%s, %s, %s, %s) RETURNING id
    """, (name, filename, datetime.now(), catalog_run_id))
    return cur.fetchone()[0]

def insert_tables(cur, model_id, tables, catalog_run_id=None):
    table_ids = {}
    for t in tables:
        cur.execute("""
            INSERT INTO catalog.pbi_tables (model_id, table_name, catalog_run_id)
            VALUES (%s, %s, %s) RETURNING id
        """, (
            model_id,
            t.get("name"),
            catalog_run_id
        ))
        table_ids[t["name"]] = cur.fetchone()[0]
    return table_ids

def insert_columns(cur, table_ids, tables, catalog_run_id=None):
    for t in tables:
        for c in t.get("columns", []):
            cur.execute("""
                INSERT INTO catalog.pbi_columns (
                    semantic_table_id, column_name, data_type, catalog_run_id
                )
                VALUES (%s, %s, %s, %s)
            """, (
                table_ids[t["name"]],
                c.get("name"),
                c.get("dataType"),
                catalog_run_id
            ))

def insert_measures(cur, table_ids, tables, catalog_run_id=None):
    for t in tables:
        for m in t.get("measures", []):
            print(m)
            cur.execute("""
                INSERT INTO catalog.pbi_measures (
                    semantic_table_id
                    , measure_name
                    , dax_expression
                    , format_string
                    , display_folder
                    , lineage_tag
                    , is_hidden
                    , is_private
                    , is_available_in_mdx
                    , catalog_run_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                table_ids[t["name"]],
                m.get("name"),
                m.get("expression"),
                m.get("formatString"),
                m.get("displayFolder"),
                m.get("lineageTag"),
                m.get("isHidden", False),
                m.get("isPrivate", False),
                m.get("isAvailableInMDX", False),
                catalog_run_id
            ))

def insert_relationships(cur, model_id, relationships, catalog_run_id=None):
    for r in relationships:
        cur.execute("""
            INSERT INTO catalog.pbi_relationships (
                model_id, from_table, from_column, to_table, to_column, is_active, catalog_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            model_id,
            r["fromTable"],
            r["fromColumn"],
            r["toTable"],
            r["toColumn"],
            r["isActive"],
            catalog_run_id
        ))

def insert_model(cur, name, catalog_run_id):
    """Insert semantic model or return existing ID if already exists"""
    
    # Check if model already exists
    cur.execute("""
        SELECT id FROM catalog.pbi_models 
        WHERE model_name = %s
    """, (name,))
    
    existing = cur.fetchone()
    
    if existing:
        model_id = existing[0]
        # Update catalog_run_id to show it was processed in this run
        cur.execute("""
            UPDATE catalog.pbi_models 
            SET catalog_run_id = %s
            WHERE id = %s
        """, (catalog_run_id, model_id))
        
        logger.info(f"Found existing semantic model: {name} (ID: {model_id})")
        return model_id
    else:
        # New model
        cur.execute("""
            INSERT INTO catalog.pbi_models 
            (model_name, catalog_run_id)
            VALUES (%s, %s) RETURNING id
        """, (name, catalog_run_id))
        
        model_id = cur.fetchone()[0]
        logger.info(f"Inserted new semantic model: {name} (ID: {model_id})")
        return model_id
    
def upsert_semantic_tables_temporal_with_summary(catalog_conn, model_id, table_info, catalog_run_id):
    """Upsert semantic table with temporal versioning and return operation type"""
    with catalog_conn.cursor() as cursor:
        table_name = table_info.get("name")
        
        # Check for existing current record
        cursor.execute("""
            SELECT id, display_folder, is_hidden, source_table
            FROM catalog.pbi_tables 
            WHERE model_id = %s AND table_name = %s AND is_current = true
        """, (model_id, table_name))
        
        existing = cursor.fetchone()
        
        if existing:
            existing_id, existing_display_folder, existing_is_hidden, existing_source_table = existing
            
            # Check if anything changed
            current_display_folder = table_info.get("display_folder")
            current_is_hidden = table_info.get("is_hidden")
            current_source_table = table_info.get("source_table")
            
            if (existing_display_folder != current_display_folder or 
                existing_is_hidden != current_is_hidden or 
                existing_source_table != current_source_table):
                
                # Mark old record as not current
                cursor.execute("""
                    UPDATE catalog.pbi_tables 
                    SET is_current = false, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (existing_id,))
                
                # Insert new record
                cursor.execute("""
                    INSERT INTO catalog.pbi_tables 
                    (model_id, table_name, display_folder, is_hidden, source_table, catalog_run_id, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (model_id, table_name, current_display_folder, current_is_hidden, current_source_table, catalog_run_id, True))
                
                new_id = cursor.fetchone()[0]
                logger.info(f"Updated semantic table: {table_name} (old ID: {existing_id}, new ID: {new_id})")
                return new_id, 'updated'  # Add operation type
            else:
                # No changes, just update catalog_run_id
                cursor.execute("""
                    UPDATE catalog.pbi_tables 
                    SET catalog_run_id = %s, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (catalog_run_id, existing_id))
                
                logger.debug(f"No changes for semantic table: {table_name} (ID: {existing_id})")
                return existing_id, 'unchanged'  # Add operation type
        else:
            # New table
            cursor.execute("""
                INSERT INTO catalog.pbi_tables 
                (model_id, table_name, display_folder, is_hidden, source_table, catalog_run_id, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (model_id, table_name, table_info.get("display_folder"), table_info.get("is_hidden"), table_info.get("source_table"), catalog_run_id, True))
            
            new_id = cursor.fetchone()[0]
            logger.info(f"Inserted new semantic table: {table_name} (ID: {new_id})")
            return new_id, 'added'
        

def upsert_semantic_columns_temporal_with_summary(catalog_conn, semantic_table_id, column_info, catalog_run_id):
    """Upsert semantic column with temporal versioning and return operation type"""
    with catalog_conn.cursor() as cursor:
        column_name = column_info.get("name")
        
        # Check for existing current record
        cursor.execute("""
            SELECT id, data_type, is_hidden, format_string, display_folder
            FROM catalog.pbi_columns 
            WHERE semantic_table_id = %s AND column_name = %s AND is_current = true
        """, (semantic_table_id, column_name))
        
        existing = cursor.fetchone()
        
        if existing:
            existing_id, existing_data_type, existing_is_hidden, existing_format_string, existing_display_folder = existing
            
            # Check if anything changed
            current_data_type = column_info.get("dataType")
            current_is_hidden = column_info.get("is_hidden")
            current_format_string = column_info.get("format_string")
            current_display_folder = column_info.get("display_folder")
            
            if (existing_data_type != current_data_type or 
                existing_is_hidden != current_is_hidden or 
                existing_format_string != current_format_string or 
                existing_display_folder != current_display_folder):
                
                # Mark old record as not current
                cursor.execute("""
                    UPDATE catalog.pbi_columns 
                    SET is_current = false, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (existing_id,))
                
                # Insert new record
                cursor.execute("""
                    INSERT INTO catalog.pbi_columns 
                    (semantic_table_id, column_name, data_type, is_hidden, format_string, display_folder, catalog_run_id, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (semantic_table_id, column_name, current_data_type, current_is_hidden, current_format_string, current_display_folder, catalog_run_id, True))
                
                new_id = cursor.fetchone()[0]
                logger.info(f"Updated semantic column: {column_name} (old ID: {existing_id}, new ID: {new_id})")
                return new_id, 'updated'
            else:
                # No changes, just update catalog_run_id
                cursor.execute("""
                    UPDATE catalog.pbi_columns 
                    SET catalog_run_id = %s, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (catalog_run_id, existing_id))
                
                logger.debug(f"No changes for semantic column: {column_name} (ID: {existing_id})")
                return existing_id, 'unchanged'
        else:
            # New column
            cursor.execute("""
                INSERT INTO catalog.pbi_columns 
                (semantic_table_id, column_name, data_type, is_hidden, format_string, display_folder, catalog_run_id, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (semantic_table_id, column_name, column_info.get("dataType"), column_info.get("is_hidden"), column_info.get("format_string"), column_info.get("display_folder"), catalog_run_id, True))
            
            new_id = cursor.fetchone()[0]
            logger.info(f"Inserted new semantic column: {column_name} (ID: {new_id})")
            return new_id, 'added'


def upsert_semantic_measures_temporal_with_summary(catalog_conn, semantic_table_id, measure_info, catalog_run_id):
    """Upsert semantic measure with temporal versioning and return operation type"""
    with catalog_conn.cursor() as cursor:
        measure_name = measure_info.get("name")
        
        # Check for existing current record
        cursor.execute("""
            SELECT id, dax_expression, format_string, display_folder, lineage_tag, is_hidden, is_private, is_available_in_mdx
            FROM catalog.pbi_measures 
            WHERE semantic_table_id = %s AND measure_name = %s AND is_current = true
        """, (semantic_table_id, measure_name))
        
        existing = cursor.fetchone()
        
        if existing:
            existing_id, existing_dax, existing_format, existing_folder, existing_lineage, existing_hidden, existing_private, existing_mdx = existing
            
            # Check if anything changed
            current_dax = measure_info.get("expression")
            current_format = measure_info.get("formatString")
            current_folder = measure_info.get("displayFolder")
            current_lineage = measure_info.get("lineageTag")
            current_hidden = measure_info.get("isHidden", False)
            current_private = measure_info.get("isPrivate", False)
            current_mdx = measure_info.get("isAvailableInMDX", False)
            
            if (existing_dax != current_dax or existing_format != current_format or 
                existing_folder != current_folder or existing_lineage != current_lineage or
                existing_hidden != current_hidden or existing_private != current_private or
                existing_mdx != current_mdx):
                
                # Mark old record as not current
                cursor.execute("""
                    UPDATE catalog.pbi_measures 
                    SET is_current = false, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (existing_id,))
                
                # Insert new record
                cursor.execute("""
                    INSERT INTO catalog.pbi_measures 
                    (semantic_table_id, measure_name, dax_expression, format_string, display_folder, 
                     lineage_tag, is_hidden, is_private, is_available_in_mdx, catalog_run_id, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (semantic_table_id, measure_name, current_dax, current_format, current_folder,
                      current_lineage, current_hidden, current_private, current_mdx, catalog_run_id, True))
                
                new_id = cursor.fetchone()[0]
                logger.info(f"Updated semantic measure: {measure_name} (old ID: {existing_id}, new ID: {new_id})")
                return new_id, 'updated'
            else:
                # No changes, just update catalog_run_id
                cursor.execute("""
                    UPDATE catalog.pbi_measures 
                    SET catalog_run_id = %s, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (catalog_run_id, existing_id))
                
                logger.debug(f"No changes for semantic measure: {measure_name} (ID: {existing_id})")
                return existing_id, 'unchanged'
        else:
            # New measure
            cursor.execute("""
                INSERT INTO catalog.pbi_measures 
                (semantic_table_id, measure_name, dax_expression, format_string, display_folder, 
                 lineage_tag, is_hidden, is_private, is_available_in_mdx, catalog_run_id, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (semantic_table_id, measure_info.get("name"), measure_info.get("expression"),
                  measure_info.get("formatString"), measure_info.get("displayFolder"),
                  measure_info.get("lineageTag"), measure_info.get("isHidden", False),
                  measure_info.get("isPrivate", False), measure_info.get("isAvailableInMDX", False),
                  catalog_run_id, True))
            
            new_id = cursor.fetchone()[0]
            logger.info(f"Inserted new semantic measure: {measure_info.get('name')} (ID: {new_id})")
            return new_id, 'added'

def upsert_semantic_relationships_temporal_with_summary(catalog_conn, model_id, relationship_info, catalog_run_id):
    """Upsert semantic relationship with temporal versioning and return operation type"""
    with catalog_conn.cursor() as cursor:
        from_table = relationship_info["fromTable"]
        from_column = relationship_info["fromColumn"]
        to_table = relationship_info["toTable"]
        to_column = relationship_info["toColumn"]
        
        # Check for existing current record
        cursor.execute("""
            SELECT id, is_active, relationship_type, cross_filter
            FROM catalog.pbi_relationships 
            WHERE model_id = %s AND from_table = %s AND from_column = %s 
            AND to_table = %s AND to_column = %s AND is_current = true
        """, (model_id, from_table, from_column, to_table, to_column))
        
        existing = cursor.fetchone()
        
        if existing:
            existing_id, existing_active, existing_type, existing_filter = existing
            
            # Check if anything changed
            current_active = relationship_info.get("isActive", True)
            current_type = relationship_info.get("relationship_type")
            current_filter = relationship_info.get("cross_filter")
            
            if (existing_active != current_active or existing_type != current_type or existing_filter != current_filter):
                
                # Mark old record as not current
                cursor.execute("""
                    UPDATE catalog.pbi_relationships 
                    SET is_current = false, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (existing_id,))
                
                # Insert new record
                cursor.execute("""
                    INSERT INTO catalog.pbi_relationships 
                    (model_id, from_table, from_column, to_table, to_column, is_active, 
                     relationship_type, cross_filter, catalog_run_id, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (model_id, from_table, from_column, to_table, to_column, current_active, 
                      current_type, current_filter, catalog_run_id, True))
                
                new_id = cursor.fetchone()[0]
                logger.info(f"Updated semantic relationship: {from_table}.{from_column} -> {to_table}.{to_column} (old ID: {existing_id}, new ID: {new_id})")
                return new_id, 'updated'
            else:
                # No changes, just update catalog_run_id
                cursor.execute("""
                    UPDATE catalog.pbi_relationships 
                    SET catalog_run_id = %s, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (catalog_run_id, existing_id))
                
                logger.debug(f"No changes for semantic relationship: {from_table}.{from_column} -> {to_table}.{to_column} (ID: {existing_id})")
                return existing_id, 'unchanged'
        else:
            # New relationship
            cursor.execute("""
                INSERT INTO catalog.pbi_relationships 
                (model_id, from_table, from_column, to_table, to_column, is_active, 
                 relationship_type, cross_filter, catalog_run_id, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (model_id, from_table, from_column, to_table, to_column, relationship_info.get("isActive", True),
                  relationship_info.get("relationship_type"), relationship_info.get("cross_filter"), catalog_run_id, True))
            
            new_id = cursor.fetchone()[0]
            logger.info(f"Inserted new semantic relationship: {from_table}.{from_column} -> {to_table}.{to_column} (ID: {new_id})")
            return new_id, 'added'
        
def extract_m_code_from_tmdl(file_path):
    """Extract M-code partition information from .tmdl file"""
    logger.debug(f"Extracting M-code from: {file_path}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    logger.debug(f"File has {len(lines)} lines")
    
    partitions = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for partition definition
        if line.startswith("partition ") and " = m" in line:
            partition_name = line.split(" = m")[0].replace("partition ", "").strip()
            
            # Remove quotes if present
            if partition_name.startswith("'") and partition_name.endswith("'"):
                partition_name = partition_name[1:-1]
            elif partition_name.startswith('"') and partition_name.endswith('"'):
                partition_name = partition_name[1:-1]
            
            logger.debug(f"Found partition: '{partition_name}' at line {i}")
            
            partition_info = {
                "name": partition_name,
                "mode": None,
                "query_group": None,
                "m_expression": ""
            }
            
            i += 1
            
            # Parse partition metadata
            while i < len(lines) and not lines[i].strip().startswith("source ="):
                line = lines[i].strip()
                
                if line.startswith("mode:"):
                    partition_info["mode"] = line.split(":", 1)[1].strip()
                    logger.debug(f"  Mode: {partition_info['mode']}")
                elif line.startswith("queryGroup:"):
                    # Remove quotes from queryGroup
                    query_group = line.split(":", 1)[1].strip().strip("'\"")
                    partition_info["query_group"] = query_group
                    logger.debug(f"  Query Group: {partition_info['query_group']}")
                
                i += 1
            
            # Extract M expression (starts after "source =")
            if i < len(lines) and lines[i].strip().startswith("source ="):
                logger.debug(f"  Found 'source =' at line {i}")
                i += 1  # Skip "source =" line
                
                m_lines = []
                indent_level = None
                
                while i < len(lines):
                    line = lines[i].rstrip()
                    
                    # Determine initial indentation
                    if indent_level is None and line.strip():
                        indent_level = len(line) - len(line.lstrip())
                        logger.debug(f"  M-code indent level: {indent_level}")
                    
                    # Check if we've reached the end of the M expression
                    if line.strip() and len(line) - len(line.lstrip()) <= indent_level - 1:
                        if not line.strip().startswith("let") and not line.strip().startswith("in"):
                            logger.debug(f"  End of M-code at line {i}")
                            break
                    
                    # Add line to M expression (remove base indentation)
                    if line.strip():
                        if len(line) >= indent_level:
                            m_lines.append(line[indent_level:])
                        else:
                            m_lines.append(line.strip())
                    else:
                        m_lines.append("")
                    
                    i += 1
                
                partition_info["m_expression"] = "\n".join(m_lines).strip()
                logger.debug(f"  M-code length: {len(partition_info['m_expression'])} characters")
            
            partitions.append(partition_info)
        else:
            i += 1
    
    logger.debug(f"Total partitions found in {file_path}: {len(partitions)}")
    return partitions

def upsert_semantic_m_code_temporal_with_summary(catalog_conn, semantic_table_id, m_code_info, catalog_run_id):
    """Upsert semantic M-code with temporal versioning and return operation type"""
    with catalog_conn.cursor() as cursor:
        partition_name = m_code_info.get("name")
        
        # Check for existing current record
        cursor.execute("""
            SELECT id, mode, query_group, m_expression
            FROM catalog.pbi_m_code 
            WHERE semantic_table_id = %s AND partition_name = %s AND is_current = true
        """, (semantic_table_id, partition_name))
        
        existing = cursor.fetchone()
        
        if existing:
            existing_id, existing_mode, existing_query_group, existing_m_expression = existing
            
            # Check if anything changed
            current_mode = m_code_info.get("mode")
            current_query_group = m_code_info.get("query_group")
            current_m_expression = m_code_info.get("m_expression")
            
            if (existing_mode != current_mode or 
                existing_query_group != current_query_group or 
                existing_m_expression != current_m_expression):
                
                # Mark old record as not current
                cursor.execute("""
                    UPDATE catalog.pbi_m_code 
                    SET is_current = false, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (existing_id,))
                
                # Insert new record
                cursor.execute("""
                    INSERT INTO catalog.pbi_m_code 
                    (semantic_table_id, partition_name, mode, query_group, m_expression, catalog_run_id, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (semantic_table_id, partition_name, current_mode, current_query_group, current_m_expression, catalog_run_id, True))
                
                new_id = cursor.fetchone()[0]
                logger.debug(f"Updated M-code partition: {partition_name} (old ID: {existing_id}, new ID: {new_id})")
                return new_id, 'updated'
            else:
                # No changes, just update catalog_run_id
                cursor.execute("""
                    UPDATE catalog.pbi_m_code 
                    SET catalog_run_id = %s, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (catalog_run_id, existing_id))
                
                logger.debug(f"No changes for M-code partition: {partition_name} (ID: {existing_id})")
                return existing_id, 'unchanged'
        else:
            # New M-code partition
            cursor.execute("""
                INSERT INTO catalog.pbi_m_code 
                (semantic_table_id, partition_name, mode, query_group, m_expression, catalog_run_id, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (semantic_table_id, partition_name, m_code_info.get("mode"), m_code_info.get("query_group"), m_code_info.get("m_expression"), catalog_run_id, True))
            
            new_id = cursor.fetchone()[0]
            logger.debug(f"Inserted new M-code partition: {partition_name} (ID: {new_id})")
            return new_id, 'added'

def process_m_code_for_model_with_summary(catalog_conn, table_ids, tables_dir, catalog_run_id):
    """Process M-code partitions for all tables with change tracking and return summary"""
    
    summary = {'added': 0, 'updated': 0, 'deleted': 0, 'total_processed': 0}
    
    for table_name, table_id in table_ids.items():
        # Get existing M-code partitions for this table
        with catalog_conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, partition_name 
                FROM catalog.pbi_m_code 
                WHERE semantic_table_id = %s AND is_current = true
            """, (table_id,))
            
            existing_partitions = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Look for corresponding .tmdl file
        tmdl_file = Path(tables_dir) / f"{table_name}.tmdl"
        current_partitions = set()
        
        if tmdl_file.exists():
            try:
                partitions = extract_m_code_from_tmdl(tmdl_file)
                
                for partition in partitions:
                    partition_name = partition.get("name")
                    current_partitions.add(partition_name)
                    
                    # Upsert the partition and get operation type
                    _, operation = upsert_semantic_m_code_temporal_with_summary(catalog_conn, table_id, partition, catalog_run_id)
                    if operation != 'unchanged':
                        summary[operation] += 1
                    summary['total_processed'] += 1  # Track total processed
                
            except Exception as e:
                logger.error(f"Error processing M-code for table {table_name}: {e}")
        
        # Mark deleted partitions (existed before but not in current run)
        deleted_partitions = set(existing_partitions.keys()) - current_partitions
        for deleted_partition_name in deleted_partitions:
            deleted_partition_id = existing_partitions[deleted_partition_name]
            with catalog_conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE catalog.pbi_m_code 
                    SET is_current = false, date_deleted = CURRENT_TIMESTAMP, 
                        deleted_by_catalog_run_id = %s, date_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (catalog_run_id, deleted_partition_id))
            
            summary['deleted'] += 1
            logger.info(f"Marked M-code partition as deleted: {table_name}.{deleted_partition_name}")
    
    return summary


def get_catalog_connection():
    """Get connection to the DataNavigator catalog database"""
    try:
        conn = psycopg2.connect(**CATALOG_DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to catalog database: {e}")
        raise

def start_powerbi_catalog_run(catalog_conn, connection_info, project_folder):
    """Start a PowerBI catalog run"""
    with catalog_conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO catalog.pbi_catalog_runs 
            (connection_id, connection_name, connection_type, connection_host, connection_port, 
             databases_to_catalog, databases_count, run_started_at, run_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'running')
            RETURNING id
        """, (
            connection_info['id'],
            connection_info['name'],
            'Power BI Semantic Model',
            project_folder,  # Use project folder as "host"
            None,  # No port for PowerBI
            f'PowerBI Project: {Path(project_folder).name}',
            1  # One "database" (the PowerBI project)
        ))
        
        run_id = cursor.fetchone()[0]
        
        # Setup logging with run ID (returns relative path)
        relative_log_filename = setup_logging_with_run_id(run_id)
        
        # Store relative log filename in database
        cursor.execute("""
            UPDATE catalog.pbi_catalog_runs 
            SET log_filename = %s
            WHERE id = %s
        """, (relative_log_filename, run_id))
        
        logger.info(f"Started PowerBI catalog run {run_id} for connection {connection_info['name']}")
        logger.info(f"Project folder: {project_folder}")
        logger.info(f"Relative log file path: {relative_log_filename}")
        return run_id

def complete_powerbi_catalog_run(catalog_conn, run_id, processed_counts):
    """Mark PowerBI catalog run as completed with processed counts"""
    try:
        with catalog_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE catalog.pbi_catalog_runs 
                SET run_completed_at = CURRENT_TIMESTAMP,
                    run_status = 'completed',
                    databases_processed = 1,
                    models_processed = %s,
                    tables_processed = %s,
                    columns_processed = %s,
                    measures_processed = %s,
                    relationships_processed = %s,
                    m_code_processed = %s
                WHERE id = %s
            """, (
                1,  # models_processed - always 1 for successful PowerBI cataloging
                processed_counts.get('tables_processed', 0),
                processed_counts.get('columns_processed', 0),
                processed_counts.get('measures_processed', 0),
                processed_counts.get('relationships_processed', 0),
                processed_counts.get('m_code_processed', 0),
                run_id
            ))
            
            catalog_conn.commit()
            logger.info(f"Successfully completed PowerBI catalog run {run_id}")
            
    except Exception as e:
        logger.error(f"Failed to complete PowerBI catalog run: {e}")
        catalog_conn.rollback()
        raise

def fail_catalog_run(catalog_conn, run_id, error_message):
    """Mark catalog run as failed"""
    logger.info(f"Marking catalog run {run_id} as failed")
    
    try:
        with catalog_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE catalog.pbi_catalog_runs 
                SET run_completed_at = CURRENT_TIMESTAMP,
                    run_status = 'failed',
                    error_message = %s
                WHERE id = %s
            """, (error_message, run_id))
            
            catalog_conn.commit()
            logger.info(f"Marked catalog run {run_id} as failed")
            
    except Exception as e:
        logger.error(f"Failed to mark run as failed: {e}")
        try:
            catalog_conn.rollback()
        except:
            pass

def process_powerbi_project(project_folder, catalog_run_id):
    """Process PowerBI project files using upsert functions with temporal versioning"""
    logger.info(f"Processing PowerBI project folder: {project_folder}")
    
    # Initialize summary counters
    summary = {
        'tables_added': 0,
        'tables_updated': 0, 
        'tables_deleted': 0,
        'columns_added': 0,
        'columns_updated': 0,
        'columns_deleted': 0,
        'measures_added': 0,
        'measures_updated': 0,
        'measures_deleted': 0,
        'relationships_added': 0,
        'relationships_updated': 0,
        'relationships_deleted': 0,
        'm_code_added': 0,
        'm_code_updated': 0,
        'm_code_deleted': 0
    }

    processed_counts = {
    'tables_processed': 0,
    'columns_processed': 0, 
    'measures_processed': 0,
    'relationships_processed': 0,
    'm_code_processed': 0
    }

    try:
        # Build expected folder structure
        project_path = Path(project_folder)
        project_name = project_path.name  # e.g., "SSM_postgres"
        
        # PowerBI project structure: SSM_postgres\SSM_postgres.SemanticModel\definition\tables
        semantic_model_path = project_path / f"{project_name}.SemanticModel"
        definition_path = semantic_model_path / "definition"
        tables_dir = definition_path / "tables"
        relationships_file = definition_path / "relationships.tmdl"
        
        logger.info(f"Expected tables directory: {tables_dir}")
        logger.info(f"Expected relationships file: {relationships_file}")
        
        # Verify folder structure exists
        if not semantic_model_path.exists():
            raise Exception(f"Semantic model folder not found: {semantic_model_path}")
        
        if not definition_path.exists():
            raise Exception(f"Definition folder not found: {definition_path}")
        
        if not tables_dir.exists():
            raise Exception(f"Tables folder not found: {tables_dir}")
        
        # Find PowerBI project files
        pbip_files = list(project_path.rglob("*.pbip"))
        tmdl_files = list(tables_dir.glob("*.tmdl"))  # Only look in tables folder
        json_files = list(project_path.rglob("*.json"))
        dax_files = list(project_path.rglob("*.dax"))
        pbism_files = list(project_path.rglob("*.pbism"))
        pbir_files = list(project_path.rglob("*.pbir"))
        
        total_files = len(pbip_files) + len(tmdl_files) + len(json_files) + len(dax_files) + len(pbism_files) + len(pbir_files)
        
        logger.info(f"Found {total_files} PowerBI project files:")
        if pbip_files:
            logger.info(f"  • {len(pbip_files)} .pbip files")
        if tmdl_files:
            logger.info(f"  • {len(tmdl_files)} .tmdl files in tables directory")
        if json_files:
            logger.info(f"  • {len(json_files)} .json files")
        
        if len(tmdl_files) == 0:
            logger.warning(f"No .tmdl files found in tables directory: {tables_dir}")
        
        # Get database connection for processing
        conn = get_catalog_connection()
        try:
            # 1. Insert the semantic model
            model_name = project_name
            model_id = insert_model(conn.cursor(), model_name, catalog_run_id)
            logger.info(f"Processed semantic model: {model_name} with ID: {model_id}")
            
            # 2. Parse and process tables from .tmdl files
            if tables_dir.exists():
                tables = parse_custom_tables_format(str(tables_dir))
                logger.info(f"Parsed {len(tables)} tables from .tmdl files")
                
                # Process tables with temporal versioning and count operations
                table_ids = {}
                for table in tables:
                    table_info = {
                        "name": table.get("name"),
                        "display_folder": table.get("display_folder"),
                        "is_hidden": table.get("is_hidden"),
                        "source_table": table.get("source_table")
                    }
                    table_id, operation = upsert_semantic_tables_temporal_with_summary(conn, model_id, table_info, catalog_run_id)
                    table_ids[table.get("name")] = table_id
                    processed_counts['tables_processed'] += 1 
                    if operation != 'unchanged':
                        summary[f'tables_{operation}'] += 1
                
                logger.info(f"Processed {len(table_ids)} tables with temporal versioning")
                
                # Process M-code partitions with summary
                m_code_summary = process_m_code_for_model_with_summary(conn, table_ids, str(tables_dir), catalog_run_id)
                summary['m_code_added'] += m_code_summary['added']
                summary['m_code_updated'] += m_code_summary['updated'] 
                summary['m_code_deleted'] += m_code_summary['deleted']
                processed_counts['m_code_processed'] += m_code_summary['total_processed']
                
                # Process columns with summary
                for table in tables:
                    table_name = table.get("name")
                    table_id = table_ids.get(table_name)
                    if table_id:
                        for column in table.get("columns", []):
                            column_info = {
                                "name": column.get("name"),
                                "dataType": column.get("dataType"),
                                "is_hidden": column.get("is_hidden"),
                                "format_string": column.get("format_string"),
                                "display_folder": column.get("display_folder")
                            }
                            _, operation = upsert_semantic_columns_temporal_with_summary(conn, table_id, column_info, catalog_run_id)
                            processed_counts['columns_processed'] += 1 
                            if operation != 'unchanged':
                                summary[f'columns_{operation}'] += 1
                
                # Process measures with summary
                for table in tables:
                    table_name = table.get("name")
                    table_id = table_ids.get(table_name)
                    if table_id:
                        for measure in table.get("measures", []):
                            measure_info = {
                                "name": measure.get("name"),
                                "expression": measure.get("expression"),
                                "formatString": measure.get("formatString"),
                                "displayFolder": measure.get("displayFolder"),
                                "lineageTag": measure.get("lineageTag"),
                                "isHidden": measure.get("isHidden", False),
                                "isPrivate": measure.get("isPrivate", False),
                                "isAvailableInMDX": measure.get("isAvailableInMDX", False)
                            }
                            _, operation = upsert_semantic_measures_temporal_with_summary(conn, table_id, measure_info, catalog_run_id)
                            processed_counts['measures_processed'] += 1 
                            if operation != 'unchanged':
                                summary[f'measures_{operation}'] += 1
                
            # Process relationships with summary
            if relationships_file.exists():
                relationships = parse_relationships(str(relationships_file))
                if relationships:
                    for relationship in relationships:
                        relationship_info = {
                            "fromTable": relationship.get("fromTable"),
                            "fromColumn": relationship.get("fromColumn"),
                            "toTable": relationship.get("toTable"),
                            "toColumn": relationship.get("toColumn"),
                            "isActive": relationship.get("isActive", True),
                            "relationship_type": relationship.get("relationship_type"),
                            "cross_filter": relationship.get("cross_filter")
                        }
                        _, operation = upsert_semantic_relationships_temporal_with_summary(conn, model_id, relationship_info, catalog_run_id)
                        processed_counts['relationships_processed'] += 1 
                        if operation != 'unchanged':
                            summary[f'relationships_{operation}'] += 1
            
            # Commit all changes
            conn.commit()
            
            # Log final summary
            logger.info("=" * 60)
            logger.info("POWERBI SEMANTIC MODEL CATALOGING SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Tables - Added: {summary['tables_added']}, Updated: {summary['tables_updated']}, Deleted: {summary['tables_deleted']}")
            logger.info(f"Columns - Added: {summary['columns_added']}, Updated: {summary['columns_updated']}, Deleted: {summary['columns_deleted']}")
            logger.info(f"Measures - Added: {summary['measures_added']}, Updated: {summary['measures_updated']}, Deleted: {summary['measures_deleted']}")
            logger.info(f"Relationships - Added: {summary['relationships_added']}, Updated: {summary['relationships_updated']}, Deleted: {summary['relationships_deleted']}")
            logger.info(f"M-Code - Added: {summary['m_code_added']}, Updated: {summary['m_code_updated']}, Deleted: {summary['m_code_deleted']}")
            logger.info("=" * 60)

            # Show what actually happened
            actual_changes = (summary['tables_added'] + summary['tables_updated'] + summary['tables_deleted'] +
                            summary['columns_added'] + summary['columns_updated'] + summary['columns_deleted'] +
                            summary['measures_added'] + summary['measures_updated'] + summary['measures_deleted'] +
                            summary['relationships_added'] + summary['relationships_updated'] + summary['relationships_deleted'] +
                            summary['m_code_added'] + summary['m_code_updated'] + summary['m_code_deleted'])

            total_processed = (processed_counts['tables_processed'] + processed_counts['columns_processed'] + 
                            processed_counts['measures_processed'] + processed_counts['relationships_processed'] + 
                            processed_counts['m_code_processed'])

            logger.info(f"Actual changes: {actual_changes}")
            logger.info(f"Total items processed: {total_processed}")
            
        finally:
            conn.close()
        
        logger.info("PowerBI project processing completed successfully")
        return summary, processed_counts
        
    except Exception as e:
        logger.error(f"Error processing PowerBI project: {e}")
        raise

def get_connection_info(connection_id):
    """Get connection info from database"""
    try:
        conn = get_catalog_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, connection_type, folder_path
                FROM config.connections 
                WHERE id = %s
            """, (connection_id,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'id': result[0],
                    'name': result[1], 
                    'connection_type': result[2],
                    'folder_path': result[3]
                }
        conn.close()
        return None
    except Exception as e:
        logger.error(f"Failed to get connection info: {e}")
        return None

def main():
    """Main PowerBI cataloging process"""
    parser = argparse.ArgumentParser(description='Catalog PowerBI semantic models')
    parser.add_argument('--connection-id', type=int, required=True, help='Connection ID for this cataloging run')
    parser.add_argument('--project-folder', type=str, help='Path to PowerBI project folder (optional - will use connection folder_path if not provided)')
    args = parser.parse_args()
    
    logger.info("Starting PowerBI semantic model cataloging")
    
    # Get connection info from database
    connection_info = get_connection_info(args.connection_id)
    if not connection_info:
        logger.error(f"Connection ID {args.connection_id} not found")
        sys.exit(1)
    
    # Determine project folder
    if args.project_folder:
        project_folder = args.project_folder
        logger.info(f"Using project folder from command line: {project_folder}")
    else:
        project_folder = connection_info.get('folder_path')
        logger.info(f"Using project folder from connection: {project_folder}")
    
    if not project_folder or not os.path.exists(project_folder):
        logger.error(f"❌ Project folder not found: {project_folder}")
        sys.exit(1)
    
    # Start catalog run
    catalog_conn = get_catalog_connection()
    try:
        catalog_run_id = start_powerbi_catalog_run(catalog_conn, connection_info, project_folder)
        catalog_conn.commit()
        logger.info(f"PowerBI catalog run {catalog_run_id} started")
    except Exception as e:
        catalog_conn.rollback()
        logger.error(f"Failed to create catalog run: {e}")
        sys.exit(1)
    finally:
        catalog_conn.close()
    
    # Setup logging with run ID
    setup_logging_with_run_id(catalog_run_id)
    
    # Process PowerBI project
    try:
        summary, processed_counts = process_powerbi_project(project_folder, catalog_run_id)
        
        # Complete the run
        complete_conn = get_catalog_connection()
        try:
            complete_powerbi_catalog_run(complete_conn, catalog_run_id, processed_counts)
            complete_conn.commit()
            logger.info(f"Successfully completed PowerBI catalog run {catalog_run_id}")
        finally:
            complete_conn.close()
            
    except Exception as e:
        # Mark as failed
        fail_conn = get_catalog_connection()
        try:
            fail_catalog_run(fail_conn, catalog_run_id, str(e))
            fail_conn.commit()
        finally:
            fail_conn.close()
        logger.error(f"PowerBI cataloging failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
