
import yaml
from tabulate import tabulate

def generate_markdown_from_yaml(yaml_path, output_md_path):
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)

    lines = ["# 📊 DataNavigator – Analyseconfiguratie", ""]

    # Table Analysis
    lines.append("## 📋 Table Analysis\n")
    headers = ["Naam", "Beschrijving", "Methode", "Prompt", "Output Table", "Status"]
    table = []
    for item in data.get("table_analysis", []):
        table.append([
            item.get("name", ""),
            item.get("description", ""),
            item.get("method", ""),
            item.get("prompt", ""),
            item.get("output_table", ""),
            item.get("status", "")
        ])
    lines.append(tabulate(table, headers=headers, tablefmt="github"))
    lines.append("")

    # Schema Analysis
    lines.append("## 🧠 Schema Analysis\n")
    headers = ["Naam", "Methode", "Input", "Output", "Notes"]
    table = []
    for item in data.get("schema_analysis", []):
        table.append([
            item.get("name", ""),
            item.get("method", ""),
            ", ".join(item.get("input", [])) if item.get("input") else "",
            ", ".join(item.get("output", [])) if item.get("output") else "",
            item.get("notes", "")
        ])
    lines.append(tabulate(table, headers=headers, tablefmt="github"))
    lines.append("")

    # Graph Building
    lines.append("## 🔄 Graph Building\n")
    headers = ["Naam", "Methode", "Input", "Output", "Gebruikt door"]
    table = []
    for item in data.get("graph_building", []):
        table.append([
            item.get("name", ""),
            item.get("method", ""),
            ", ".join(item.get("input", [])) if item.get("input") else "",
            ", ".join(item.get("output", [])) if item.get("output") else "",
            ", ".join(item.get("provides_for", [])) if item.get("provides_for") else ""
        ])
    lines.append(tabulate(table, headers=headers, tablefmt="github"))

    with open(output_md_path, "w") as out:
        out.write("\n".join(lines))
