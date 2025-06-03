import os

# Mappenstructuur
folders = [
    "manifests/base",
    "sql_templates",
    "udg"
]

# Bestanden met hun paden
files = [
    "manifests/base/bron_syntess.yaml",
    "manifests/base/uniform_core.yaml",
    "sql_templates/bron_view.sql.j2",
    "sql_templates/uniform_view.sql.j2",
    "udg/__init__.py",
    "udg/generator.py",
    "udg/config.py",
    "requirements.txt",
    "README.md"
]

# Maak mappen aan
for folder in folders:
    os.makedirs(folder, exist_ok=True)

# Maak bestanden aan
for file in files:
    with open(file, 'w') as f:
        pass  # Lege bestanden aanmaken

print("Mappen en bestanden zijn aangemaakt.")