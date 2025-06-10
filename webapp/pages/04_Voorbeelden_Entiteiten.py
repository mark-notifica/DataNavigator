import runpy
from pathlib import Path

# Laad de oorspronkelijke pagina uit de datamodel generator
page_path = Path(__file__).resolve().parents[2] / 'datamodel_generator' / 'data-needs-coordinator' / 'app' / 'pages' / '02_Voorbeelden_Entiteiten.py'
runpy.run_path(str(page_path), run_name='__main__')
