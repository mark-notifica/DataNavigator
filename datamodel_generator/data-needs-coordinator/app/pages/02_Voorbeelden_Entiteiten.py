import streamlit as st
from mermaid import render_er_diagram

st.header("ERD Voorbeelden")

st.markdown("""
### Invoerformaat en Voorbeelden
- Voer bij de entiteiten de tabelnamen en hun kolommen in, gescheiden door komma's. Bij de relaties geef je de entiteit namen en hun relatie aan, ook gescheiden door komma's.
- Geef per kolom het datatype op na een dubbele punt, bijvoorbeeld `id:int`.
- Dit helpt om het ER-diagram correct te genereren en geeft duidelijkheid over de structuur van de data.

### Relatie Types:
- `1:1` : één-op-één (bijv. Persoon - Paspoort)
- `1:N` : één-op-veel (bijv. Team - Speler)
- `N:1` : veel-op-één (bijv. Order - Klant)
- `N:M` : veel-op-veel (bijv. Student - Vak)

### Basis voorbeeld:
#### Entiteiten (tabellen en kolommen)
```
Klant(id:int, naam:string, email:string) Order(id:int, datum:date, totaal:float, klant_id:int)
```
            
#### Relaties
```            
("Klant", "Order", "1:N", "id", "klant_id", "plaatst")
```
""")
basis_diagram = """erDiagram
    Klant {
        int id
        string naam
        string email
    }
    Order {
        int id
        date datum
        float totaal
        int klant_id
    }
    Klant ||--o{ Order : plaatst
"""
render_er_diagram(basis_diagram)

st.markdown("""
                        
### Complex voorbeeld:
#### Entiteiten (tabellen en kolommen)
```
Project(id:int, naam:string, start_datum:date),Taak(id:int, naam:string, deadline:date, project_id:int, medewerker_id:int),Medewerker(id:int, naam:string, functie:string)

```
            
#### Relaties            
```
("Project", "Taak", "1:N", "id", "project_id", "heeft"),("Taak", "Medewerker", "N:1", "medewerker_id", "id", "uitgevoerd_door")
```

""")

st.markdown("""
#### Diagram
""")
complex_diagram = """erDiagram
    Project {
        int id
        string naam
        date start_datum
    }
    Taak {
        int id
        string naam
        date deadline
        int project_id
        int medewerker_id
    }
    Medewerker {
        int id
        string naam
        string functie
    }
    Project ||--o{ Taak : heeft
    Taak }o--|| Medewerker : uitgevoerd_door
"""
render_er_diagram(complex_diagram)