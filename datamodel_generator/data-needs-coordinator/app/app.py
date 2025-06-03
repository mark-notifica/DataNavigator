import streamlit as st
from database import save_information_need, init_database
from mermaid import generate_mermaid_diagram, parse_relationship_notation, render_er_diagram

st.set_page_config(page_title="Inventarisatie Informatiebehoefte", page_icon="📋", layout="wide")
st.title("Uniform Datamodel Generator")
st.write("Gebruik het menu links om een pagina te kiezen.")

# def main():
#     if not init_database():
#         st.error("❌ Database initialisatie mislukt")
#         st.stop()
#     else:
#         st.success("✅ Database gereed")

#     st.title("📋 Inventarisatie van Informatiebehoefte")

#     # Question 1
#     st.header("1. Omschrijving van de Behoefte")
#     description = st.text_area(
#         "Wat is de informatiebehoefte?",
#         placeholder="Beschrijf de behoefte (bijv. inzicht in studievoortgang van studenten)."
#     )

#     # Help section
#     with st.expander("ℹ️ Hulp bij het invoeren van entiteiten"):
#         st.markdown("""
#         ### Invoerformaat en Voorbeelden
#         Voer entiteiten in met hun relaties, gescheiden door komma's.

#         ### Relatie Types:
#         - `1:1` : één-op-één (bijv. Persoon - Paspoort)
#         - `1:N` : één-op-veel (bijv. Team - Speler)
#         - `N:1` : veel-op-één (bijv. Order - Klant)
#         - `N:M` : veel-op-veel (bijv. Student - Vak)

#         ### Basis voorbeeld:
#         ```
#         Klant, Order
#         ("Klant", "Order", "1:N")
#         ```

#         ### Complex voorbeeld:
#         ```
#         Project, Taak, Medewerker
#         ("Project", "Taak", "1:N"), ("Taak", "Medewerker", "N:1")
#         ```
#         """)

#     # Basis voorbeeld
#     basis_diagram = """erDiagram
#         Klant {
#             int id
#             string naam
#         }
#         Order {
#             int id
#             date datum
#         }
#         Klant ||--o{ Order : plaatst
#     """

#     render_er_diagram(basis_diagram)

#     # Complex voorbeeld
#     complex_diagram = """erDiagram
#         Project {
#             int id
#             string naam
#             date start_datum
#         }
#         Taak {
#             int id
#             string naam
#             date deadline
#         }
#         Medewerker {
#             int id
#             string naam
#             string functie
#         }
#         Project ||--o{ Taak : heeft
#         Taak }o--|| Medewerker : uitgevoerd_door
#     """

#     render_er_diagram(complex_diagram)

#     # Input fields
#     entities = st.text_area(
#         "Stap 2a: Voer de entiteiten in",
#         placeholder="Voer entiteiten in, gescheiden door komma's (bijv: Klant, Order)"
#     )

#     relationships = st.text_area(
#         "Stap 2b: Voer de relaties tussen de entiteiten in",
#         placeholder="""Voer relaties in zoals dit voorbeeld:
# ("Klant", "Order", "1:N")"""
#     )

#     if st.button("Informatie Opslaan"):
#         if description and entities:
#             try:
#                 entities_data, relationships_list = parse_relationship_notation(entities, relationships)
#                 if save_information_need(description, list(entities_data.keys())):
#                     st.success("✅ Informatie opgeslagen")
#                     mermaid_diagram = generate_mermaid_diagram(entities_data, relationships_list)
#                     render_er_diagram(mermaid_diagram)
#                 else:
#                     st.error("❌ Opslaan mislukt")
#             except Exception as e:
#                 st.error(f"❌ Fout in invoer: {str(e)}")
#         else:
#             st.error("Vul alle velden in")

# if __name__ == "__main__":
#     main()