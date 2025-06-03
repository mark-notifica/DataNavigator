import streamlit as st
from database import save_information_need, init_database
from mermaid import generate_mermaid_diagram, parse_relationship_notation, render_er_diagram

if not init_database():
        st.error("❌ Database initialisatie mislukt")
        st.stop()
else:
    st.success("✅ Database gereed")

st.title("📋 Inventarisatie Informatiebehoefte")

# Question 1
st.header("1. Omschrijving van de Behoefte")
description = st.text_area(
    "Wat is de informatiebehoefte?",
    placeholder="Beschrijf de behoefte (bijv. inzicht in studievoortgang van studenten)."
)

st.header("2. Invoer van de entiteiten")
# Input fields
entities = st.text_area(
    "Stap 2a: Voer de tabellen,kolommen en datatypes in.",
    placeholder="Voer entiteiten in, gescheiden door komma's. Voorbeeld; Project(id:int, naam:string, start_datum:date),Taak(id:int, naam:string, deadline:date, project_id:int, medewerker_id:int)"
    ""
)

relationships = st.text_area(
    "Stap 2b: Voer de relaties tussen de entiteiten in",
    placeholder="""Voer relaties in, gescheiden door komma's. Voorbeeld;
("Project", "Taak", "1:N", "id", "project_id", "heeft")"""
)

if st.button("Informatie Opslaan"):
    if description and entities:
        try:
            entities_data, relationships_list = parse_relationship_notation(entities, relationships)
            if save_information_need(description, list(entities_data.keys())):
                st.success("✅ Informatie opgeslagen")
                mermaid_diagram = generate_mermaid_diagram(entities_data, relationships_list)
                render_er_diagram(mermaid_diagram)
            else:
                st.error("❌ Opslaan mislukt")
        except Exception as e:
            st.error(f"❌ Fout in invoer: {str(e)}")
    else:
        st.error("Vul alle velden in")