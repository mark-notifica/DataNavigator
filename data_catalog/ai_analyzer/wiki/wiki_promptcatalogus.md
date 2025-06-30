# Promptcatalogus: Table & Schema Runners

Deze catalogus beschrijft de beschikbare analyseprofielen binnen de `table_runner` en `schema_runner`, met per profiel het doel, verwachte output en een voorbeeldprompt.

---

## 📘 `analysis_type`: `table_description` (table_runner)

**Doel**:
- Begrijpen wat de tabel representeert
- Herkennen of het een feitentabel of dimensietabel is
- Benoemen van belangrijke sleutels of relaties

**Output**:
```json
{
  "summary": "Beschrijving van de tabel",
  "type": "FACT" | "DIMENSION" | "OTHER",
  "suggested_keys": ["kolom1", "kolom2"]
}
```

**Voorbeeldprompt**:
```
🔍 Doel: Beschrijf de functie van de tabel `FactVerkoopregels`.
- Benoem wat de tabel representeert
- Geef aan of het een feitentabel of dimensietabel is (of anders)
- Beschrijf mogelijke sleutels of relaties met andere tabellen
Voorbeelddata:
{"artikelcode": "A123", "klant_id": 42, "datum": "2024-01-01", "aantal": 5, "bedrag": 250.00}
...
```

---

## 🧬 `analysis_type`: `column_classification` (table_runner)

**Doel**:
- Classificeer kolommen volgens hun rol in de data

**Classificaties**:
- `PRIMARY_KEY`, `FOREIGN_KEY`, `DATE`, `MEASURE`, `DIMENSION`, `CONTEXT`, `FLAG`, `IDENTIFIER`, `ENUM`, `ORDERING_FIELD`, `TIMESTAMP`, `DURATION`, `SENSITIVE`

**Prompt**:
🧬 Doel: Classificeer de kolommen van de tabel.
Gebruik één van de volgende labels per kolom.

Antwoord als JSON: `{ "kolomnaam": "LABEL" }`

**Output**:
```json
{
  "artikelcode": "DIMENSION",
  "klant_id": "FOREIGN_KEY",
  "aantal": "MEASURE",
  "datum": "DATE"
}
```

---

## ⚠️ `analysis_type`: `data_quality_check` (table_runner)

**Doel**:
- Signaleren van mogelijke datakwaliteitsproblemen

**Output**:
```json
{
  "null_warnings": ["datum heeft 80% nulls"],
  "duplicate_warnings": ["mogelijke dubbels op klant_id + datum"],
  "datatype_anomalies": ["bedrag lijkt tekst maar hoort getal"]
}
```

**Voorbeeldprompt**:
```
⚠️ Doel: Controleer de data op opvallende patronen of kwaliteitsproblemen.
- Zijn er kolommen met veel NULLs?
- Zijn er duplicaten?
- Zijn er rare datatypes?
Voorbeelddata:
{"artikelcode": "A123", "klant_id": null, "datum": null, "aantal": "5", "bedrag": "tweehonderd"}
...
```

---

## 🧠 `analysis_type`: `view_definition_analysis` (table_runner)

**Doel**:
- Begrijpen wat een view doet op basis van de SQL-definitie
- Herkennen van onderliggende tabellen en logica

**Output**:
```json
{
  "summary": "Beschrijving van de view",
  "underlying_tables": ["FactVerkoopregels", "DimKlant"],
  "suggestions": ["Herbenoem kolom X", "Voeg WHERE-clausule toe voor filtering"]
}
```

**Voorbeeldprompt**:
```
🧠 Doel: Analyseer de onderstaande SQL-viewdefinitie.
- Wat is het doel van deze view?
- Welke onderliggende tabellen en relaties worden gebruikt?
- Welke businesslogica valt op?
- Zijn er suggesties voor optimalisatie, hernoemen of documentatie?

Viewdefinitie van `vw_VerkopenActueel`:
--- SQL START ---
SELECT v.datum, k.klantnaam, v.bedrag
FROM verkoop v
JOIN klant k ON v.klant_id = k.id
WHERE v.jaar = 2024
--- SQL END ---
```

---

## 🧪 `analysis_type`: `all_in_one` (table_runner)

**Doel**:
- Combineer alle bovenstaande analyses in één prompt

**Output**:
- Bevat alle velden van de drie andere profielen

**Gebruik alleen voor volledige runs met ruime context.**

---

## 📘 `analysis_type`: `schema_context` (schema_runner)

**Doel**:
- Herkennen van het thema of de rol van het schema in het grotere datamodel

**Output**:
```json
{
  "schema_summary": "Schema bevat verkoopdata, gesplitst in orders, regels en producten."
}
```

---

## 📊 `analysis_type`: `table_overview` (schema_runner)

**Doel**:
- Samenvatten van de tabellen in een schema

**Output**:
```json
{
  "table_summaries": {
    "FactVerkoopregels": "Feitentabel met verkoopregels",
    "DimProduct": "Productdimensie met kenmerken"
  }
}
```

---

## 🧹 `analysis_type`: `naming_convention` (schema_runner)

**Doel**:
- Beoordelen van naamgevingsconventies binnen het schema

**Output**:
```json
{
  "issues": ["Tabel `tbl_klanten` wijkt af van PascalCase"],
  "suggestions": ["Gebruik DimKlant i.p.v. tbl_klanten"]
}
```

---

## 🧪 `analysis_type`: `all_in_one` (schema_runner)

**Doel**:
- Combineer schema-context, tabeloverzicht en naamgevingsanalyse

**Output**:
- Bevat alle velden van de drie andere profielen

**Gebruik voor validatie of eindanalyse van complete schema’s.**

---

## 🗂️ Richtlijnen bij uitbreiding
- Houd prompts taakgericht per `analysis_type`
- Voeg voorbeelddata toe bij table-analyses waar van toepassing
- Voor views: sla prompts op in `catalog_table_descriptions` net als gewone tabellen
- Beperk het aantal tabellen in 1 schema-prompt tot max. 10 voor contextbeheer

---

Laatste update: 2025-06-27