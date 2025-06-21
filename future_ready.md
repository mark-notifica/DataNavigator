# DataNavigator: Architectuurvisie voor toekomstig multi-tenant gebruik

📅 Laatst bijgewerkt: 2025-06-21

## 🔍 Doel
Hoewel DataNavigator momenteel wordt ingezet voor intern gebruik, is het ontworpen met het oog op schaalbaarheid naar meerdere klanten zonder fundamentele herbouw.

## 🧱 Principes en afspraken

### 1. Eén codebase, meerdere klantdatabases
- Iedere klant draait op een eigen database (zelfde structuur, gescheiden data).
- De applicatie is onafhankelijk van specifieke klantdata geprogrammeerd.

### 2. Volledige context in analyse en opslag
- Alle referenties naar tabellen bevatten: `server`, `database`, `schema`, `tabel`
- AI-prompts krijgen altijd volledige context mee.
- Catalogus- en analysetabellen gebruiken samengestelde sleutels om klantonafhankelijk te blijven.

### 3. Dynamische configuratie
- Connectiebeheer gebeurt via tabellen (`config.connections`), niet via hardcoded variabelen.
- AI-analyse en metadata-operaties vragen expliciet om context, i.p.v. impliciete 'actieve database'.

### 4. AI-aansturing bewust gescheiden van uitvoering
- AI-output wordt opgeslagen in JSON-vorm per tabel of batch.
- Descripties kunnen AI- of handmatig gegenereerd zijn (kolom `source`).
- Versiebeheer is ingebouwd (`is_current`, timestamps).

### 5. Beveiliging en toekomst
- Het ontwerp maakt het mogelijk om klanten te isoleren op dataniveau.
- Het platform kan in de toekomst draaien binnen containers met per sessie een klantcontext.
- Webapp en CLI zijn uitbreidbaar met gebruikersrollen of klantfilters.

---

## 🗂️ Toekomstige uitbreidingen (optioneel)
- UI-component voor klantbeheer
- Geautomatiseerde provisioning van klantdatabases via templates
- Multi-user authenticatie en sessiebeheer
- Tenant ID als abstractielaag in metadata