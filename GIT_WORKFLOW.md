# 🚀 Git Workflow voor DataNavigator

Dit document beschrijft de manier van werken met Git binnen het DataNavigator-project, inclusief branch-strategie, naamconventies, inzet van AI-assistenten en praktische commando’s.

---

## 🔧 Werkwijze

We werken **niet rechtstreeks op `main`**, maar via branches en pull requests. Dit zorgt voor meer structuur, duidelijkheid en betere versiecontrole.

---

## 📂 Branch-strategie

| Branch type   | Prefix         | Gebruik voor                             |
|---------------|----------------|------------------------------------------|
| Feature       | `feature/`     | Nieuwe functionaliteiten                 |
| Bugfix        | `fix/`         | Kleine fouten of bugs                    |
| Refactor      | `refactor/`    | Technische herstructureringen            |
| Documentatie  | `docs/`        | Aanpassingen aan documentatie            |
| Overig        | `chore/`       | Kleine taken (cleanup, config, etc.)     |

Voorbeelden:
- `feature/catalog-powerbi`
- `fix/typo-readme`
- `chore/update-requirements`

---

## ✅ Stappenplan

### 🔀 1. Nieuwe branch maken

```bash
git checkout -b feature/mijn-feature
```

### 🛠 2. Code schrijven + committen

```bash
git add .
git commit -m "🧩 Voeg Power BI scanning toe aan catalogus"
```

### ⬆️ 3. Push naar GitHub

```bash
git push -u origin feature/mijn-feature
```

### 🧪 4. Maak een Pull Request via GitHub

- Base: `main`
- Compare: `feature/mijn-feature`
- Voeg titel en beschrijving toe
- (optioneel) Voeg reviewer toe

### ✅ 5. Merge pull request (via GitHub)

### 🔄 6. Werk `main` lokaal bij

```bash
git checkout main
git pull origin main
```

---

## 🧠 Richtlijnen voor AI-assistenten (zoals ChatGPT)

AI-tools mogen ingezet worden ter ondersteuning, **niet ter vervanging van denken of testen**.

### ✅ Wel gebruiken voor:
- Voorstellen van code-structuur of SQL-queries
- Genereren van documentatie (README, templates, env-sjablonen)
- Debuggen van foutmeldingen (stap-voor-stap analyse)
- Uitleggen van complexe concepten (zoals Git, SQL-optimalisatie, etc.)
- Herschrijven of formatteren van tekst

### 🚫 Niet blind gebruiken voor:
- Productiecode zonder eigen controle
- Aanpassingen aan security, authenticatie of configuratiebestanden zonder begrip
- Refactors zonder testdekking of domeinkennis
- Auto-generated commit messages of PR-teksten zonder aanpassing

> ⚠️ AI is een copiloot, geen autopiloot.

---

## 🗂 Tips

- Houd branches klein en gericht
- Begin commit messages met emoji voor leesbaarheid (optioneel):
  - ✨ Nieuwe feature
  - 🐛 Bugfix
  - 🧹 Cleanup
  - 📝 Documentatie
- Gebruik `.gitignore` om gevoelige of tijdelijke bestanden uit te sluiten

---

## 🧰 Nuttige Git-commando’s

| Doel                         | Command                                       |
|------------------------------|-----------------------------------------------|
| Huidige branch tonen         | `git branch`                                  |
| Nieuwe branch maken          | `git checkout -b feature/xyz`                 |
| Lokale wijzigingen tonen     | `git status`                                  |
| Bestanden toevoegen          | `git add .`                                   |
| Commit maken                 | `git commit -m "✨ Voeg nieuwe feature toe"`   |
| Branch pushen                | `git push -u origin feature/xyz`              |
| Wisselen naar `main`         | `git checkout main`                           |
| Laatste versie van main      | `git pull origin main`                        |
| Branch verwijderen lokaal    | `git branch -d feature/xyz`                   |
| Branch verwijderen remote    | `git push origin --delete feature/xyz`        |

---

## 📦 Toekomstige uitbreiding

Overweeg later gebruik van:
- Automatische testen (CI)
- Code-review regels
- Release-tags
- Linting en formattering
