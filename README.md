# DataNavigator

The **DataNavigator** combines the following components:

## 📚 Data Catalog

Provides visibility into available data (e.g., tables, columns, data types) after the initial extraction from source systems.  
It starts from raw data in an analytics database or data lake.  
**Data stewards** and **functional data modelers** can enrich these objects with metadata, giving users more context and meaning.

## 🧠 Information Needs Module

Allows documentation of the organization's or business's information requirements.

## 🔄 Needs ↔ Source Mapping

Enables mapping and matching of information needs to technical data delivery.  
It includes information about data origin — whether directly from the source or derived/calculated.

## 🏗️ Uniform Data Model Generator

Automatically generates consistent SQL data models.  
The goal is to automate the generation of correct SQL, while still allowing for manual development first before integration into the generator.

## ✅ Feedback Loop & Validation

Facilitates the evaluation of the technical solution against the original information needs.

---

🎯 **Objective**:  
To provide insight into **what exists** and **what is needed**, from both the source data and business perspectives.  
From this insight, the actual data model can be created and managed — supporting ETL automation.


### 🛠 Requirements

```
To be filled later
```
---

### 🗂 File Structure

```
To be filled later
```
---

## ▶️ Usage

```
To be filled later
```
---

### 📝 Configuration (`servers_config.yaml`)

```
To be filled later
```

---




## 🔄 3. Roadmap

* Store data navigator database credentials in .env
* Webapp interface for setting up connections to datasources to be cataloged including credentials to datasources.
  - Postgress database server
  - Azure SQL server
  - Power BI semantic model (project)
* Webapp interface for actual cataloging (one-time and periodically) 
* Webapp interface (HTML/React UI) to browse the catalog and add metadata like descriptions.
* Create webinterface to set-up initial data navigator database and store credentials outside code base.
* Introduce users and userrights in webapp interface to create connections and access connections. 
* Add additionnal datasource connectors (like Azure Data Lake Storage V2 and Farbic Lakehouse).

