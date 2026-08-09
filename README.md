# DataNavigator

> **Governance-ready metadata for business-critical intelligence**

An AI-powered data catalog that transforms "pretty solid" answers into audit-ready, validated knowledge — because in business intelligence, one missing transaction can cost millions.

[![Status](https://img.shields.io/badge/status-in%20development-yellow)](https://github.com/mark-notifica/DataNavigator)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 🔌 Connection Manager

Manage and test connections to PostgreSQL, Azure SQL Server or Power BI semantic models directly from the web interface. Connections can be added, edited and removed without manual configuration files.

## ⚙️ Catalog Execution

Execute cataloging jobs for configured connections. Each run stores a log file so historical executions can be viewed and analysed later from the interface.

## 🤖 AI Analyzer

Analyze catalogued tables using an AI service. The generated JSON results help to further enrich the catalog with summaries and suggestions.

---

## 🎯 The Problem

Your colleagues dump Power BI files and ETL scripts into ChatGPT or Claude. They get answers — but different answers every time. For exploration and learning, that's fine. **For business-critical intelligence, it's catastrophic.**

### What "Pretty Solid" Actually Costs:

| Risk Area | Impact | Cost |
|-----------|--------|------|
| **Inconsistent Metric Definitions** | Two analysts report different revenue numbers | $100K-1M+ in financial restatements |
| **Incomplete Data Inventory** | Compliance audit finds gaps | $50K-500K in audit findings |
| **Missed Dependencies** | Production deployment breaks 28 systems | $10K-100K per hour of downtime |
| **Wrong Business Decisions** | Executive strategy based on incorrect KPIs | Impossible to quantify |

**In business intelligence, we operate under the "One Missing Transaction" principle:**
- One missing customer = Understated revenue
- One wrong date = Quarter misallocated
- One missed field = Compliance violation
- One incorrect join = Cascade of bad decisions

---

## ✅ The Solution: DataNavigator

DataNavigator provides the **human-validated metadata foundation** that makes AI-powered data discovery reliable enough for business-critical work.

### The Reliability Framework

| Capability | ChatGPT/Claude | DataNavigator |
|------------|----------------|---------------|
| **Deterministic** | ❌ Different answer each time | ✅ Same approved answer always |
| **Complete** | ❌ "Here's what I found..." | ✅ "Here are ALL 47 columns" |
| **Validated** | ❌ AI's best guess | ✅ Human-approved & verified |
| **Traceable** | ❌ No audit trail | ✅ Who approved, when, why |
| **Versioned** | ❌ Can't track definition changes | ✅ Full change history |

---

## 🏗️ Architecture: Unified Node-Based Catalog

DataNavigator uses a **central node table** architecture optimized for RAG (Retrieval-Augmented Generation) and semantic search:

```
nodes (central table - easy to read and query):
┌─────────┬──────────────────────┬──────────────┬─────────────────────────┬──────────────┬──────────────────┐
│ node_id │ name                 │ node_type_id │ description             │ source       │ vector_embedding │
├─────────┼──────────────────────┼──────────────┼─────────────────────────┼──────────────┼──────────────────┤
│ 1       │ dim_customer         │ table        │ Customer dimension with │ DataWarehouse│ [0.23, -0.15...] │
│         │                      │              │ master data...          │              │                  │
│ 2       │ customer_id          │ column       │ Unique customer ID      │ DataWarehouse│ [0.11, 0.08...]  │
│ 3       │ Total Revenue        │ dax_measure  │ Sum of all closed-won...│ PowerBI_Sales│ [-0.05, 0.32...] │
│ 4       │ Get_Customer_Data    │ m_query      │ Fetches customer data...│ PowerBI_Sales│ [0.18, -0.22...] │
└─────────┴──────────────────────┴──────────────┴─────────────────────────┴──────────────┴──────────────────┘

node_types (defines all object types):
┌──────────────┬─────────────────────┐
│ type_id      │ type_name           │
├──────────────┼─────────────────────┤
│ table        │ Database Table      │
│ column       │ Table Column        │
│ view         │ Database View       │
│ dax_measure  │ Power BI Measure    │
│ m_query      │ Power BI M-Code     │
│ pipeline     │ ETL Pipeline        │
└──────────────┴─────────────────────┘

node_columns (object-specific attributes when needed):
┌─────────┬───────────────┬──────────┬─────────────┐
│ node_id │ data_type     │ nullable │ parent_node │
├─────────┼───────────────┼──────────┼─────────────┤
│ 2       │ INTEGER       │ NO       │ 1           │
└─────────┴───────────────┴──────────┴─────────────┘
```

### Why This Architecture?

✅ **Single table for RAG queries** - Simple LLM context  
✅ **Consistent vector embeddings** - Across all object types  
✅ **Fast semantic search** - One vector similarity query finds everything  
✅ **Flexible schema** - Add new object types without migration  
✅ **Easy to understand** - Human-readable central table  

---

## 🔄 The Validation Workflow

DataNavigator combines **AI speed** with **human accuracy**:

### 1️⃣ AI Analysis (Speed)
```
AI scans dim_customer table
Generates: "Appears to contain customer master data with 47 columns..."
Confidence: 75%
```

### 2️⃣ Human Review (Accuracy)
```
Data Steward Sarah Chen reviews and corrects:
"Also includes prospects (not just customers). Address fields are legacy - 
use address_id to join to dim_address instead. status_code values: 
A=Active, I=Inactive, P=Prospect"

✅ Approved: 2024-12-12 by Sarah Chen
```

### 3️⃣ Vector Embedding (Searchability)
```
Approved description → vector embedding
Now searchable by: "customer", "prospect", "address", "active"
```

### 4️⃣ Governance Metadata (Traceability)
```
Stored with complete audit trail:
- Approval date: 2024-12-12
- Approved by: Sarah Chen (Data Steward)
- Version: 2 (previous archived)
- Change reason: "Clarified prospect inclusion"
- Last validated: 2024-12-12
```

**Result:** Every query gets the SAME, APPROVED, TRACEABLE answer.

---

## 💡 Core Capabilities

### 1. Multi-Source Cataloging
- Automatically catalogs objects from data warehouses, databases, Power BI semantic models, ETL pipelines
- Handles diverse patterns, naming conventions, and documentation quality
- Central node architecture treats all objects consistently

### 2. AI-Powered Description Generation
- AI Analyzer generates initial descriptions for undocumented objects
- Provides starting point for cryptic technical objects (`FLD_01`, `Measure 147`)
- Dynamic LLM selection: cloud providers or local models (Mistral)

### 3. Human-in-the-Loop Validation
- Data stewards approve, reject, or improve AI-generated descriptions
- Captures organizational knowledge and business rules AI cannot infer
- Combines AI speed with human domain expertise

### 4. Semantic Search via Vector Embeddings
- Converts approved descriptions into vector embeddings
- Enables meaning-based search beyond keyword matching
- Stored directly in central node table for performance

### 5. AI Chat Interface (RAG System)
- Conversational interface for end users to explore catalog
- Vector search retrieves relevant nodes, enriches user prompts
- Answers questions like:
  - "Where is customer revenue stored?"
  - "How do I calculate churn rate?"
  - "What downstream reports use this table?"
- Explains technical objects in business terms

### 6. Data Lineage & Impact Analysis
- Pre-mapped relationships between objects
- Instant impact analysis: "What breaks if I change this?"
- Visualization of data flows and dependencies

---

## 👥 Who It's For

### Primary Users

**Data Analysts & Business Users**
- Discover data without SQL expertise
- Get reliable, consistent answers about metrics and definitions
- Understand where data comes from and how it's calculated

**Data Engineers**
- Document and maintain catalog metadata
- Review and approve AI-generated descriptions
- Perform impact analysis before deployments

**Data Stewards**
- Ensure data quality and governance
- Approve canonical definitions
- Maintain audit trails for compliance

**Analytics Teams**
- Navigate complex data landscapes
- Onboard new team members efficiently
- Reduce "where is this data?" questions by 80%+

### Organization Size

**Ideal for organizations with:**
- 50+ people working with data
- 10+ data sources
- Regulated industry requirements (finance, healthcare, etc.)
- Business-critical reporting where accuracy is non-negotiable

---

## 🎯 Key Differentiators

### vs. "Just Using ChatGPT/Claude"

| Dimension | ChatGPT/Claude | DataNavigator |
|-----------|----------------|---------------|
| **Consistency** | Different answers each time | Same approved answer always |
| **Completeness** | Might miss objects outside context | Guaranteed complete catalog |
| **Validation** | No verification process | Human-approved definitions |
| **Governance** | No audit trail | Full traceability & versioning |
| **Scale** | Manual effort per query | Automated indexing, instant results |
| **Cross-referencing** | Limited to files uploaded | Relationships across all sources |
| **Security** | Data sent to external APIs | On-prem or private cloud option |
| **Knowledge retention** | Lost after conversation | Permanent organizational knowledge |

### vs. Connected LLMs (Copilot, ChatGPT Enterprise)

**DataNavigator complements, doesn't compete:**
- Connected LLMs: Great for broad discovery and retrieval
- DataNavigator: Provides the structured, validated metadata layer that makes LLMs reliable

**Position as the "Source of Truth" layer** that AI assistants can query for business-critical answers.

---

## 🛠️ Technology Stack

**Backend:**
- Python 3.11+
- PostgreSQL (catalog database with vector extensions)
- SQLAlchemy (ORM)

**Frontend:**
- Streamlit (web interface)
- Modern, responsive UI for catalog management

**AI/ML:**
- Vector embeddings (pgvector or similar)
- RAG (Retrieval-Augmented Generation) architecture
- Dynamic LLM integration:
  - OpenAI (GPT-4, etc.)
  - Anthropic (Claude)
  - Local models (Mistral, Llama)

**Data Connectors:**
- PostgreSQL ✅
- Azure SQL ✅
- Power BI (PBIX/XMLA) ✅
- Planned: Azure Data Lake, Fabric Lakehouse

---

## 🚀 Getting Started

### Prerequisites

```bash
- Python 3.11+
- PostgreSQL 14+
- Git
```

### Installation

```bash
# Clone the repository
git clone https://github.com/mark-notifica/DataNavigator.git
cd DataNavigator

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your database credentials and API keys

# Run database migrations
python db/setup.py

# Start the application
streamlit run home.py
```

### Configuration

Create a `.env` file with:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/datanavigator

# LLM Configuration
OPENAI_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here
LOCAL_LLM_ENDPOINT=http://localhost:11434  # For Mistral/Ollama

# Data Source Connections
# (Managed via web interface)
```

---

## 📋 Roadmap

### Current (v0.1 - In Development)
- ✅ Unified node-based catalog architecture
- ✅ PostgreSQL data source connector
- 🔄 AI description generator with approval workflow
- 🔄 Vector embedding generation
- 🔄 Basic web interface for catalog management

### Near-term (v0.2)
- AI chat interface with RAG
- Power BI connector (PBIX/XMLA)
- Azure SQL connector
- Enhanced lineage visualization
- User authentication & role-based access

### Medium-term (v0.3)
- Advanced impact analysis
- Azure Data Lake connector
- Fabric Lakehouse connector
- API for external tool integration
- Bulk import/export capabilities

### Long-term (v1.0)
- Custom data source connector framework
- Advanced governance workflows
- Data quality monitoring
- Integration with popular BI tools
- Enterprise SSO support

---

## 🤝 Contributing

DataNavigator is in active development. Contributions welcome!

### Development Setup

```bash
# Install development dependencies
pip install -r dev-requirements.txt

# Run tests
pytest

# Code formatting
black .
flake8
```

### Workflow

See [GIT_WORKFLOW.md](GIT_WORKFLOW.md) for our branching strategy and contribution guidelines.

---

## 📄 License

[To be determined]

---

## 📞 Contact

**Project Maintainer:** Mark (mark-notifica)  
**Repository:** [https://github.com/mark-notifica/DataNavigator](https://github.com/mark-notifica/DataNavigator)

---

## 💬 Support

- **Issues:** [GitHub Issues](https://github.com/mark-notifica/DataNavigator/issues)
- **Discussions:** [GitHub Discussions](https://github.com/mark-notifica/DataNavigator/discussions)

---

## 🙏 Acknowledgments

Built with modern data catalog best practices and informed by real-world challenges in business intelligence governance.

---

**Remember:** In business intelligence, "pretty solid" isn't good enough. DataNavigator ensures your data intelligence is deterministic, complete, validated, and traceable — because your board meeting deserves better than ChatGPT's best guess.
