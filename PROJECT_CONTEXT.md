# FPL Pipeline - Project Handoff Document

## 🎯 Current Status: Project Setup & Data Exploration Phase

**Last Updated:** August 10, 2025  
**Phase:** Initial setup and historical data exploration  
**Next Chat Context:** Reference this document + current progress

---

## 📋 Project Overview

**Goal:** Build a production-ready Fantasy Premier League data pipeline for player performance prediction  
**Timeline:** 3-6 months with burst work patterns  
**Target:** First data engineering role (portfolio project)

### Tech Stack Decisions Made
- **Languages:** Python + SQL (focused approach)
- **Database:** DuckDB (local) → BigQuery (production later)
- **Orchestration:** Prefect (chosen over Dagster/Airflow)
- **Transformations:** dbt
- **Environment:** uv (modern Python dependency management)
- **Containerization:** Docker (later phase)
- **Cloud:** GCP (when ready for production)

---

## 📊 Data Source Details

**Primary Dataset:** [vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League)
- **Historical Data:** 5 seasons (2019-20 through 2023-24)
- **Key File:** `merged_gw.csv` per season (all players, all gameweeks)
- **Update Frequency:** Repo updates at season end, API for weekly data
- **Data Structure:** `season/gws/merged_gw.csv` format
- **Next Season Starts:** August 15, 2025 (5 days from project start)

### Data Schema (from exploration)
- Player performance by gameweek (GW 1-38 per season)
- Key fields: name, position, team, GW, total_points, minutes, goals_scored, assists
- ~600+ players per season, 38 gameweeks each

---

## 🏗️ Current Project Structure

```
fpl-pipeline/
├── pyproject.toml              # uv project configuration
├── uv.lock                     # Dependency lockfile
├── .env                        # Environment variables
├── .gitignore                  # Git ignore rules
├── README.md                   # Project documentation
├── PROJECT_CONTEXT.md          # This handoff document
├── data/                       # DuckDB files, cached data
├── src/
│   ├── __init__.py
│   ├── ingestion/              # Data loading scripts
│   │   ├── __init__.py
│   │   ├── historical_loader.py
│   │   └── api_loader.py       # (future - live API)
│   ├── ml/                     # Model training/prediction
│   │   ├── __init__.py
│   │   ├── features.py
│   │   ├── models.py
│   │   └── predictions.py
│   └── flows/                  # Prefect workflows
│       ├── __init__.py
│       └── main_pipeline.py
├── dbt/                        # dbt project (will initialize)
├── models/                     # Saved ML models
├── logs/                       # Pipeline logs
├── tests/                      # Unit tests
└── docs/                       # Documentation
```

---

## 🚀 Setup Commands (Completed/To Complete)

### Initial Setup
```bash
# Create project directory
mkdir fpl-pipeline && cd fpl-pipeline

# Initialize with uv
uv init
uv add duckdb pandas polars

# Create directory structure
mkdir -p {data,src/{ingestion,ml,flows},dbt,models,logs,tests,docs}

# Create initial files
touch README.md .env .gitignore PROJECT_CONTEXT.md
touch src/__init__.py
touch src/ingestion/{__init__.py,historical_loader.py,api_loader.py}
touch src/ml/{__init__.py,features.py,models.py,predictions.py}
touch src/flows/{__init__.py,main_pipeline.py}
```

### Dependencies Added
- `duckdb` - Local database
- `pandas` - Data manipulation
- `polars` - High-performance data processing

---

## 📝 Current Scripts Available

### 1. Data Exploration Script (`data_exploration.py`)
**Purpose:** Understand vaastav dataset structure and quality  
**Key Functions:**
- `explore_season_structure()` - Analyze individual season data
- `check_data_consistency()` - Verify column consistency across seasons
- `setup_duckdb_database()` - Initial database setup with sample data

**Status:** Ready to run
**Next Step:** Execute to understand data before designing dbt models

### 2. Project Setup Script (`project_setup.sh`)
**Purpose:** Complete project initialization  
**Status:** Commands ready to execute

---

## 🎯 Immediate Next Steps (This Session)

1. **✅ Complete project setup** (run setup commands)
2. **🔄 Run data exploration** (execute exploration script)
3. **📊 Analyze results** (understand data structure and quality)
4. **📋 Design dbt schema** (based on exploration findings)

---

## 📅 Planned Development Phases

### Phase 1: Foundation (Weeks 1-4) - CURRENT PHASE
- [x] Project structure and environment setup
- [ ] Data exploration and understanding
- [ ] Historical data loading pipeline
- [ ] Basic dbt staging models
- [ ] DuckDB schema design

### Phase 2: Data Pipeline (Weeks 5-8)
- [ ] Complete dbt transformations (staging → marts)
- [ ] Feature engineering for ML
- [ ] Data quality testing
- [ ] Prefect workflow integration

### Phase 3: ML Integration (Weeks 9-12)
- [ ] Model training pipeline
- [ ] Prediction workflows
- [ ] Model versioning and evaluation
- [ ] Performance tracking

### Phase 4: Production (Optional Extension)
- [ ] Docker containerization
- [ ] GCP deployment
- [ ] BigQuery migration
- [ ] Production monitoring

---

## 🔧 Technical Decisions Made

### Database Strategy
- **Local Development:** DuckDB (fast, embedded, perfect for development)
- **Production:** BigQuery (serverless, cost-effective, industry standard)
- **Migration Path:** Design dbt models to work with both

### Orchestration Choice: Prefect
**Why Prefect over alternatives:**
- Modern, excellent developer experience
- Great cloud offerings for later
- Better suited for ML workflows than Airflow
- Shows current tech knowledge

### Data Loading Strategy
- **Historical:** Load all 5 seasons from GitHub repo initially
- **Incremental:** Weekly API calls for new gameweek data
- **Storage:** Raw data in staging, transformed data in marts

---

## 🚨 Key Decisions to Revisit

1. **Individual Player Files:** Currently focusing on `merged_gw.csv`, may need individual player files if additional stats required
2. **ML Model Complexity:** Traditional ML focus (scikit-learn/XGBoost) vs more advanced techniques
3. **Feature Engineering:** Scope of features (basic stats vs advanced metrics)
4. **Production Timeline:** When to move from local to cloud

---

## 💡 Questions for Next Session

1. **Data Quality:** What issues did exploration reveal?
2. **Schema Design:** How should we structure dbt staging models?
3. **Feature Priorities:** Which player/team features are most important?
4. **API Integration:** Live FPL API structure and rate limits?

---

## 🔄 How to Use This Document in Future Chats

**Starting a new chat:**
1. Reference this document: "I'm working on the FPL pipeline project - here's my context document"
2. Update current status: "I've completed X, currently working on Y, stuck on Z"
3. Specify immediate need: "I need help with [specific task]"

**Updating this document:**
- Add new decisions and progress
- Update phase status and next steps
- Note any blockers or issues discovered
- Maintain as single source of truth

---

## 📁 Files to Reference

- **Project Spec:** `fpl_project_spec` artifact (comprehensive project overview)
- **Setup Commands:** `project_setup` artifact (bash commands)
- **Data Exploration:** `data_exploration` artifact (Python script)
- **This Document:** `project_handoff_doc` artifact (current context)

**Copy these artifacts into your project for offline reference and version control.**
