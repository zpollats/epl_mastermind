# FPL Pipeline - Project Handoff Document

## 🎯 Current Status: Historical Data Complete, Ready for dbt Setup

**Last Updated:** August 10, 2025  
**Phase:** Historical data loading complete, dbt models designed  
**Next Chat Context:** Reference this document + start dbt setup and ML pipeline development

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
│   │   ├── data_exploration.py
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

### 1. Complete Historical Data Loader ✅ COMPLETED
**File:** `src/ingestion/complete_historical_loader.py`  
**Purpose:** Load all 5 seasons of FPL data with robust error handling  
**Status:** ✅ Successfully executed - created complete database  
**Achievement:** 133,273 rows across 5 complete seasons in DuckDB

### 2. Data Exploration Scripts ✅ COMPLETED
**Files:** `src/ingestion/data_exploration.py` + `robust_data_exploration.py`  
**Purpose:** Understand data structure, quality, and schema evolution  
**Status:** ✅ Completed - identified all data quality issues and solutions

### 3. dbt Project Structure 🔄 READY TO IMPLEMENT
**Files:** Complete dbt project with staging, intermediate, and marts models  
**Purpose:** Transform raw data into ML-ready features  
**Status:** 🔄 Designed and ready for implementation  
**Next Step:** Run setup commands and create model files

### 4. API Integration Design 📋 DESIGNED FOR LATER
**File:** `src/ingestion/api_loader_design.py`  
**Purpose:** Handle live FPL API data for new season  
**Status:** 📋 Designed but not implemented (lower priority)

---

## 🚀 Immediate Next Steps (Next Session)

1. **✅ Complete project setup** (DONE)
2. **✅ Run data exploration** (DONE) 
3. **✅ Load complete historical dataset** (DONE - 133k+ rows)
4. **🔄 Setup dbt and run transformations** (NEXT PRIORITY)
   - Install dbt-core and dbt-duckdb
   - Initialize dbt project in `dbt/` directory
   - Copy all model files from artifacts
   - Run staging → intermediate → marts pipeline
5. **📋 Build ML training pipeline** (AFTER dbt)
6. **📋 Create prediction workflow** (AFTER ML)

---

## 📅 Planned Development Phases

### Phase 1: Foundation (Weeks 1-4) - ✅ NEARLY COMPLETE
- [x] Project structure and environment setup
- [x] Data exploration and understanding  
- [x] Complete historical data loading pipeline (133k+ rows)
- [x] DuckDB database with indexes and validation
- [ ] dbt setup and staging models (NEXT SESSION)
- [ ] Feature engineering pipeline

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

## 🎉 Major Achievements This Session

### ✅ Complete Historical Dataset (133,273 rows)
- **All 5 seasons loaded:** 2020-21 through 2024-25 (complete)
- **Smart loading strategy:** Combined merged files + individual gameweek files
- **Data quality handled:** Parsing errors, schema evolution, negative points
- **Performance optimized:** DuckDB with proper indexes
- **1,864 unique players** across 189 gameweeks

### ✅ Production-Ready Data Pipeline
- **Robust error handling:** Multiple parsing strategies, graceful degradation
- **Comprehensive logging:** Full observability of data loading process
- **Schema evolution tracking:** Documented changes across seasons
- **Database design:** Proper indexing and validation queries

### ✅ Complete dbt Architecture Designed
- **Staging models:** Clean and standardize raw data
- **Intermediate models:** Feature engineering (rolling stats, team performance, position benchmarks)
- **Marts models:** ML-ready features with target variable (`next_gw_points`)
- **Data quality tests:** Comprehensive validation and monitoring

### ✅ Modern Development Setup
- **uv dependency management:** Fast, modern Python environment
- **Professional project structure:** Ready for portfolio showcase
- **Git initialization:** Clean commit history from start
- **Documentation:** Living context document for continuity

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

**Current Artifacts (Copy to Project):**
- **Project Context:** `project_handoff_doc` (this document - keep updated)
- **dbt Setup Commands:** `dbt_setup_commands` (installation and init steps)
- **dbt Project Config:** `dbt_project_setup` (dbt_project.yml content)
- **dbt Profiles:** `dbt_profiles` (profiles.yml for DuckDB connection)
- **dbt Sources:** `dbt_sources_config` (models/staging/_sources.yml)
- **dbt Staging Models:** `dbt_staging_models` (staging layer SQL)
- **dbt Intermediate Models:** `dbt_intermediate_models` (feature engineering SQL)
- **dbt Marts Models:** `dbt_marts_models` (ML-ready features SQL)

**Completed Scripts in Project:**
- `src/ingestion/complete_historical_loader.py` ✅ (successfully executed)
- `data/fpl_complete.db` ✅ (complete dataset ready)

**Copy these artifacts into your project and commit before next session!**