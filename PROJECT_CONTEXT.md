# FPL Pipeline - Project Handoff Document

## 🎯 Current Status: Baseline ML Model Complete, Ready for Optimization

**Last Updated:** August 17, 2025 (Session 3)  
**Phase:** Baseline ML model trained, optimization opportunities identified  
**Next Chat Context:** Reference this document + focus on model improvement and feature engineering

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
│   │   ├── explore_features.py         # review the features we have created in mart_ml_features
│   │   ├── train_baseline.py           # train a baseline model
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

1. **Add opponent strength to features (HIGHEST PRIORITY)**
   - Next opponent defensive strength
   - Next opponent attacking strength
   - Historical performance vs similar oppoents
   - Home/away context for next fixture
   - Position-specific oppoenent analysis

2. **🔧 Model Improvement (HIGH PRIORITY)**
   - Feature engineering: Add fixture difficulty, recent form trends, injury indicators
   - Advanced models: Try XGBoost, ensemble methods, position-specific models
   - Hyperparameter tuning: Current RandomForest is basic, optimize parameters
   - Target engineering: Consider different prediction targets (categorical, expected vs actual)

3. **📊 Model Analysis & Debugging**
   - Analyze prediction errors: Where is the model failing? Which players/situations?
   - Feature interaction analysis: Are we missing key feature combinations?
   - Temporal analysis: Does model perform worse in certain gameweeks/seasons?
   - Position-specific modeling: Train separate models for each position?

4. **🎯 FPL-Specific Optimization**
   - Focus on big haul prediction (currently only 14.3% recall)
   - Optimize for captain picks and differential players
   - Integrate price changes and ownership trends
   - Add fixture congestion and rotation risk features

---

## 📅 Planned Development Phases

### Phase 1: Foundation (Weeks 1-4) - ✅ COMPLETE
- [x] Project structure and environment setup
- [x] Data exploration and understanding  
- [x] Complete historical data loading pipeline (133k+ rows)
- [x] DuckDB database with indexes and validation
- [x] dbt setup and staging models
- [x] Complete feature engineering pipeline (staging → intermediate → marts)

### Phase 2: ML Development (Weeks 5-8) - ✅ BASELINE COMPLETE, 🔄 OPTIMIZATION NEEDED
- [x] ML feature exploration and analysis (excellent data quality confirmed)
- [x] Time-based train/validation/test splits (proper temporal validation)
- [x] Baseline model development (RandomForest trained and saved)
- [x] Model evaluation and feature importance analysis
- [ ] **Model improvement and optimization** (CRITICAL NEXT STEP)
- [ ] **Advanced feature engineering** (fixture difficulty, form trends)
- [ ] **FPL-specific model tuning** (big haul prediction optimization)

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

### ✅ Complete Baseline ML Pipeline 
- **Feature exploration:** 63k+ ML-ready records with excellent data quality
- **Time-based validation:** Proper temporal splits (2020-2023 train, 2023-24 val, 2024-25 test)
- **RandomForest baseline:** Trained and saved model with comprehensive evaluation
- **Feature importance:** Identified key predictors (season_avg_points, player_value, position_percentile)
- **FPL-specific metrics:** Directional accuracy, big haul prediction, position analysis

### 📊 Baseline Model Results (Room for Improvement)
- **Test Performance:** MAE 2.23 points, R² 0.048, RMSE 3.00
- **Directional Accuracy:** 57.5% (slightly better than random)
- **Big Haul Recall:** 14.3% (343 actual big hauls, captured 49)
- **Feature Rankings:** season_avg_points (0.217) > player_value (0.119) > position_percentile (0.104)

### ✅ Production Infrastructure
- **Model persistence:** Saved RandomForest model and feature metadata
- **Evaluation framework:** Comprehensive FPL-specific performance metrics
- **Time-series validation:** Proper temporal evaluation preventing data leakage

### 🔍 Key Insights for Model Improvement
- **Low explained variance (R² = 0.048):** FPL points are inherently noisy/unpredictable
- **Feature dominance:** Season averages and player value are most predictive
- **Big haul challenge:** Only capturing 14% of high-scoring gameweeks
- **Model conservatism:** Predictions have much lower variance than actual points

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