# Data Ingestion, Processing, and Analysis Pipeline

## 🚀 Objective

This project demonstrates the design and implementation of a data pipeline that ingests, processes, stores, and analyzes a dataset to extract meaningful insights. The pipeline is designed with robustness, scalability, and modularity in mind—showcasing practical data engineering techniques in a modern stack.

---

## 🧰 Tech Stack

| Component           | Technology Used         | Rationale                                                                 |
|--------------------|-------------------------|---------------------------------------------------------------------------|
| Orchestration      | Dagster                 | Modular, typed, and production-ready orchestration framework              |
| Ingestion & Scripts| Python                  | Flexibility and support for robust data handling                         |
| Transformation     | dbt                     | Industry-standard for SQL-based transformations and data modeling        |
| Storage            | PostgreSQL              | Relational DB with strong support for analytics and complex queries      |
| Containerization   | Docker                  | Ensures reproducibility and ease of deployment                           |

---

## 🔧 Setup Instructions

### 1. Clone the repository
```bash
git  https://github.com/1byte-yoda/covid19_data_pipeline.git
cd covid19_data_pipeline
```
### 2. Environment Setup
- Python 3.11+
- Docker & Docker Compose

### 3. Initialize the Docker Environment
```bash
docker compose up --build
```

### 4. Running DBT Models
```bash
make dbt_run
```

### 5. Running DBT Tests
```bash
make dbt_test
```

## 📦 Project Structure
```text
.
├── dags/                  # Dagster pipelines
├── dbt/                   # dbt models & transformations
├── docker-compose.yml     # Docker stack setup
├── ingestion/             # Python scripts for data ingestion
├── notebooks/             # Optional: EDA and correlation analysis
├── schema/                # SQL schema files (DDL)
└── README.md              # This file
```

## 📥 Data Ingestion
The ingestion/ingest_data.py script connects to the raw data source (CSV/JSON/API).

It handles:
- Missing values 
- Type mismatches 
- Schema inference & validation 
- Deduplication 
- The data is loaded into a raw schema in PostgreSQL.

## 🧹 Data Processing
dbt models transform raw data into a clean, query-ready format:
- Null handling and data type casting 
- ISO 8601 timestamp normalization 
- De-duplication logic 
- Derived columns for analysis 

The clean tables are stored in the analytics schema.


## 📌 Design Decisions & Architecture
**Dagster over Airflow/Mage:** Offers better local development UX and modular solids.

**dbt for transformation:** Enables version control, documentation, and model dependency graphs.

**PostgreSQL:** Chosen for its rich analytical functions and compatibility with dbt.

**Dockerized Workflow:** Promotes reproducibility across development and production environments.


## 🧪 Testing & Validation
Ingestion scripts include unit tests for edge cases (missing/invalid records).

dbt includes schema and data tests (e.g., not null, unique).

E2E validation via Dagster pipeline runs.