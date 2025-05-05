# COVID19 Global Pandemic Tracker Pipeline

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#overview">Overview</a>
    </li>
    <li>
      <a href="#the-data">The Data</a>
    </li>
    <li>
      <a href="#the-opportunity">The Opportunity</a>
    </li>
    <li>
      <a href="#technology-architecture">Technology Architecture</a>
    </li>
    <li>
      <a href="#design-decisions">Design Decisions</a>
    </li>
    <li>
      <a href="#covid19-global-pandemic-dashboard">COVID19 Global Pandemic Dashboard</a>
    </li>
    <li>
      <a href="#setup-instructions">Setup Instructions</a>
    </li>
    <li>
      <a href="#access-to-web-user-interface">Access to Web User Interface</a>
    </li>
    <li>
      <a href="#project-structure">Project Structure</a>
    </li>
    <li>
      <a href="#maintainability-commands">Maintainability Commands</a>
    </li>
  </ol>
</details>


## Overview

This project aims to consolidate COVID19 Data from heterogeneous data source through a data pipeline. This will enable the analysis of the famous COVID19 pandemic to extract meaningful insights.
The pipeline is designed with robustness, scalability, and modularity in mind—showcasing practical data engineering techniques in a modern data stack.

---

## The Data
The main data that we are going to use is the COVID19 Data from the [COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19/).
The dataset contains daily cumulative reports of confirmed, death, recovered, and active cases from different administrative area levels globally.

Additionally, we will also utilize the data from [covid19datahub.io](https://covid19datahub.io/) to enrich our analysis and be able to find interesting correlations and patterns from different variables
such as government policy measures, hospitalization, vaccination, and covid testing data.

---

## The Opportunity
Below, we will define the focus area of our analysis to centralize the scope our project.
1. What are the top 10 countries with most confirmed or death cases?
2. What are the top 10 countries with least confirmed or death cases?
3. How many daily deaths on average per country?
4. Which countries has the lowest death to confirmed cases ratio
5. How does the deaths, active, and confirmed cases change over time?
6. Top 10 Countries with most daily tests and hospitalizations?
7. What are the most fully vaccinated countries with respect to their population?
8. What are the effects of government policies to COVID19 deaths / recovered / confirmed cases?
9. Is there a positive effect on the likelihood of getting infected for those countries wearing a face mask?

---
## Technology Architecture
![mds](img/covid-pipeline.png)

---
## Design Decisions
**Dagster over Airflow/Mage:** Modular and typed Python Based orchestration framework which offers better local development UX. Offers open source support for Modern data stack
integration which makes it easy to maintain. Good metadata handling for data lineage and data observability. Offers Dagster Cloud for reliable and scalable Production workloads 

**DBT for transformation:** Wrapper for SQL based transformations. It enables version control, documentation, and model dependency graphs which also has a plugin for dagster. Handles the database object
creation for you, just write your SQL transformation. Also offers data testing which could be integrated with CICDs to ensure data correctness and reliability.

**DuckDB for Compute:** The T in ELT. Easy to set up/lightweight in-memory database - which makes it faster than the typical SQL databases. It supports reading/writing data from various systems
like DeltaLakes. Can be easily swap with MotherDuck for a more scalable production workload. 

**MinIO for Datalake:** The L storage in ELT. Open source datalake which supports AWS S3. Docker compatible which makes it Portable between local development and production.

**DLT:** The E in ELT. Python based data ingestion tool which has support for various sources / destinations. Offers good job metadata management to track ingestion loads.

**Delta File Format:** Used alongside DLT to store the ingested data into S3. It is using Parquet behind the curtain for columnar storage. 
It offers a good integration with DLT which makes it easy to operate. Schema Evolution ready that enables the system to be more robust to upstream schema changes 

**Dockerized Workflow:** Promotes reproducibility across development and production environments.

**Apache Superset:** Business Intelligence Tool that is part of The Modern Data Stack. It has compatibility with Dimensional Model slicing. Offers wide range of chart types. 
It has a built-in user access control feature which is ideal for data governance implementation.

**Star Schema:** A dimensional model which is composed of facts and dimension tables. This data model brings the balance between storage efficiency - just right amount of redundancy,
usability - lesser joins due to its denormalized trait, and performance - lesser joins == lesser data processing/shuffling.


---
## COVID19 Global Pandemic Dashboard
![mds](img/superset-dashboard.png)

---
## Setup Instructions

### 1. Clone The Repository
```bash
git  https://github.com/1byte-yoda/covid19_data_pipeline.git
cd covid19_data_pipeline
```
### 2. Download The Required Software
Visit the following link and follow the instructions for software installation
- [Python 3.11+](https://www.python.org/downloads/)
- [Docker & Docker Compose Desktop](https://docs.docker.com/compose/install/)

### 3. Initialize The Infrastructure Needed For The Project
The command below will create a `.env` file of off the `.env.example` file which contains the project config and credentials.
Then, a MinIO S3 Bucket and its name will be appended to .env file 
```bash
make init_infra
```

### 4. Start the Docker Containers
```bash
make up
```

### 5. Download Initial Dagster Assets
The dagster data pipeline was designed to run based on a date range partition. And back filling the data from 2020-01-22 up to 2023-03-09 is very time consuming
given the limited compute that we have. To make it easier, we will pre-download and pre-compute the data from 2020-01-22 to 2023-02-28, and we can still play around the remaining data.
The command below will download the COVID19 data from an S3 bucket and dump it into our MinIO bucket. And then it will also run a full refresh on our dbt models.
```bash
make initial_assets
```

## Access to Web User Interface
- **Dagster** http://localhost:3000
- **Minio** http://localhost:9051
  - ```
    username: admin
    password: minioadmin
    ```
- **Superset** http://localhost:8088
  - ```
    username: admin
    password: admin
    ```

---

## Project Structure
```text
.
├── dags/                  # Dagster pipelines
├── dagster_home/          # Volume mount for dagster pipeline's dagster.yml config
├── data/                  # Volume mount for S3 datalake and DuckDB storage
├── docker/                # Contains the Dockerfiles and init scripts for the docker containers used in the docker-compose.yaml
├── img/                   # The image files used for README
├── infra/                 # Contains Infrastructure As Code / Terraform Scripts to create S3 buckets
├── transformer/           # Has the DBT scripts for the data models in cleansed, curated, and data mart layers.
├── .env.example           # The example .env file template that can be used by the docker containers
├── .gitignore             # Git Ignored files / folder
├── .sqlfluff              # Configuration for SQL Fluff Formatter / Linter
├── .sqlfluffignore        # Contains the list of files / folders to be ignored during the SQL lint / formatting process 
├── docker-compose.yml     # Docker Modern Data Stack setup
├── Makefile               # Has helpful commands and shortcuts for development productivity
├── pytest.ini             # The configuration file for pytest execution preferences / settings
└── README.md              # This file
├── requirements.txt       # Python Packages used by this project
├── requirements-dev.txt   # Separate Python Packages used in addition to requirements.txt, solely for development purposes
├── tox.ini                # Flake8 configuration for linting preferences
```
  
---
## Maintainability Commands
### 1. Install Python Modules Locally for Development / IDE Lints
```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install uv
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt
```
### 2. Running DBT Tests
```bash
make dbt_test
```
### 3. Running Python Unit Tests
```bash
make pytest
```
### 4. Format DBT/SQL Scripts
```bash
make dbt_fmt
```
### 5. Format Python Scripts
```bash
make black
```
### 6. Lint Python Scripts For Errors
```bash
make flake8
```

[//]: # (## Data Ingestion)

[//]: # ()
[//]: # (## Data Processing)

[//]: # ()
[//]: # (## Testing & Validation)
