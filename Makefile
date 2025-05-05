-include .env
export

DUCKDB_FILE_PATH := ..$(DUCKDB_FILE_PATH)
DBT_FOLDER := transformer
FOLDERS_WITH_YML := dags transformer dagster_home docker-compose.yaml
PY_FOLDERS := dags

.PHONY: dbt_lint dbt_fmt dbt_test yaml_fmt black flake8 typehint checklist pytest, dbt_run dbt_refresh initial_assets init_infra up down

# Check for any SQL Lints in DBT
.PHONY: dbt_lint
dbt_lint:
	sqlfluff lint $(DBT_FOLDER) -v

# Format SQL Code with DBT support
.PHONY: dbt_fmt
dbt_fmt:
	sqlfluff fix $(DBT_FOLDER) -v

# Run DBT Tests
.PHONY: dbt_test
dbt_test:
	docker compose run --rm dbt dbt deps
	docker compose run --rm dbt dbt test

# Format .yaml/yml files
.PHONY: yaml_fmt
yaml_fmt:
	yamlfix $(FOLDERS_WITH_YML)

# Format Python Code using Black - PEP8 standard
.PHONY: black
black:
	black -l 200 $(PY_FOLDERS)

# Check for any Python Lints
.PHONY: flake8
flake8:
	flake8 $(PY_FOLDERS)

# Check for any missing typing
.PHONY: typehint
typehint:
	mypy --ignore-missing-imports $(PY_FOLDERS)

# Run Dagster Asset Unit Tests
.PHONY: pytest
pytest:
	pytest $(PY_FOLDERS)/covid_pipeline_tests

# Run Checklist for Continuous Integration and Deployment
.PHONY: checklist
checklist: black dbt_fmt dbt_lint flake8 dbt_test

# Run DBT Models
.PHONY: dbt_run
dbt_run:
	docker compose run --rm dbt dbt deps
	docker compose run --rm dbt dbt run

# Run a Full Refresh on DBT Models
.PHONY: dbt_refresh
dbt_refresh:
	docker compose run --rm -it dbt dbt build --full-refresh

# Setup initial data in s3 bucket and initial models in DBT for convenient presentation
.PHONY: one_time_init
initial_assets:
	docker exec -it dagster bash /init_assets.sh
	make dbt_refresh
	@echo Dagster Assets from 2020-01-22 up to 2023-02-28 has been pre-downloaded for your convenience
	@echo Assets from 2023-03-01 to 2023-03-09 can be materialized manually.

# Initialize the infrastructure needed for the project
.PHONY: init_infra
init_infra:
	cat .env.example > .env
	docker compose up minio --build -d
	@sleep 20
	terraform -chdir=infra/dev init
	terraform -chdir=infra/dev apply -auto-approve
	@echo "\nDESTINATION__FILESYSTEM__BUCKET_URL=s3://$$(terraform -chdir=infra/dev output -raw bucket_name)" >> .env

tf_fmt:
	terraform -chdir=infra/dev fmt .

# Docker Compose Up
.PHONY: up
up:
	docker compose up --build --remove-orphans

# Docker Compose Down
.PHONY: down
down:
	docker compose down
