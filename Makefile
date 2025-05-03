-include .env
export

DUCKDB_FILE_PATH := ..$(DUCKDB_FILE_PATH)
DBT_FOLDER := transformer
FOLDERS_WITH_YML := dags transformer dagster_home docker-compose.yaml
PY_FOLDERS := dags

.PHONY: dbt_lint dbt_fmt dbt_test yaml_fmt black flake8 typehint checklist pytest

# Check for any SQL Lints in DBT
.PHONY: dbt_lint
dbt_lint:
	sqlfluff lint $(DBT_FOLDER)

# Format SQL Code with DBT support
.PHONY: dbt_fmt
dbt_fmt:
	sqlfluff fix $(DBT_FOLDER)

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
	pytest $(PY_FOLDERS)/covid_pipeline_tests -v -s

# Run Checklist for Continuous Integration and Deployment
.PHONY: checklist
checklist: black dbt_fmt dbt_lint flake8 dbt_test

.PHONY: dbt_run
dbt_run:
	docker compose run --rm dbt dbt deps
	docker compose run --rm dbt dbt run