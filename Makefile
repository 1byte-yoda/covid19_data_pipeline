-include .env
export

.PHONY: dbt_lint dbt_fmt dbt_test yaml_fmt black flake8 typehint checklist

DUCKDB_FILE_PATH := ..$(DUCKDB_FILE_PATH)
DBT_FOLDER := transformer
FOLDERS_WITH_YML := covid_pipeline transformer dagster_home docker-compose.yaml
PY_FOLDERS := covid_pipeline

# Check for any SQL Lints in DBT
dbt_lint:
	sqlfluff lint $(DBT_FOLDER)

# Format SQL Code with DBT support
dbt_fmt:
	sqlfluff fix $(DBT_FOLDER)

# Run DBT Tests
dbt_test:
	docker compose run dbt dbt test

# Format .yaml/yml files
yaml_fmt:
	yamlfix $(FOLDERS_WITH_YML)

# Format Python Code using Black - PEP8 standard
black:
	black -l 170 $(PY_FOLDERS)

# Check for any Python Lints
flake8:
	flake8 $(PY_FOLDERS)

# Check for any missing typing
typehint:
	mypy --ignore-missing-imports $(PY_FOLDERS)

# Run Checklist for Continuous Integration and Deployment
checklist: black dbt_fmt dbt_lint flake8 dbt_test