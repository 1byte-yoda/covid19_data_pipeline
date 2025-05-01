-include .env
export

DUCKDB_FILE_PATH := ..$(DUCKDB_FILE_PATH)
DBT_FOLDER := transformer
FOLDERS_WITH_YML := dagster transformer dagster_home docker-compose.yaml

.PHONY : dbt_lint

dbt_lint:
	sqlfluff lint $(DBT_FOLDER)

dbt_clean:
	sqlfluff fix $(DBT_FOLDER)

yaml_clean:
	yamlfix $(FOLDERS_WITH_YML)