-include .env
export

DUCKDB_FILE_PATH := ..$(DUCKDB_FILE_PATH)

.PHONY : dbt_lint

dbt_lint:
	sqlfluff lint covid19_dbt


dbt_clean:
	sqlfluff fix covid19_dbt