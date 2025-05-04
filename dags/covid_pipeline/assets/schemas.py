import os
import json

import duckdb
import jsondiff as jd
import pandas as pd
from dlt.common.schema import TColumnSchema
from dagster import TableSchema, TableColumn, TableColumnConstraints, TableConstraints, MetadataValue, AssetExecutionContext, AssetMaterialization


class Covid19CsseGithub:
    @staticmethod
    def dagster_table_schema() -> TableSchema:
        return TableSchema(
            columns=[
                TableColumn(name="row_num", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="row_key", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="fips", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="admin2", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="province_state", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="country_region", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="last_update", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="lat", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="longx", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="latitude", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="longitude", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="confirmed", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="deaths", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),  #
                TableColumn(name="recovered", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),  #
                TableColumn(name="active", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),  #
                TableColumn(name="combined_key", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}), #
                TableColumn(name="incident_rate", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),  #
                TableColumn(name="incidence_rate", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),  #
                TableColumn(name="case_fatality_ratio", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),  #
                TableColumn(name="file_md5", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="source_url", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="id", type="text", description=None, constraints=TableColumnConstraints(nullable=False, unique=False, other=["primary_key"]), tags={}),  #
                TableColumn(name="load_date", type="timestamp", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["precision", "timezone"]), tags={}),  # noqa  #
                TableColumn(name="year", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="month", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="day", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
            ],
            constraints=TableConstraints(other=[]),
        )

    @classmethod
    def pandas_schema(cls) -> dict[str, str]:
        return _convert_to_pandas_dtypes(table_schema=cls.dagster_table_schema())

    @classmethod
    def dlt_schema(cls) -> list[TColumnSchema]:
        return _convert_table_schema_to_tcolumn_schema(table_schema=cls.dagster_table_schema())

class Covid19DataHub:
    @classmethod
    def dagster_table_schema(cls) -> TableSchema:
        return TableSchema(
            columns=[
                TableColumn(name="row_num", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="row_key", type="text", description=None, constraints=TableColumnConstraints(nullable=False, unique=False, other=["primary_key"]), tags={}),
                TableColumn(name="id", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="date", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="administrative_area_level", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="country", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="state", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="city", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="population", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="school_closing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="workplace_closing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="cancel_events", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="gatherings_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="transport_closing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="stay_home_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="internal_movement_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="international_movement_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="information_campaigns", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="testing_policy", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="contact_tracing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="facial_coverings", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="vaccination_policy", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="elderly_people_protection", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="stringency_index", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="containment_health_index", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="economic_support_index", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="confirmed", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="deaths", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="recovered", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="tests", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="vaccines", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="people_vaccinated", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="people_fully_vaccinated", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="hosp", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="icu", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="vent", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="year", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="month", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="day", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
            ],
            constraints=TableConstraints(other=[]),
        )

    @classmethod
    def pandas_schema(cls) -> dict[str, str]:
        return _convert_to_pandas_dtypes(table_schema=cls.dagster_table_schema())

    @classmethod
    def dlt_schema(cls) -> list[TColumnSchema]:
        return _convert_table_schema_to_tcolumn_schema(table_schema=cls.dagster_table_schema())


def _convert_to_pandas_dtypes(table_schema: TableSchema):
    type_mapping = {
        "bigint": "int64",
        "double": "float64",
        "text": "string",
        "timestamp": "datetime64[ns]",
    }

    pandas_dtypes = {}
    for column in table_schema.columns:
        col_name = column.name
        col_type = column.type.lower()
        pandas_dtype = type_mapping.get(col_type, "object")  # Default to object if unknown
        pandas_dtypes[col_name] = pandas_dtype

    return pandas_dtypes


def _convert_table_schema_to_tcolumn_schema(table_schema: TableSchema) -> list[TColumnSchema]:
    supported_hints = {
        "partition", "cluster", "sort", "primary_key", "row_key",
        "parent_key", "root_key", "merge_key", "variant",
        "hard_delete", "incremental"
    }

    tcolumn_schemas = []

    for col in table_schema.columns:
        tcol: TColumnSchema = {
            "name": col.name,
            "data_type": col.type,
            "nullable": col.constraints.nullable,
            "unique": col.constraints.unique
        }

        for hint in col.constraints.other:
            if hint in supported_hints:
                tcol[hint] = True  # noqa

        if "dedup_sort" in col.tags:
            tcol["dedup_sort"] = col.tags["dedup_sort"]

        tcolumn_schemas.append(tcol)

    return tcolumn_schemas


def _create_duckdb_s3_json_secret(con) -> None:
    end_point = os.environ.get("MINIO_ENDPOINT_URL")
    key_id = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    query = f"""
        INSTALL json;

        CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{key_id}',
        SECRET '{secret_key}',
        URL_STYLE 'path',
        USE_SSL false,
        ENDPOINT '{end_point}'
        );
    """
    con.execute(query)


def _get_previous_schema_json(con, s3_uri: str, db_schema: str, table_name: str, dlt_schema: str) -> dict:
    sql = f"""
        SELECT json_extract(schema, '$.tables.{table_name}.columns') AS column_json FROM (
            SELECT *,
                   RANK() OVER(ORDER BY t1.inserted_at DESC) AS ranking
            FROM read_json("{s3_uri}/{db_schema}/_dlt_loads/*.jsonl") AS t1
            JOIN read_json("{s3_uri}/{db_schema}/_dlt_version/*.jsonl") AS t2 On t1.schema_version_hash = t2.version_hash
            WHERE t1.schema_name = '{dlt_schema}'
        ) AS tbl
        WHERE ranking = 2
    """
    prev_df = con.query(sql).df()

    if not prev_df.empty:
        column_json = prev_df.loc[0, "column_json"]
        if column_json:
            prev_schema = json.loads(column_json)
            return prev_schema

    return {}


def _get_schema_json_by_hash(con, s3_uri: str, version_hash: str, db_schema: str, table_name: str, dlt_schema: str) -> dict:
    sql = f"""
        SELECT json_extract(schema, '$.tables.{table_name}.columns') AS column_json
        FROM read_json("{s3_uri}/{db_schema}/_dlt_version/*.jsonl")
        WHERE version_hash = '{version_hash}'
        AND schema_name = '{dlt_schema}'
    """
    df = con.query(sql).df()

    if not df.empty:
        column_json = df.loc[0, "column_json"]
        if column_json:
            schema = json.loads(column_json)
            return schema

    return {}


def _get_schema_diff_markdown(table_name: str, prev_schema: dict, latest_schema: dict) -> str:
    if not prev_schema:
        return ""

    schema_diff = jd.diff(prev_schema, latest_schema, syntax="explicit")
    md = f"Table Updated: {table_name}\n\n"

    for operator, columns in schema_diff.items():
        for column_name, column_metadata in columns.items():
            metadata_md = f": {column_metadata}" if column_metadata else ""
            md += f"\t\tColumn {operator.label.title()} {column_name}{metadata_md}\n\n"

    return md if schema_diff else ""


def _get_schema_changes(latest_schema_hash: str, table_name: str, db_schema: str, dlt_schema: str):
    s3_uri = os.environ.get("DESTINATION__FILESYSTEM__BUCKET_URL")
    con = duckdb.connect()
    _create_duckdb_s3_json_secret(con=con)

    prev_schema = _get_previous_schema_json(con=con, s3_uri=s3_uri, db_schema=db_schema, table_name=table_name, dlt_schema=dlt_schema)
    latest_schema = _get_schema_json_by_hash(con=con, s3_uri=s3_uri, version_hash=latest_schema_hash, db_schema=db_schema, table_name=table_name, dlt_schema=dlt_schema)
    schema_diff_md = _get_schema_diff_markdown(table_name=table_name, prev_schema=prev_schema, latest_schema=latest_schema)

    return schema_diff_md


def add_schema_evolution_metadata(context: AssetExecutionContext , asset: AssetMaterialization, schema_hash: str, dlt_schema: str):
    md_content = ""

    if schema_hash:
        latest_hash_version = schema_hash[0]
        table_name, db_schema = asset.asset_key.to_python_identifier(), asset.metadata["dataset_name"]
        md_content += _get_schema_changes(
            latest_schema_hash=latest_hash_version, table_name=table_name, db_schema=db_schema, dlt_schema=dlt_schema
        )

    context.add_output_metadata(metadata={"schema_updates": MetadataValue.md(md_content)})

    return md_content


def init_delta_tables():
    from deltalake import write_deltalake
    covid19datahub_df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in Covid19DataHub.pandas_schema().items()})
    covid19_github_df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in Covid19CsseGithub.pandas_schema().items()})
    write_deltalake(table_or_uri="s3://testing/covid19/github_csse_daily", data=covid19_github_df, partition_by=["year", "month", "day"], mode="overwrite", storage_options={"allow_http": "true", "endpoint_url": "http://localhost:9050", "access_key_id": "datalake", "secret_access_key": "datalake"}, schema_mode="overwrite")
    write_deltalake(table_or_uri="s3://testing/covid19/covid19datahub", data=covid19datahub_df, partition_by=["year", "month", "day"], mode="overwrite", storage_options={"allow_http": "true", "endpoint_url": "http://localhost:9050", "access_key_id": "datalake", "secret_access_key": "datalake"}, schema_mode="overwrite")

if __name__ == '__main__':
    init_delta_tables()
