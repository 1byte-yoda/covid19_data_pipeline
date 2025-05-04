import json
from datetime import datetime

from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext
from ..project import covid19_dbt_project

from . import daily_partitions, CustomDagsterDbtTranslator


@dbt_assets(manifest=covid19_dbt_project.manifest_path, partitions_def=daily_partitions, dagster_dbt_translator=CustomDagsterDbtTranslator())
def covid19_dbt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_time_window

    dbt_vars = {"min_date": datetime.strftime(start, "%Y-%m-%d"), "max_date": datetime.strftime(end, "%Y-%m-%d")}
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]
    yield from (dbt.cli(dbt_build_args, context=context).stream().fetch_row_counts().fetch_column_metadata(with_column_lineage=False))
