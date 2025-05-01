import json
from datetime import datetime

import dlt
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import DailyPartitionsDefinition, AssetKey, AssetSpec, AssetDep
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets

from .covid19_github_csse import covid_github_source
from .covid_datahub import covid_datahub_source
from ..project import covid19_dbt_project

daily_partitions = DailyPartitionsDefinition(start_date="2020-01-22", end_date="2023-03-09", fmt="%Y-%m-%d")
# end_date="01-10-2023",
# "03-09-2023"

class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Overrides asset spec to override asset key to be the dlt resource name."""
        default_spec = super().get_asset_spec(data)
        deps = [AssetDep(asset=AssetKey(f"dlt_{data.resource.source_name}"), partition_mapping=d.partition_mapping) for d in default_spec.deps]
        return default_spec.replace_attributes(
            key=AssetKey(f"{data.resource.name}"), deps=deps
        )


@dlt_assets (
    dlt_source=covid_github_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="github_csse_daily",
        destination=dlt.destinations.filesystem(),
        dataset_name="covid19"
    ),
    partitions_def=daily_partitions,
    dagster_dlt_translator=CustomDagsterDltTranslator()
)
def covid19_github_csse_assets(context: AssetExecutionContext, dagster_dlt: DagsterDltResource):
    start, end = context.partition_time_window
    yield from dagster_dlt.run(context=context, dlt_source=covid_github_source(start_date=start, end_date=end))


@dlt_assets (
    dlt_source=covid_datahub_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="covid_datahub",
        destination=dlt.destinations.filesystem(),
        dataset_name="covid19",
    ),
    dagster_dlt_translator=CustomDagsterDltTranslator(),
    partitions_def=daily_partitions
)
def covid_datahub_assets(context: AssetExecutionContext, dagster_dlt: DagsterDltResource):
    start, end = context.partition_time_window

    yield from dagster_dlt.run(context=context, dlt_source=covid_datahub_source(start_date=start, end_date=end))


@dbt_assets(manifest=covid19_dbt_project.manifest_path, partitions_def=daily_partitions)
def covid19_dbt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_time_window

    dbt_vars = {
        "min_date": datetime.strftime(start, "%Y-%m-%d"),
        "max_date": datetime.strftime(end, "%Y-%m-%d")
    }
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]
    yield from (
        dbt.cli(dbt_build_args, context=context)
        .stream()
        .fetch_row_counts()
        .fetch_column_metadata(with_column_lineage=False)
    )
