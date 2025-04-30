from datetime import datetime

import dlt
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import DailyPartitionsDefinition
from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets

from .covid19_github_csse import github_source
from .covid_datahub import covid_datahub_source
from .project import covid19_dbt_project

daily_partitions = DailyPartitionsDefinition(start_date="01-01-2020", fmt="%m-%d-%Y")
# end_date="01-10-2023",
# "03-09-2023"

@dlt_assets (
    dlt_source=github_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="github_csse_daily",
        destination=dlt.destinations.filesystem(),
        dataset_name="covid19"
    ),
    partitions_def=daily_partitions
)
def covid19_github_csse_assets(context: AssetExecutionContext, dagster_dlt: DagsterDltResource):
    partition_key_range = context.partition_key_range
    start_date = datetime.strptime(partition_key_range.start, "%m-%d-%Y").date()

    if partition_key_range.end:
        end_date = datetime.strptime(partition_key_range.end, "%m-%d-%Y").date()

    else:
        end_date = start_date

    yield from dagster_dlt.run(context=context, dlt_source=github_source(start_date=start_date, end_date=end_date))


@dlt_assets (
    dlt_source=covid_datahub_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="covid_datahub",
        destination=dlt.destinations.filesystem(),
        dataset_name="covid19"
    ),
    partitions_def=daily_partitions
)
def covid_datahub_assets(context: AssetExecutionContext, dagster_dlt: DagsterDltResource):
    partition_key_range = context.partition_key_range
    start_date = datetime.strptime(partition_key_range.start, "%m-%d-%Y").date()

    if partition_key_range.end:
        end_date = datetime.strptime(partition_key_range.end, "%m-%d-%Y").date()

    else:
        end_date = start_date

    yield from dagster_dlt.run(context=context, dlt_source=covid_datahub_source(start_date=start_date, end_date=end_date))


@dbt_assets(manifest=covid19_dbt_project.manifest_path)
def covid19_dbt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
