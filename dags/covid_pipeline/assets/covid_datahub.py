import hashlib
import os
from datetime import date, timedelta, datetime
from typing import Optional, Iterable

import dlt
import pandas as pd
from deltalake import DeltaTable
from dlt.common.runtime.slack import send_slack_message
from loguru import logger
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from . import CustomDagsterDltTranslator, daily_partitions
from .schemas import Covid19DataHub, add_schema_evolution_metadata
from ..constants import SLACK_HOOK

_STORAGE_OPTIONS = {"AWS_REGION": "ap-southeast-1", "skip_signature": "true"}


@dlt.resource(
    name="covid19datahub",
    table_format="delta",
    table_name="covid19datahub",
    columns=Covid19DataHub.dlt_schema(),
    primary_key=["row_key"],
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def get_covid_datahub(start_date: date, end_date: date, s3_bucket_path: str) -> Iterable[pd.DataFrame]:
    while start_date <= end_date:
        logger.debug(f"Downloading data for partition {start_date}...")

        partitions = _get_hive_partition_filter(dt=start_date)
        df = _safe_download_from_s3(s3_bucket_path=s3_bucket_path, partitions=partitions)

        yield df

        start_date = start_date + timedelta(days=1)


@dlt.source
def covid_datahub_source(start_date: Optional[date] = None, end_date: Optional[date] = None, s3_bucket_path: Optional[str] = None):
    if not s3_bucket_path:
        s3_bucket_path = os.environ.get("COVID_DATAHUB_S3_BUCKET", "s3://covid19datahub-dataset/covid19/covid19datahub")

    df = get_covid_datahub(start_date=start_date, end_date=end_date, s3_bucket_path=s3_bucket_path)

    return df


@dlt_assets(
    dlt_source=covid_datahub_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="covid_datahub",
        destination=dlt.destinations.filesystem(),
        dataset_name="covid19",
    ),
    dagster_dlt_translator=CustomDagsterDltTranslator(),
    partitions_def=daily_partitions,
)
def covid_datahub_assets(context: AssetExecutionContext, dagster_dlt: DagsterDltResource):
    start, end = context.partition_key_range
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    materialized_assets = dagster_dlt.run(context=context, dlt_source=covid_datahub_source(start_date=start_date, end_date=end_date))

    schema_name = materialized_assets._dlt_pipeline.default_schema_name  # noqa

    for asset in materialized_assets:
        schema_hash = asset.metadata.get("schema_hash")
        md_content = add_schema_evolution_metadata(context=context, asset=asset, schema_hash=schema_hash, dlt_schema=schema_name)

        if SLACK_HOOK and md_content:
            send_slack_message(SLACK_HOOK, message=md_content)

        yield asset


def _safe_download_from_s3(s3_bucket_path: str, partitions: list[tuple[str, str, str]]) -> pd.DataFrame:
    columns_dtypes = Covid19DataHub.pandas_schema()

    df = DeltaTable(s3_bucket_path, storage_options=_STORAGE_OPTIONS).to_pandas(partitions=partitions)

    if not df.empty:
        # Define the row_key field as the primary key for the partition being processed
        df["row_num"] = df.groupby("date").cumcount() + 1
        df["row_key"] = df.apply(_generate_row_key, axis=1)
        df = df.astype({col: dtype for col, dtype in columns_dtypes.items() if col in df.columns})
        return df

    else:
        # Return an empty dataframe to avoid dlt ingest interruption
        df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in columns_dtypes.items()})
        return df.astype(dtype=columns_dtypes)


def _generate_row_key(row: pd.Series) -> str:
    return hashlib.md5(f"{row['id']}_{row['date']}_{row['row_num']}".encode()).hexdigest()


def _get_hive_partition_filter(dt: date) -> list[tuple[str, str, str]]:
    year, month, day = str(dt.year), str(dt.month), str(dt.day)
    partitions = [("year", "=", year), ("month", "=", month), ("day", "=", day)]
    return partitions
