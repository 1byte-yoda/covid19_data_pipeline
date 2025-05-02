import hashlib
import os
from datetime import date, timedelta
from typing import Optional, Iterable

import dlt
import pandas as pd
from deltalake import DeltaTable
from loguru import logger
from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets

from . import CustomDagsterDltTranslator, daily_partitions

_STORAGE_OPTIONS = {"AWS_REGION": "ap-southeast-1", "skip_signature": "true"}


@dlt.resource(
    name="covid19datahub",
    table_format="delta",
    table_name="covid19datahub",
    columns={"year": {"partition": True}, "month": {"partition": True}, "day": {"partition": True}},  # explicitly defining column partitions
    primary_key=["row_key"],
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def get_covid_datahub(start_date: date, end_date: date, s3_bucket_path: str) -> Iterable[pd.DataFrame]:
    while start_date < end_date:
        logger.debug(f"Downloading data for partition {start_date}...")

        partitions = _get_hive_partition_filter(dt=start_date)
        df = DeltaTable(s3_bucket_path, storage_options=_STORAGE_OPTIONS).to_pandas(partitions=partitions)

        # Define the row_key field as the primary key for the partition being processed
        df["row_num"] = df.groupby("date").cumcount() + 1
        df["row_key"] = df.apply(_generate_row_key, axis=1)

        yield df

        start_date = start_date + timedelta(days=1)


@dlt.source
def covid_datahub_source(start_date: Optional[date] = None, end_date: Optional[date] = None, s3_bucket_path: Optional[str] = None):
    if not s3_bucket_path:
        s3_bucket_path = os.environ.get("COVID_DATAHUB_S3_BUCKET", "s3://covid19datahub-dataset/covid19/covid19datahub")
    df = get_covid_datahub(start_date=start_date, end_date=end_date, s3_bucket_path=s3_bucket_path)
    return df if df is not None else pd.DataFrame()


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
    yield from dagster_dlt.run(context=context, dlt_source=covid_datahub_source(start_date=start, end_date=end))


def _generate_row_key(row: pd.Series) -> str:
    return hashlib.md5(f"{row['id']}_{row['date']}_{row['row_num']}".encode()).hexdigest()


def _get_hive_partition_filter(dt: date) -> list[tuple[str, str, str]]:
    year, month, day = str(dt.year), str(dt.month), str(dt.day)
    partitions = [("year", "=", year), ("month", "=", month), ("day", "=", day)]
    return partitions
