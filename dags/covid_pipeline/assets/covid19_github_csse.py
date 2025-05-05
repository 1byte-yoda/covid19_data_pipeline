import hashlib
from io import BytesIO
from typing import Optional
from datetime import date, timedelta, datetime

import dlt
import requests
import pandas as pd
from loguru import logger
from urllib.error import HTTPError
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster import AssetExecutionContext
from dlt.common.runtime.slack import send_slack_message

from . import CustomDagsterDltTranslator, daily_partitions
from .schemas import Covid19CsseGithub, add_schema_evolution_metadata
from ..constants import SLACK_HOOK, GITHUB_COVID_URL


@dlt.resource(
    name="github_csse_daily",
    table_format="delta",
    table_name="github_csse_daily",
    columns=Covid19CsseGithub.dlt_schema(),
    primary_key="id",
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def get_github_csse_daily(start_date: date, end_date: date, url: str = GITHUB_COVID_URL):
    while start_date <= end_date:
        query_date = start_date.strftime("%m-%d-%Y")

        download_url = url.format(query_date=query_date)
        logger.debug(f"Downloading csv file from: {download_url}")

        df = _safe_download_with_md5_and_url(url=download_url)

        # Converting Integer Fields to Float which will allow pyarrow to concatenate dataframes.
        # Ints can be Floats but not the other way around
        df = df.astype({col: "float" for col in df.select_dtypes(include=["int"]).columns})
        df = _create_id(df=df)
        df = _create_hive_partition_fields(df=df, ingest_date=start_date)

        yield df
        start_date = start_date + timedelta(days=1)


@dlt.source
def covid_github_source(start_date: Optional[date] = None, end_date: Optional[date] = None):
    df = get_github_csse_daily(start_date=start_date, end_date=end_date)
    return df


@dlt_assets(
    dlt_source=covid_github_source(),
    dlt_pipeline=dlt.pipeline(pipeline_name="github_csse_daily", destination=dlt.destinations.filesystem(), dataset_name="covid19"),
    partitions_def=daily_partitions,
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def covid19_github_csse_assets(context: AssetExecutionContext, dagster_dlt: DagsterDltResource):
    start, end = context.partition_key_range

    materialized_assets = dagster_dlt.run(context=context, dlt_source=covid_github_source(start_date=datetime.strptime(start, "%Y-%m-%d"), end_date=datetime.strptime(end, "%Y-%m-%d")))

    schema_name = materialized_assets._dlt_pipeline.default_schema_name  # noqa

    for asset in materialized_assets:
        schema_hash = asset.metadata.get("schema_hash")
        md_content = add_schema_evolution_metadata(context=context, asset=asset, schema_hash=schema_hash, dlt_schema=schema_name)

        if SLACK_HOOK and md_content:
            send_slack_message(SLACK_HOOK, message=md_content)

        yield asset


def _safe_download_with_md5_and_url(url: str) -> pd.DataFrame:
    columns_dtypes = Covid19CsseGithub.pandas_schema()

    try:
        response = requests.get(url)

        df = pd.read_csv(BytesIO(response.content))
        df["file_md5"] = hashlib.md5(response.content).hexdigest()
        df["source_url"] = url
        df = df.astype({col: dtype for col, dtype in columns_dtypes.items() if col in df.columns})
        return df

    except HTTPError as e:
        logger.exception(e)

        # Create empty DataFrame with explicit dtypes
        df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in columns_dtypes.items()})
        return df


def _create_hive_partition_fields(df: pd.DataFrame, ingest_date: date) -> pd.DataFrame:
    logger.debug(f"Hive partition date to create: {ingest_date}")

    df["load_date"] = ingest_date
    df["year"] = ingest_date.year
    df["month"] = ingest_date.month
    df["day"] = ingest_date.day

    return df


def _create_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(names="row_num")
    df["id"] = df.apply(_generate_md5, axis=1)
    return df


def _generate_md5(row: pd.Series):
    concat_str = f"{row['file_md5']}_{row['row_num']}"
    return hashlib.md5(concat_str.encode("utf-8")).hexdigest()
