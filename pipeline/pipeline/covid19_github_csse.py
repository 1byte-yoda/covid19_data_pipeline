import hashlib
from datetime import date, timedelta
from io import BytesIO
from typing import Optional

import requests
from urllib.error import HTTPError
from loguru import logger
import pandas as pd
import dlt


def _safe_download_covid_df_with_md5(url: str) -> pd.DataFrame:
    try:
        print("Downloading CSV from:", url)
        response = requests.get(url)
        md5_hash = hashlib.md5(response.content).hexdigest()
        df = pd.read_csv(BytesIO(response.content))
        df["file_md5"] = md5_hash
        return df
    except HTTPError as e:
        logger.exception(e)
        return pd.DataFrame()


def _create_hive_partition_fields(df: pd.DataFrame, ingest_date: date) -> pd.DataFrame:
    logger.debug(f"Hive partition date to create: {ingest_date}")
    df["load_date"] = ingest_date
    df["year"] = ingest_date.year
    df["month"] = ingest_date.month
    df["day"] = ingest_date.day
    return df


def generate_md5(row: pd.Series):
    concat_str = f"{row['file_md5']}_{row['row_num']}"
    return hashlib.md5(concat_str.encode('utf-8')).hexdigest()


@dlt.resource(
    name="covid_cases_github",
    table_format="delta",
    table_name="github_csse_daily",
    columns={"year": {"partition": True}, "month": {"partition": True}, "day": {"partition": True}},
    primary_key="id",
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def get_github_csse_daily(start_date: date, end_date: date):
    partition_date = start_date
    while partition_date <= end_date:
        query_date = partition_date.strftime("%m-%d-%Y")
        url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/{query_date}.csv"
        df = _safe_download_covid_df_with_md5(url=url)
        df = df.reset_index(names="row_num")
        df["id"] = df.apply(generate_md5, axis=1)
        df["source_url"] = url
        df["before_load_rows"] = df.shape[0]
        df["before_load_cols"] = str(list(df.columns))
        df = _create_hive_partition_fields(df=df, ingest_date=partition_date)
        df["after_load_rows"] = df.shape[0]
        df["after_load_cols"] = str(list(df.columns))
        yield df
        partition_date = partition_date + timedelta(days=1)


@dlt.source
def github_source(start_date: Optional[date] = None, end_date: Optional[date] = None):
    if start_date is None:
        start_date = date.today()
        end_date = date.today()
    return get_github_csse_daily(start_date=start_date, end_date=end_date)
