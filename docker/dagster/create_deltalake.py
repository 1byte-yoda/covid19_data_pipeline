import os
from typing import Union

import pandas as pd

from deltalake import write_deltalake, DeltaTable
from deltalake._internal import TableNotFoundError
from loguru import logger

from dags.covid_pipeline.assets.schemas import Covid19DataHub, Covid19CsseGithub


MINIO_ENDPOINT_URL = os.environ.get("MINIO_ENDPOINT_URL", "localhost:9050")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "datalake")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "datalake")

storage_options = {"allow_http": "true", "endpoint_url": f"http://{MINIO_ENDPOINT_URL}", "access_key_id": MINIO_ACCESS_KEY, "secret_access_key": MINIO_SECRET_KEY}
s3_bucket = os.environ.get("DESTINATION__FILESYSTEM__BUCKET_URL", "s3://scratch")


def _safe_create_deltalake(s3_uri: str, table_schema: Union[Covid19CsseGithub, Covid19DataHub]):
    try:
        dt = DeltaTable(table_uri=s3_uri, storage_options=storage_options)
        logger.debug(f"Table for {s3_uri} already exists with {len(dt.to_pandas())} rows")
    except TableNotFoundError:
        logger.debug("Table not found!")
        logger.debug(f"Creating deltalake table {s3_uri}")
        df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in table_schema.pandas_schema().items()})
        write_deltalake(
            table_or_uri=s3_uri, data=df, partition_by=["year", "month", "day"], mode="append", storage_options=storage_options
        )


def main():
    github_csse_daily_s3 = f"{s3_bucket}/covid19/github_csse_daily"
    covid19datahub_s3 = f"{s3_bucket}/covid19/covid19datahub"

    _safe_create_deltalake(s3_uri=github_csse_daily_s3, table_schema=Covid19CsseGithub())
    _safe_create_deltalake(s3_uri=covid19datahub_s3, table_schema=Covid19DataHub())


if __name__ == '__main__':
    main()