import hashlib
from datetime import date, timedelta
from typing import Optional

import dlt
import pandas as pd
from deltalake import DeltaTable


@dlt.resource(
    name="covid19datahub",
    table_format="delta",
    table_name="covid19datahub",
    columns={"year": {"partition": True}, "month": {"partition": True}, "day": {"partition": True}},
    primary_key=["row_key"],
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def get_covid_datahub(start_date: date, end_date: date):
    query_date = start_date
    while query_date <= end_date:
        year, month, day = str(query_date.year), str(query_date.month), str(query_date.day)
        bucket_url = "s3://covid19datahub-dataset/covid19/covid19datahub"
        storage_options = {"AWS_REGION": "ap-southeast-1", "skip_signature": "true"}
        partitions = [("year", "=", year), ("month", "=", month), ("day", "=", day)]
        df = DeltaTable(bucket_url, storage_options=storage_options).to_pandas(partitions=partitions)
        df["row_num"] = df.groupby("date").cumcount() + 1
        df["row_key"] = df.apply(lambda row: hashlib.md5(f"{row['id']}_{row['date']}_{row['row_num']}".encode()).hexdigest(), axis=1)
        yield df
        query_date = query_date + timedelta(days=1)


@dlt.source
def covid_datahub_source(start_date: Optional[date] = None, end_date: Optional[date] = None):
    if start_date is None:
        start_date = date.today()
        end_date = date.today()
    df = get_covid_datahub(start_date=start_date, end_date=end_date)
    return df if df is not None else pd.DataFrame()
