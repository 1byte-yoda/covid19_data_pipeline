from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import pandas as pd
import pytest
from dagster import PartitionKeyRange, TableSchema
from deltalake import write_deltalake

from dags.covid_pipeline.assets.schemas import Covid19CsseGithub

np.random.seed(50)


def generate_dataframe_from_table_schema(schema: TableSchema, load_date: datetime, num_rows: int = 5) -> pd.DataFrame:
    type_mapping = {
        "bigint": lambda: np.array([10000] * num_rows, dtype=np.int64),
        "int": lambda: np.array([200] * num_rows, dtype=np.int32),
        "double": lambda: np.round(np.array([500.25] * num_rows, dtype=np.double), 2),
        "text": lambda: [f"sample_{i}" for i in range(num_rows)],
        "timestamp": lambda: pd.Timestamp(load_date)
    }

    data = {}
    for column in schema.columns:
        if "primary_key" in column.constraints.other:
            data[column.name] = [f"{str(i)}_{load_date}" for i in range(num_rows)]
        else:
            generator = type_mapping.get(column.type, lambda: [None] * num_rows)
            data[column.name] = generator()

    df = pd.DataFrame(data)

    return df


def make_csv(query_date: str) -> bytes:
    df = generate_dataframe_from_table_schema(load_date=datetime.strptime(query_date, "%Y-%m-%d"), schema=Covid19CsseGithub.dagster_table_schema())
    df = df.drop("row_num", axis=1)
    df["load_date"] = pd.Timestamp(query_date)
    csv_buffer = BytesIO()
    csv_buffer.seek(0)
    df.to_csv(csv_buffer, index=False)  # noqa
    return csv_buffer.getvalue()


@pytest.fixture
def mock_delta_table(tmp_path_factory):
    def query(partition_key_range: PartitionKeyRange, schema: TableSchema) -> str:
        start_date, end_date = partition_key_range
        tmp_dir = tmp_path_factory.mktemp("s3")
        fake_s3_bucket_url = f"file://{tmp_dir}/covid19/covid19datahub"

        while start_date < end_date:
            df = generate_dataframe_from_table_schema(schema=schema, load_date=start_date)
            df["year"] = start_date.year
            df["month"] = start_date.month
            df["day"] = start_date.day
            df["date"] = datetime.strftime(start_date, "%Y-%m-%d")

            write_deltalake(fake_s3_bucket_url, data=df, partition_by=["year", "month", "day"], mode="append")
            start_date = start_date + timedelta(days=1)
        return fake_s3_bucket_url
    return query



def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code, content):
            self.json_data = json_data
            self.status_code = status_code
            self.content = content

        def json(self):
            return self.json_data

    if args[0] == "https://raw.githubusercontent.com/cssegisanddata/covid-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/02-16-2023.csv":
        return MockResponse({"key1": "value1"}, 200, make_csv(query_date="2023-02-16"))
    elif args[0] == "https://raw.githubusercontent.com/cssegisanddata/covid-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/02-17-2023.csv":
        return MockResponse({"key2": "value2"}, 200, make_csv(query_date="2023-02-17"))

    return MockResponse(None, 404, content=None)