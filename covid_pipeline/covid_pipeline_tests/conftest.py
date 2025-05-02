from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import pandas as pd
import pytest
from deltalake import DeltaTable, write_deltalake


def make_df(query_date: str) -> pd.DataFrame:
    dt = datetime.strptime(query_date, "%Y-%m-%d")
    n_rows = 5
    df = pd.DataFrame(
        {
            "fips": [1009] * n_rows,
            "admin2": np.random.choice(["Barbour"], n_rows),
            "province_state": np.random.choice(["Alabama"], n_rows),
            "country_region": ["US"] * n_rows,
            "last_update": [dt.isoformat()] * n_rows,
            "lat": [139.55] * n_rows,
            "longx": [-20.8] * n_rows,
            "confirmed": [100000] * n_rows,
            "deaths": [20] * n_rows,
            "recovered": [50000] * n_rows,
            "active": [49950] * n_rows,
            "combined_key": [f"County{i}, State" for i in range(n_rows)],
            "incident_rate": [20] * n_rows,
            "case_fatality_ratio": [5] * n_rows,
        }
    )
    return df


def make_csv(query_date: str) -> bytes:
    df = make_df(query_date=query_date)
    csv_buffer = BytesIO()
    csv_buffer.seek(0)
    df.to_csv(csv_buffer, index=False)  # noqa
    return csv_buffer.getvalue()


@pytest.fixture
def mock_delta_table(tmp_path_factory):
    def query(start_date: datetime, end_date: datetime) -> str:
        tmp_dir = tmp_path_factory.mktemp("s3")
        fake_s3_bucket_url = f"file://{tmp_dir}/covid19/covid19datahub"
        while start_date < end_date:
            df = make_df(query_date=start_date.strftime("%Y-%m-%d"))
            df["year"] = start_date.year
            df["month"] = start_date.month
            df["day"] = start_date.day
            df["date"] = start_date.date()
            df = df.reset_index(names="id")

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