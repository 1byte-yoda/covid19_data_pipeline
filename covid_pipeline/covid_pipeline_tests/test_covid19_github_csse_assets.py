import os
from datetime import datetime
from io import BytesIO
from unittest.mock import patch

import dagster as dg
from dagster import AssetKey, TableSchema, TableColumn, TableColumnConstraints, TableConstraints, PartitionKeyRange
from dagster_dlt import DagsterDltResource
from deltalake import DeltaTable

from covid_pipeline.covid_pipeline.assets.covid19_github_csse import covid19_github_csse_assets
from covid_pipeline.covid_pipeline_tests.conftest import mocked_requests_get


@patch("covid_pipeline.covid_pipeline.assets.covid19_github_csse.requests.get", side_effect=mocked_requests_get)
def test_covid_datahub_asset_schema_is_correctly_materialized(_, tmp_path_factory):
    # GIVEN
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    context: dg.AssetExecutionContext = dg.build_asset_context(partition_key_range=PartitionKeyRange(datetime(2023, 2, 16), datetime(2023, 2, 17)))  # noqa
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir

    # WHEN
    result = covid19_github_csse_assets(context=context, dagster_dlt=DagsterDltResource())
    materialized_asset = next(result)  # noqa

    # THEN
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/github_csse_daily").to_pandas()
    covid19_github_schema = TableSchema(
        columns=[
            TableColumn(name="row_num", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="fips", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="admin2", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="province_state", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="country_region", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="last_update", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="lat", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="longx", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="confirmed", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="deaths", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="recovered", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="active", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="combined_key", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="incident_rate", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="case_fatality_ratio", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="file_md5", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="source_url", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
            TableColumn(name="id", type="text", description=None, constraints=TableColumnConstraints(nullable=False, unique=False, other=["primary_key"]), tags={}),
            TableColumn(name="load_date", type="timestamp", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["precision", "timezone"]), tags={}),  # noqa
            TableColumn(name="year", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
            TableColumn(name="month", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
            TableColumn(name="day", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
        ],
        constraints=TableConstraints(other=[]),
    )

    assert materialized_asset.asset_key == AssetKey("github_csse_daily")
    assert materialized_asset.metadata["dataset_name"] == "covid19"
    assert materialized_asset.metadata["dagster/column_schema"] == covid19_github_schema
    assert len(df) == 5
    assert df["id"].nunique() == len(df)


@patch("covid_pipeline.covid_pipeline.assets.covid19_github_csse.requests.get", side_effect=mocked_requests_get)
def test_github_covid_csv_asset_is_idempotent(_, tmp_path_factory):
    # GIVEN
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir

    # WHEN - Run the dlt ingester twice with the same partition/date key
    for _ in range(2):
        context: dg.AssetExecutionContext = dg.build_asset_context(partition_key_range=PartitionKeyRange(datetime(2023, 2, 16), datetime(2023, 2, 17)))  # noqa
        result = covid19_github_csse_assets(context=context, dagster_dlt=DagsterDltResource())
        _ = next(result)

    # THEN - Data Must Be Merged into the Delta Table without duplicates
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/github_csse_daily").to_pandas()

    assert len(df) == 5
    assert len(df) == df["id"].nunique()


@patch("covid_pipeline.covid_pipeline.assets.covid19_github_csse.requests.get", side_effect=mocked_requests_get)
def test_github_covid_csv_asset_download_with_range_key(_, tmp_path_factory):
    # GIVEN
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir

    # WHEN - Run the dlt ingester with the ranged partition/date key

    context: dg.AssetExecutionContext = dg.build_asset_context(
        partition_key_range=PartitionKeyRange(datetime(2023, 2, 16), datetime(2023, 2, 18))  # noqa
    )
    result = covid19_github_csse_assets(context=context, dagster_dlt=DagsterDltResource())
    _ = next(result)  # noqa

    # THEN - Data for 20203-02-16 and 2023-02-17 Must Be Inserted to the Delta Table without duplicates
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/github_csse_daily").to_pandas()

    assert len(df) == 10
    assert df["id"].nunique() == len(df)
