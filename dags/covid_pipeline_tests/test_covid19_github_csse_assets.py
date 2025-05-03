import os
from datetime import datetime
from unittest.mock import patch

from dagster import AssetKey, PartitionKeyRange, build_asset_context
from dagster_dlt import DagsterDltResource
from deltalake import DeltaTable

from dags.covid_pipeline.assets.covid19_github_csse import covid19_github_csse_assets
from dags.covid_pipeline.assets.schemas import Covid19CsseGithub
from dags.covid_pipeline_tests.conftest import mocked_requests_get


@patch("dags.covid_pipeline.assets.covid19_github_csse.requests.get", side_effect=mocked_requests_get)
def test_covid_github_csse_csv_asset_schema_is_correctly_materialized(_, tmp_path_factory):
    # GIVEN - Covid19CsseGithub data for 2023-02-16
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    context = build_asset_context(partition_key_range=PartitionKeyRange(datetime(2023, 2, 16), datetime(2023, 2, 17)))  # noqa
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir

    # WHEN - DLT Pipeline Ingest Data from patched mocked_requests_get
    result = covid19_github_csse_assets(context=context, dagster_dlt=DagsterDltResource())
    materialized_asset = next(result)  # noqa

    # THEN
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/github_csse_daily").to_pandas()
    covid19_github_schema = Covid19CsseGithub.dagster_table_schema()

    assert materialized_asset.asset_key == AssetKey("github_csse_daily")
    assert materialized_asset.metadata["dataset_name"] == "covid19"
    assert materialized_asset.metadata["dagster/column_schema"] == covid19_github_schema
    assert len(df) == 5
    assert df["id"].nunique() == len(df)


@patch("dags.covid_pipeline.assets.covid19_github_csse.requests.get", side_effect=mocked_requests_get)
def test_covid_github_csse_csv_asset_is_idempotent(_, tmp_path_factory):
    # GIVEN - Covid19CsseGithub data for 2023-02-16
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir

    # WHEN - Run the dlt ingester twice with the same partition/date key
    for _ in range(2):
        context = build_asset_context(partition_key_range=PartitionKeyRange(datetime(2023, 2, 16), datetime(2023, 2, 17)))  # noqa
        result = covid19_github_csse_assets(context=context, dagster_dlt=DagsterDltResource())
        _ = next(result)  # noqa

    # THEN - Data Must Be Merged into the Delta Table without duplicates
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/github_csse_daily").to_pandas()

    assert len(df) == 5
    assert len(df) == df["id"].nunique()


@patch("dags.covid_pipeline.assets.covid19_github_csse.requests.get", side_effect=mocked_requests_get)
def test_github_covid_csv_asset_download_with_range_key(_, tmp_path_factory):
    # GIVEN - Covid19CsseGithub Ranged Data for 2023-02-16 to 2023-02-17
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir

    # WHEN - Run the dlt ingester with the ranged partition/date key
    context = build_asset_context(
        partition_key_range=PartitionKeyRange(datetime(2023, 2, 16), datetime(2023, 2, 18))  # noqa
    )
    result = covid19_github_csse_assets(context=context, dagster_dlt=DagsterDltResource())
    _ = next(result)  # noqa

    # THEN - Data for 20203-02-16 and 2023-02-17 Must Be Inserted to the Delta Table without duplicates
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/github_csse_daily").to_pandas()

    assert len(df) == 10
    assert df["id"].nunique() == len(df)
