import os
from datetime import datetime, date
from unittest.mock import patch

import dagster as dg
import pandas as pd
from dagster import AssetKey, TableSchema, TableColumn, TableColumnConstraints, TableConstraints, PartitionKeyRange
from dagster_dlt import DagsterDltResource
from deltalake import DeltaTable

from covid_pipeline.covid_pipeline.assets.covid_datahub import covid_datahub_assets
from covid_pipeline.covid_pipeline_tests.conftest import mocked_requests_get, mock_delta_table


def test_covid19_delta_asset_schema_is_correctly_materialized(tmp_path_factory, mock_delta_table):
    # GIVEN
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    context: dg.AssetExecutionContext = dg.build_asset_context(partition_key_range=PartitionKeyRange(datetime(2023, 2, 1), datetime(2023, 2, 2)))  # noqa
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir
    s3_bucket_url = mock_delta_table(start_date=datetime(2023, 2, 1), end_date=datetime(2023, 2, 2))
    os.environ["COVID_DATAHUB_S3_BUCKET"]  = s3_bucket_url

    # WHEN
    result = covid_datahub_assets(context=context, dagster_dlt=DagsterDltResource())
    materialized_asset = next(result)  # noqa

    # THEN
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/covid19datahub").to_pandas()


    assert materialized_asset.asset_key == AssetKey("covid19datahub")
    assert materialized_asset.metadata["dataset_name"] == "covid19"
    # assert materialized_asset.metadata["dagster/column_schema"] == covid19_github_schema
    assert len(df) == 5
    assert df["row_key"].nunique() == len(df)


def test_covid19_delta_csv_asset_is_idempotent(tmp_path_factory, mock_delta_table):
    # GIVEN
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir
    s3_bucket_url = mock_delta_table(start_date=datetime(2023, 2, 1), end_date=datetime(2023, 2, 2))
    os.environ["COVID_DATAHUB_S3_BUCKET"]  = s3_bucket_url

    # WHEN
    for _ in range(2):
        context: dg.AssetExecutionContext = dg.build_asset_context(partition_key_range=PartitionKeyRange(datetime(2023, 2, 1), datetime(2023, 2, 2)))  # noqa
        result = covid_datahub_assets(context=context, dagster_dlt=DagsterDltResource())
        materialized_asset = next(result)  # noqa

    # THEN
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/covid19datahub").to_pandas()


    assert len(df) == 5
    assert df["row_key"].nunique() == len(df)


def test_covid19_asset_download_with_range_key(tmp_path_factory, mock_delta_table):
    # GIVEN
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir
    s3_bucket_url = mock_delta_table(start_date=datetime(2023, 2, 1), end_date=datetime(2023, 2, 4))
    os.environ["COVID_DATAHUB_S3_BUCKET"]  = s3_bucket_url

    # WHEN - Run the dlt ingester with the ranged partition/date key
    context: dg.AssetExecutionContext = dg.build_asset_context(
        partition_key_range=PartitionKeyRange(datetime(2023, 2, 1), datetime(2023, 2, 4))  # noqa
    )
    result = covid_datahub_assets(context=context, dagster_dlt=DagsterDltResource())
    _ = next(result)  # noqa

    # THEN - Data for 20203-02-16 and 2023-02-17 Must Be Inserted to the Delta Table without duplicates
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/covid19datahub").to_pandas()

    assert len(df) == 15
    assert df["row_key"].nunique() == len(df)
    assert df["date"].sort_values().unique().tolist() == [date(2023, 2, 1), date(2023, 2, 2), date(2023, 2, 3)]
