import os
from datetime import datetime

import dagster as dg
from dagster import AssetKey, PartitionKeyRange
from dagster_dlt import DagsterDltResource
from deltalake import DeltaTable

from dags.covid_pipeline.assets.covid_datahub import covid_datahub_assets
from dags.covid_pipeline.assets.schemas import Covid19DataHub


def test_covid19_delta_asset_schema_is_correctly_materialized(tmp_path_factory, mock_delta_table):
    # GIVEN - Covid19DataHub data for 2023-02-01
    partition_key_range = PartitionKeyRange(datetime(2023, 2, 1), datetime(2023, 2, 2))  # noqa
    context = dg.build_asset_context(partition_key_range=partition_key_range)
    tmp_dir = str(tmp_path_factory.mktemp("data"))

    # -- Patch below with local storage for testing
    covid19datahub_schema = Covid19DataHub.dagster_table_schema()
    s3_bucket_url = mock_delta_table(partition_key_range=partition_key_range, schema=covid19datahub_schema)
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir
    os.environ["COVID_DATAHUB_S3_BUCKET"] = s3_bucket_url

    # WHEN - DLT Pipeline Ingest Data from COVID_DATAHUB_S3_BUCKET
    result = covid_datahub_assets(context=context, dagster_dlt=DagsterDltResource())
    materialized_asset = next(result)  # noqa

    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/covid19datahub").to_pandas()

    # THEN Schema Metadata Must Match
    assert materialized_asset.asset_key == AssetKey("covid19datahub")
    assert materialized_asset.metadata["dataset_name"] == "covid19"
    assert materialized_asset.metadata["dagster/column_schema"] == covid19datahub_schema
    assert len(df) == 5
    assert df["row_key"].nunique() == len(df)


def test_covid19_delta_csv_asset_is_idempotent(tmp_path_factory, mock_delta_table):
    # GIVEN - Covid19DataHub data for 2023-02-01
    partition_key_range = PartitionKeyRange(datetime(2023, 2, 1), datetime(2023, 2, 2))  # noqa
    s3_bucket_url = mock_delta_table(partition_key_range=partition_key_range, schema=Covid19DataHub.dagster_table_schema())

    # -- Patch below with local storage for testing
    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir
    os.environ["COVID_DATAHUB_S3_BUCKET"] = s3_bucket_url

    # WHEN - DLT Pipeline Ingest The Exact Same Data Twice from COVID_DATAHUB_S3_BUCKET
    for _ in range(2):
        context: dg.AssetExecutionContext = dg.build_asset_context(partition_key_range=partition_key_range)
        result = covid_datahub_assets(context=context, dagster_dlt=DagsterDltResource())
        materialized_asset = next(result)  # noqa

    # THEN Data Must Be Merged into the Delta Table without duplicates
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/covid19datahub").to_pandas()

    assert len(df) == 5
    assert df["row_key"].nunique() == len(df)


def test_covid19_asset_download_with_range_key(tmp_path_factory, mock_delta_table):
    # GIVEN - Covid19DataHub Ranged Data from 2023-02-01 to 20203-02-03
    partition_key_range = PartitionKeyRange(datetime(2023, 2, 1), datetime(2023, 2, 4))  # noqa
    s3_bucket_url = mock_delta_table(partition_key_range, schema=Covid19DataHub.dagster_table_schema())

    tmp_dir = str(tmp_path_factory.mktemp("data"))
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = tmp_dir
    os.environ["COVID_DATAHUB_S3_BUCKET"] = s3_bucket_url

    # WHEN - DLT Pipeline Ingest Data from COVID_DATAHUB_S3_BUCKET
    context: dg.AssetExecutionContext = dg.build_asset_context(partition_key_range=partition_key_range)
    result = covid_datahub_assets(context=context, dagster_dlt=DagsterDltResource())
    _ = next(result)  # noqa

    # THEN - Data for 20203-02-16 and 2023-02-17 Must Be Inserted to the Delta Table without duplicates
    df = DeltaTable(table_uri=f"file://{tmp_dir}/covid19/covid19datahub").to_pandas()

    assert len(df) == 15
    assert df["row_key"].nunique() == len(df)
    assert df["date"].sort_values().unique().tolist() == ["2023-02-01", "2023-02-02", "2023-02-03"]
