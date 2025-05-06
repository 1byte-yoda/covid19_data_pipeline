from datetime import timedelta, date

from dagster import asset, Output, AssetMaterialization, AssetExecutionContext, AssetSelection


@asset(group_name="external")
def register_existing_s3_data(context: AssetExecutionContext):
    start, end = date(2020, 1, 22), date(2023, 2, 28)

    group_selection = AssetSelection.groups("default")
    asset_keys = group_selection.resolve(context.repository_def.asset_graph)
    while start <= end:
        partition = start.strftime("%Y-%m-%d")

        for asset_key in asset_keys:
            context.log_event(
                AssetMaterialization(
                    asset_key=asset_key, partition=partition, metadata={"external": True, "source": "manual_s3_sync", "notes": f"Partition {partition} marked as fulfilled via external upload."}
                )
            )

        start = start + timedelta(days=1)

    yield Output(None)
