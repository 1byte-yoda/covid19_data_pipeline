from typing import Optional, Any

from dagster import DailyPartitionsDefinition, AssetKey, AssetSpec, AssetDep
from dagster_dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator


daily_partitions = DailyPartitionsDefinition(start_date="2020-01-22", end_date="2023-03-09", fmt="%Y-%m-%d")


class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Overrides asset spec to override asset key to be the dlt resource name."""
        default_spec = super().get_asset_spec(data)
        deps = [AssetDep(asset=AssetKey(f"dlt_{data.resource.source_name}"), partition_mapping=d.partition_mapping) for d in default_spec.deps]
        return default_spec.replace_attributes(key=AssetKey(f"{data.resource.name}"), deps=deps)

    def get_group_name(self, dbt_resource_props: dict[str, Any]) -> Optional[str]:
        return "default"


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: dict[str, Any]) -> Optional[str]:
        return "default"
