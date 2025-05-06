from dagster import Definitions, load_assets_from_package_module
from dagster_dbt import DbtCliResource

from . import assets
from .jobs import jobs
from .project import covid19_dbt_project
from .schedules import schedules

from dagster_dlt import DagsterDltResource

dlt_resource = DagsterDltResource()
dbt_resource = DbtCliResource(project_dir=covid19_dbt_project)

_assets = load_assets_from_package_module(assets)

defs = Definitions(
    assets=_assets,
    schedules=schedules,
    jobs=jobs,
    resources={
        "dbt": dbt_resource,
        "dagster_dlt": dlt_resource,
    },
)
