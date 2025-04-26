from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import covid19_dbt_dbt_assets, covid19_github_csse_assets
from .project import covid19_dbt_project
from .schedules import schedules

from dagster_embedded_elt.dlt import DagsterDltResource

dlt_resource = DagsterDltResource()

defs = Definitions(
    assets=[covid19_github_csse_assets],
    schedules=schedules,
    resources={
        # "dbt": DbtCliResource(project_dir=covid19_dbt_project),
        "dagster_dlt": dlt_resource,
    },
)