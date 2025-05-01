from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import covid19_github_csse_assets, covid_datahub_assets, covid19_dbt_dbt_assets
from .project import covid19_dbt_project
from .schedules import schedules

from dagster_embedded_elt.dlt import DagsterDltResource

dlt_resource = DagsterDltResource()
dbt_resource = DbtCliResource(project_dir=covid19_dbt_project)

defs = Definitions(
    assets=[covid19_github_csse_assets, covid_datahub_assets, covid19_dbt_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": dbt_resource,
        "dagster_dlt": dlt_resource,
    },
)