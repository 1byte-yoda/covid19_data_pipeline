from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets.covid_datahub import covid_datahub_assets
from .assets.covid19_github_csse import covid19_github_csse_assets
from .assets.dbt_core import covid19_dbt_dbt_assets
from .assets.helper import register_existing_s3_data
from .project import covid19_dbt_project
from .schedules import schedules

from dagster_dlt import DagsterDltResource

dlt_resource = DagsterDltResource()
dbt_resource = DbtCliResource(project_dir=covid19_dbt_project)

defs = Definitions(
    assets=[covid19_github_csse_assets, covid_datahub_assets, covid19_dbt_dbt_assets, register_existing_s3_data],
    schedules=schedules,
    resources={
        "dbt": dbt_resource,
        "dagster_dlt": dlt_resource,
    },
)
