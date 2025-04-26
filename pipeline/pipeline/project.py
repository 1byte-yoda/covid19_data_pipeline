from pathlib import Path

from dagster_dbt import DbtProject

covid19_dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "covid19_dbt").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
covid19_dbt_project.prepare_if_dev()