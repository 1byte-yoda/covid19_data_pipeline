from pathlib import Path

from dagster_dbt import DbtProject


DBT_PROJECT_NAME = "transformer"


def get_project_root():
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / DBT_PROJECT_NAME).exists():
            return parent
    return None


DBT_PROJECT_DIR = get_project_root() / "transformer"


covid19_dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
covid19_dbt_project.prepare_if_dev()
