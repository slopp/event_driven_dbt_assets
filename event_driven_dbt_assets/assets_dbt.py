from dagster_dbt import load_assets_from_dbt_project
from dagster._utils import file_relative_path

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")    

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR, 
    profiles_dir=DBT_PROJECT_DIR
)