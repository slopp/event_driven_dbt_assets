from dagster import Definitions, load_assets_from_modules, FreshnessPolicy, AutoMaterializePolicy, ScheduleDefinition, DefaultScheduleStatus
from dagster._utils import file_relative_path
from event_driven_dbt_assets import assets_dbt, assets_raw
from event_driven_dbt_assets.resources import CSVIOManager, CSVWatcher
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_dbt import dbt_cli_resource
from event_driven_dbt_assets.jobs import keep_source_data_udpated


DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
DUCKDB_DATABASE = file_relative_path(__file__, "../dbt_project/example.duckdb")

assets_raw = load_assets_from_modules(
    modules=[assets_raw],
    auto_materialize_policy=AutoMaterializePolicy.eager() # materialize this asset as soon as new source data is observed
)

assets_dbt = load_assets_from_modules(
    modules=[assets_dbt],
    key_prefix="analytics",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5), # alternatively set freshness policies per dbt model via config metadata
    auto_materialize_policy=AutoMaterializePolicy.lazy() # only materialize this asset to meet its freshness policy
)

defs = Definitions(
    assets=[*assets_raw, *assets_dbt],
    jobs=[keep_source_data_udpated],
    resources={
        "csv_reader": CSVIOManager(dir="source_data"),
        "csv_mtime": CSVWatcher(dir="source_data"),
        "warehouse": DuckDBPandasIOManager(database=DUCKDB_DATABASE),
        "dbt": dbt_cli_resource.configured({"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROJECT_DIR})
    },
    schedules=[
        ScheduleDefinition(
            name="check_sources",
            cron_schedule="* * * * *",
            job=keep_source_data_udpated,
            default_status=DefaultScheduleStatus.RUNNING
        )
    ]
)