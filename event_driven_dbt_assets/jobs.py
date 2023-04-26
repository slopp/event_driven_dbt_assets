from dagster import op, job, OpExecutionContext, DataVersion, AssetKey, DataProvenance, AssetObservation
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from event_driven_dbt_assets.resources import CSVWatcher
from event_driven_dbt_assets.assets_raw import SOURCES

@op
def watch_for_data_updates(context: OpExecutionContext, csv_mtime: CSVWatcher):
    """ Checks if data has been updated and if so notifies Dagster so downstream assets are eligible for auto materialization """ 

    # TODO Here is where you pull events from your topic, or check for file updates, or whatever
    # Then given the event, or the file mtime, or whatever, record an observation for the appropriate source assets

    for s in SOURCES:
        mtime = csv_mtime.get_mtime(s) 
        
        context.log_event(
            AssetObservation(
                asset_key=AssetKey(f"source_{s}"), 
                tags= {
                    DATA_VERSION_TAG: str(mtime)
                }               
            )
        )

    return "success"

@job
def keep_source_data_udpated():
    """ Checks if data has been updated and if so notifies Dagster so downstream assets are eligible for auto materialization """ 
    watch_for_data_updates()
