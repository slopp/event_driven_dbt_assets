from dagster import SourceAsset, asset, OpExecutionContext, AssetIn, observable_source_asset, DataVersion
import pandas as pd
from typing import List
from event_driven_dbt_assets.resources import CSVWatcher

SOURCES = ["customers", "orders"]

def source_asset_factory(source: str) -> observable_source_asset:
     """ Given a source, create a sourcew asset representing the raw source file """
     @observable_source_asset(
         name=f"source_{source}",
         io_manager_key="csv_reader"
     )
     def observed_source(context: OpExecutionContext, csv_mtime: CSVWatcher):
         f""" source asset for {source} """
         mtime = csv_mtime.get_mtime(source)
         return DataVersion(str(mtime))
     
     return observed_source

source_assets = [source_asset_factory(s) for s in SOURCES]

def asset_factory(source: str) -> asset:
    """ Given a source, create an asset representing a loaded table for the source """
    @asset(
        name=source,
        key_prefix="raw_data",
        io_manager_key="warehouse",
        ins={f"source_{source}": AssetIn(f"source_{source}")}
    )
    def loaded_source(context: OpExecutionContext, **kwargs):
        f""" The raw {source} data from the csv file loaded into our warehouse."""
        
        for asset_in_key, asset_in_value in kwargs.items():
            data = asset_in_value 

        context.add_output_metadata(
            {
                "Preview": data.to_markdown(),
                "SQL to Query": f"SELECT * FROM raw_data.{source}"
            }
        )
        return data

    return loaded_source

raw_loaded_assets = [asset_factory(s) for s in SOURCES]