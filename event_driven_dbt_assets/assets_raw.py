from dagster import SourceAsset, asset, OpExecutionContext, AssetIn
import pandas as pd
from typing import List

SOURCES = ["customers", "orders"]

source_assets = [SourceAsset(key=f"source_{s}", io_manager_key="csv_reader", description=f"source asset for {s}") for s in SOURCES]

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