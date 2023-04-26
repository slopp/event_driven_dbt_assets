from dagster import ConfigurableIOManager, ConfigurableResource, InputContext, OutputContext, AssetObservation, observable_source_asset, AssetKey
from typing import Any
import pandas as pd
import os

def file_from_source_asset_key(key: AssetKey) -> str:
    """ Translates from a source asset key like ['source_orders'] to orders.csv based on the naming conventions in this sample project"""
    asset_name: str = key.path[-1]
    file = asset_name.removeprefix("source_")
    return f"{file}.csv"

class CSVIOManager(ConfigurableIOManager):
    dir: str

    def load_input(self, context: InputContext) -> pd.DataFrame:
        file = file_from_source_asset_key(context.asset_key)
        path = f"{self.dir}/{file}"
        return pd.read_csv(path)
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        raise Exception("The CSVIOManager is currently for source assets only!")
    
class CSVWatcher(ConfigurableResource):
    dir: str

    def get_mtime(self, file):
        file = f"{self.dir}/{file}.csv"
        return os.path.getmtime(file)