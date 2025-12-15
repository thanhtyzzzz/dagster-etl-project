"""
Dagster Definitions
Load all assets and configure resources
"""
from dagster import Definitions, load_assets_from_modules
from dagster._core.definitions.executor_definition import in_process_executor
from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    executor=in_process_executor,
)
