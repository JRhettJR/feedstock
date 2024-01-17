import pathlib

import pandas as pd
from loguru import logger as log

from ...general import clean_numeric_col
from ...util.cleaners.helpers import add_missing_columns
from ..readers import read_cleaned_files
from .applications import create_comprehensive_apps_from_cleaned_files
from .harvest import create_comprehensive_harvest_from_cleaned_files
from .tillage import create_comprehensive_tillage_from_cleaned_files


def create_comprehensive_inputs_from_cleaned_files(
    path_to_data: str | pathlib.Path,
    grower,
    growing_cycle,
):
    # read-in cleaned files
    log.info("reading cleaned files...")
    cleaned_files = read_cleaned_files(path_to_data, grower, growing_cycle)

    if len(cleaned_files) == 0:
        log.warning(f"no cleaned files at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    log.info("collecting max total input operations for planting and applications...")
    apps = create_comprehensive_apps_from_cleaned_files(cleaned_files)
    log.info("collecting max total input operations for tillage...")
    till = create_comprehensive_tillage_from_cleaned_files(cleaned_files)
    log.info("collecting max total input operations for harvest...")
    harvest = create_comprehensive_harvest_from_cleaned_files(cleaned_files)

    inputs = pd.concat([apps, till, harvest])
    inputs = inputs.sort_values(
        by=["Field_name", "Operation_start"], ascending=True, ignore_index=True
    )

    # ensure that table columns have the right type
    num_cols = [
        "Applied_total",
        "Area_applied",
        "Applied_rate",
        "Total_dry_yield",
        "Total_dry_yield_check",
        "Moisture",
    ]
    inputs = add_missing_columns(inputs, num_cols)
    for col in num_cols:
        inputs[col] = inputs[col].apply(clean_numeric_col)

    for col in ["Operation_start", "Operation_end"]:
        inputs[col] = pd.to_datetime(inputs[col])

    return inputs
