import os
import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ...data_prep.constants import DATA_AGGREGATORS
from ...general import read_verified_file
from ..readers import read_cleaned_files
from .applications import create_app_input_comparison_by_crop
from .harvest import create_harvest_input_comparison
from .tillage import create_tillage_input_comparison


def add_max_delta_for_inputs(inputs: pd.DataFrame) -> pd.DataFrame:
    # get data source columns
    rel_cols = [col for col in inputs.columns if col in DATA_AGGREGATORS]

    # line-by-line: add max delta
    for i, row in inputs.iterrows():
        temp = row[rel_cols]
        min_ = np.nanmin(temp.values)
        max_ = np.nanmax(temp.values)

        # set value
        inputs.loc[i, "Max_delta"] = max_ - min_

    return inputs


def generate_comprehensive_overview_comparison_of_inputs(
    path_to_data: str | pathlib.Path,
    path_to_processed: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
) -> pd.DataFrame:
    # read-in cleaned files
    log.info("reading cleaned files...")
    cleaned_files = read_cleaned_files(path_to_processed, grower, growing_cycle)

    if len(cleaned_files) == 0:
        log.warning(f"no cleaned files at {path_to_processed} for grower {grower}")
        return pd.DataFrame()

    log.info("creating input comparison overview for planting and applications...")
    apps = create_app_input_comparison_by_crop(cleaned_files)
    log.info("creating input comparison overview for tillage...")
    till = create_tillage_input_comparison(cleaned_files)
    log.info("creating input comparison overview for harvest...")
    harvest = create_harvest_input_comparison(cleaned_files)

    inputs = pd.concat([apps, till, harvest])
    inputs = inputs.sort_values(
        by=["Field_name", "Operation_type", "Crop_type", "Operation_type", "Product"],
        ascending=True,
        ignore_index=True,
    )

    verified = read_verified_file(path_to_data, grower)
    # mark verified fields
    if not verified.empty:
        inputs["Verified_field"] = inputs.Field_name.apply(
            lambda f: True if verified.Field_name.isin([f]).any() else False
        )
        inputs = pd.merge(
            inputs, verified[["Field_name", "Area_applied"]], on="Field_name"
        )
        inputs = inputs.rename(columns={"Area_applied": "Acres"})
    else:
        inputs["Verified_field"] = np.nan
        inputs["Acres"] = np.nan

    # change column order
    cols = ["Field_name", "Crop_type", "Operation_type"]
    inputs = inputs[[*cols, *inputs.drop(columns=[*cols])]]

    inputs = add_max_delta_for_inputs(inputs)
    inputs["Max_delta_per_acre"] = inputs.Max_delta / inputs.Acres

    return inputs


def create_comprehensive_overview_comparison_of_inputs(
    path_to_data: str | pathlib.Path,
    path_to_processed: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
) -> pd.DataFrame:
    overview = generate_comprehensive_overview_comparison_of_inputs(
        path_to_data, path_to_processed, grower, growing_cycle
    )

    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if overview.empty:
        log.warning(
            f"No data to save for {grower}_data_discrepancies_overview_{growing_cycle}.csv"
        )
    else:
        overview.to_csv(
            path_to_dest.joinpath(
                grower + "_data_discrepancies_overview_" + str(growing_cycle) + ".csv"
            ),
            index=False,
        )

    return overview
