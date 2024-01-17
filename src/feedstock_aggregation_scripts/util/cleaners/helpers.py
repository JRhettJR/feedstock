import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ...config import settings
from ...data_prep.constants import DA_GRANULAR, GRAN_PLANTING
from ..readers.general import read_file_by_file_type

# Define globals
qu_converter = pd.read_csv(
    pathlib.Path(settings.data_prep.source_path).joinpath("unit_conversions.csv")
)

qm_converter = pd.read_csv(
    pathlib.Path(settings.data_prep.source_path).joinpath("unit_mapping_table.csv")
)

# extract all planting related observed units
PLANTING_UNITS_RAW = qu_converter[qu_converter.target_unit == "BAG"].unit.unique()
SEEDING_UNITS_RAW = qm_converter[qm_converter.clear_unit == "AC"].unit.unique()

seeding_planting_params = np.concatenate((PLANTING_UNITS_RAW, SEEDING_UNITS_RAW))


def generate_Granular_sub_crop_type_in_harvest(crop_type):
    if not isinstance(crop_type, str):
        return crop_type

    temp = crop_type.split("-")[-1]
    return temp.strip()


def clean_Granular_crop_type_in_harvest(crop_type):
    if not isinstance(crop_type, str):
        return crop_type

    temp = crop_type.split("-")[0]
    temp = temp.replace("Commercial", "")
    return temp.strip()


def filter_Granular_apps(apps, path_to_data, grower, growing_cycle):
    """the filtered apps does not include planting data as contained in the internally generated planting file"""
    planting = read_file_by_file_type(
        path_to_data,
        grower,
        growing_cycle,
        data_aggregator=DA_GRANULAR,
        file_type=GRAN_PLANTING,
    )
    # tillage = read_file_by_file_type(path_to_data, grower, growing_cycle, data_aggregator=DA_GRANULAR, file_type=GRAN_TILLAGE)

    if planting.empty:
        log.warning(
            f"missing planting file for system Granular for grower {grower} and cycle {growing_cycle}. Application file will contain planting info"
        )
        return apps

    # apps = get_cleaned_file_by_file_type(path_to_data, grower, growing_cycle, data_aggregator=DA_GRANULAR, file_type=GRAN_APPLICATION)
    planting = planting.drop(columns="Product_type")
    apps = (
        pd.merge(apps, planting, indicator=True, how="outer")
        .query('_merge=="left_only"')
        .drop("_merge", axis=1)
    )

    return apps.reset_index(drop=True)


# %%
def clean_units(unit, verbose=False):
    if not isinstance(unit, str):
        return unit

    if unit == "---" or unit == "--":
        return np.nan

    u = qm_converter[qm_converter.unit == unit.lower()]

    if u.empty:
        if unit not in qm_converter.clear_unit.unique() and verbose:
            log.warning(f"no mapping for unit {unit}")
        return unit
    # extract clear unit record from table
    clear_unit = u.clear_unit.iloc[0]
    # avoid overwriting units that have no `clear_unit` assigned yet
    u = clear_unit if not pd.isna(clear_unit) else unit
    return u


def convert_quantity_by_unit(quantity, unit, params_to_convert=None):
    if params_to_convert is None:
        params_to_convert = []
    if not isinstance(unit, str):
        return quantity, unit
    unit = clean_units(unit)

    if len(params_to_convert) != 0:
        # this check enables us to convert only planting related units (seeds) into
        # the only accepted unit by BE: "BAG"
        # to achieve this we are using the above defined `PLANTING_UNITS_RAW` passed
        # in as the `params_to_convert`. If the passed `unit` is not within this list,
        # we simply return the original `quantity` and `unit`.
        if unit in params_to_convert:
            temp = qu_converter[qu_converter.unit == unit.lower()]
        else:
            return quantity, unit
    else:
        temp = qu_converter[qu_converter.unit == unit.lower()]

    if temp.empty:
        return quantity * 1, unit

    conversion_factor = temp.conversion_factor.iloc[0]
    u = temp.target_unit.iloc[0]

    return quantity * conversion_factor, u


def add_missing_columns(df, columns):
    temp = df.copy()
    for col in columns:
        if col not in temp.columns:
            temp[col] = np.nan
    return temp
