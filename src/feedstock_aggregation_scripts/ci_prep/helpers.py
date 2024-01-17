import pathlib
from datetime import datetime

import numpy as np
import pandas as pd
from loguru import logger as log

from ..data_prep.constants import FDCIC_CROPS, NOT_FOUND
from ..fd_cic.defaults_2021 import get_fdcic_2021_defaults
from ..general import read_verified_file
from .constants import (
    BULK_TEMPLATE_COLS,
    FDCIC_DIESEL,
    FDCIC_ELECTRICITY,
    FDCIC_GAS,
    FDCIC_LPG,
    FDCIC_NG,
)


def get_datetime(year: int, month: int, day: int) -> datetime:
    """returns a datetime object with given date for date comparisons"""
    return datetime.strptime(
        str(year) + "-" + str(month) + "-" + str(day) + " 00:00:00", "%Y-%m-%d %H:%M:%S"
    )


def mark_verified_fields(
    source_df: pd.DataFrame, path_to_data: str | pathlib.Path, grower: str
):
    verified = read_verified_file(path_to_data, grower)
    # mark verified fields
    if not verified.empty:
        source_df["Verified_field"] = source_df.Field_name.apply(
            lambda f: True if verified.Field_name.isin([f]).any() else False
        )

    return source_df


def clean_input_type(df: pd.DataFrame) -> pd.DataFrame:
    if "Input_type" in df.columns:
        df.Input_type = df.Input_type.apply(lambda i: np.NaN if i == "OTHER" else i)
        return df
    else:
        log.warning("no column `Input_type` in data frame")
        return df


def get_attributes_from_all_sources(col_name: str, data_sources: dict) -> pd.Series:
    attributes = pd.Series(dtype="object")

    for name, source in data_sources.items():
        if col_name in source.columns:
            attributes = pd.concat([attributes, pd.Series(source[col_name].unique())])
        else:
            log.warning(f"column {col_name} not in source {name}")

    return attributes.unique()


# derive `TILL_PRACTICE` from `NUM_TILL_PASSES` and `TILL_DEPTH`
# according to tillage business rules
def derive_tillage_practice_from_passes_and_depth():
    pass


# set fuel to defaults
def get_default_for_fuel_type_and_unit(bulk_template, fuel_type):
    """Returns an artificial operation entry to set the default for the required fuel_type"""
    # initialise temp frame
    pd.DataFrame(columns=BULK_TEMPLATE_COLS)

    defaults = get_fdcic_2021_defaults()
    fuel_value = defaults.get(fuel_type, NOT_FOUND)

    if fuel_value == NOT_FOUND:
        pass

    if fuel_type in [FDCIC_DIESEL, FDCIC_GAS, FDCIC_LPG]:
        pass

    elif fuel_type == FDCIC_NG:
        pass

    elif fuel_type == FDCIC_ELECTRICITY:
        pass

    else:
        log.warning(f"unknown fuel type {fuel_type}")
        pass


def initialize_decision_matrix(bulk_mapping_overview: pd.DataFrame) -> pd.DataFrame:
    fields = get_attributes_from_all_sources(
        "Field_name", {"Bulk_overview": bulk_mapping_overview}
    )

    return pd.DataFrame(columns=["Field_name"], data=fields)


def adjust_operation_type(op_type: str) -> str:
    """For upload the operation type needs to be
    one of the following:

    - TILLAGE
    - APPLYING_PRODUCTS
    - IRRIGATION
    - DRY_DOWN
    - HARVEST
    """
    app_prod_types = [
        "Application",
        "Planting",
        "Spreading",
        "Seedbed preparation",
        "Spraying",
        "Injecting",
        "Aerial Spraying",
    ]

    if not isinstance(op_type, str):
        log.warning(f"operation type {op_type} not a string")
        return op_type

    if op_type == "Tillage":
        return "TILLAGE"

    elif op_type == "Harvest":
        return "HARVEST"

    # TODO: add cases for IRRIGATION and DRY_DOWN

    elif op_type in app_prod_types:
        return "APPLYING_PRODUCTS"

    else:
        log.warning(f"unknown operation type: {op_type}")
        return op_type


def adjust_input_type(input_type: str, input_unit: str) -> str | None:
    """For upload the input type needs to be one of the
    following (depending on product name):

    - FERTILIZER
    - SEED
    - HERBICIDE
    - FUNGICIDE
    - INSECTICIDE
    """
    if input_unit == "BAG":
        return "SEED"

    elif not isinstance(input_type, str):
        return input_type

    elif input_type.lower() in ["fertiliser", "fertilizer"]:
        return "FERTILIZER"

    elif input_type.lower() == "herbicide":
        return "HERBICIDE"

    elif input_type.lower() == "insecticide":
        return "INSECTICIDE"

    elif input_type.lower() == "fungicide":
        return "FUNGICIDE"

    elif input_type.lower() in ["seed", "seeds"]:
        return "SEED"

    else:
        log.warning(f"unknown input type {input_type} --> set to NaN")
        return None


def determine_major_crop_types(field: str, decisions: pd.DataFrame) -> str | None:
    temp = decisions[
        (decisions["Field_name"].isin([field]))
        & (decisions["Crop_type"].isin(FDCIC_CROPS))
    ]
    num_crops = len(temp["Crop_type"].unique())
    if num_crops == 1:
        return temp["Crop_type"].iloc[0]

    elif num_crops > 1:
        return "Potential_split_field"

    else:
        return None


def adjust_crop_type(field: str, decisions: pd.DataFrame) -> pd.DataFrame:
    temp = decisions[decisions["Field_name"].isin([field])]

    return temp["Major_crop_type"].iloc[0]


def adjust_yield_for_secondary_crops(bulk_template: pd.DataFrame) -> pd.DataFrame:
    bulk_template["OPERATION_NAME"].fillna("", inplace=True)

    for idx, row in bulk_template.iterrows():
        if not row["OPERATION_TYPE"] == "HARVEST":
            continue

        if not row["YIELD"] >= 0.0 and not any(
            t in row["OPERATION_NAME"].title() for t in ["Bale", "Rake", "Baling"]
        ):
            continue

        yield_col_loc = bulk_template.columns.get_loc("YIELD")

        # TODO: WHY ARE WE DOING THIS? THE BE CAN ONLY HANDLE INPUT RATES ON TRUE PRODUCTS?
        # input_rate_col_loc = bulk_template.columns.get_loc("INPUT_RATE")
        #
        # if not pd.isna(bulk_template.iloc[idx, input_rate_col_loc]):
        #     log.warning(
        #         f"overwriting INPUT_RATE for HARVEST operation on field {row['FIELD_NAME']}"
        #     )
        # # transfer value from `YIELD` column to `INPUT_RATE` column
        # bulk_template.iloc[idx, input_rate_col_loc] = bulk_template.iloc[
        #     idx, yield_col_loc
        # ]

        # Erase value in `YIELD` column, if crop is not main crop.
        # To accomplish this, we are checking for FD-CIC crops and
        # make sure to exclude baling operations.
        if (row["CROP_TYPE"] not in FDCIC_CROPS) or (
            any(t in row["OPERATION_NAME"].title() for t in ["Bale", "Rake", "Baling"])
        ):
            bulk_template.iloc[idx, yield_col_loc] = None

    return bulk_template
