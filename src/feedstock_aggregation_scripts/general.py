import pathlib
from datetime import datetime

import numpy as np
import pandas as pd
from loguru import logger as log

from .data_prep.constants import (
    CC_REPORT,
    LIME_REPORT,
    MANURE_REPORT,
    NOT_FOUND,
    SPLIT_FIELD,
    col_renamer,
)

# -------------------------------------------------------------------------------------------
# General functions - READING DATA
#


def read_product_mapping_file(path_to_data: str | pathlib.Path):
    path = pathlib.Path(path_to_data).glob("*input_products_mapping*.csv")
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No product mapping file at {path_to_data}")
        return pd.DataFrame()

    product_mapping = pd.read_csv(path)

    return product_mapping


def read_field_name_mapping(path_to_data: str | pathlib.Path, grower: str):
    path = pathlib.Path(path_to_data).joinpath(grower).glob("*field_name_mapping*.csv")
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No field name mapping file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    field_mapping = pd.read_csv(path)

    return field_mapping


def read_verified_file(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int = 2022
):
    # TODO: function needs to be adapted to include growing_cycle
    #
    # Suggestion: changing file naming convention to
    # `{grower}_verified_acres_{growing_cycle}.csv`
    path = pathlib.Path(path_to_data).joinpath(grower).glob("*verified_acres.csv")
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No verified_acres file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    verified = pd.read_csv(path)
    verified = unify_cols(verified)

    verified = verified[verified.Verified.isin(["x", "X"])]

    return verified.reset_index(drop=True)


def read_cleaned_file(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int, da_name: str
):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob("*" + da_name + "_cleaned_" + str(growing_cycle) + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No cleaned {da_name} file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    cleaned = pd.read_csv(path, parse_dates=["Operation_start", "Operation_end"])

    return cleaned


def read_lime_report(path_to_data: str | pathlib.Path, grower: str):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_" + LIME_REPORT + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No lime report file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    lime = pd.read_csv(path)

    return lime


def read_manure_report(path_to_data: str | pathlib.Path, grower: str) -> pd.DataFrame:
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_" + MANURE_REPORT + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No manure report file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    manure = pd.read_csv(path)

    return manure


def read_cover_crop_report(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_" + CC_REPORT + "_" + str(growing_cycle) + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(
            f"No cover crop report file at {path_to_data} for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame()

    return pd.read_csv(path)


def read_split_field_report(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(
            grower
            + "_"
            + data_aggregator
            + "_"
            + SPLIT_FIELD
            + "_"
            + str(growing_cycle)
            + ".csv"
        )
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No split field report file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    sf = pd.read_csv(path)

    return sf


# MOVED TO `util/readers/generated_reports/`
#
# def read_shp_file_overview(path_to_data: str | pathlib.Path, grower: str):
#     path = (
#         pathlib.Path(path_to_data)
#         .joinpath(grower)
#         .glob(grower + "_shp_file_overview.csv")
#     )
#     path = next(path, NOT_FOUND)

#     if path == NOT_FOUND:
#         log.warning(f"No shp file overview file at {path_to_data} for grower {grower}")
#         return pd.DataFrame(columns=["Field_name", "Acreage_calc"])

#     shp = pd.read_csv(path)

#     return shp


REFERENCE_ACREAGE_REPORT = "reference_acreage_report"


def read_reference_acreage_report(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(
            grower
            + "_"
            + data_aggregator
            + "_"
            + REFERENCE_ACREAGE_REPORT
            + "_"
            + str(growing_cycle)
            + ".csv"
        )
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(
            f"No {REFERENCE_ACREAGE_REPORT} file at {path_to_data} for grower {grower} and cycle {growing_cycle} and system {data_aggregator}"
        )
        return pd.DataFrame()

    ref_acreage = pd.read_csv(path)

    return ref_acreage


def get_attributes_from_all_sources(col_name: str, data_sources: dict):
    """returns all unique entries in `col_name` from all `data_sources`.

    Params:
    col_name     - target column name to extract unique values from across all `data_sources`
    data_sources - dictionary; contains data from different sources as values, source names as keys
    """
    attributes = pd.Series(dtype="object")

    for name, source in data_sources.items():
        if col_name in source.columns:
            attributes = pd.concat([attributes, pd.Series(source[col_name].unique())])
        else:
            log.info(f"column {col_name} not in source {name}")

    return attributes.unique()


# -------------------------------------------------------------------------------------------
# General functions - DATA CLEANING
#


def clean_cols(name: str):
    return name.strip()


def clean_col_entry(entry):
    if not isinstance(entry, str):
        return entry
    temp = entry.strip()
    temp = temp.replace("’", "'")
    return temp.strip()


def replace_wspc(name):
    return name.replace("/", " ").replace(" ", "_").replace(".", "_")


def rename_cols(df):
    temp = df.rename(columns=col_renamer)
    return temp


def unify_cols(df):
    temp = df.rename(columns=clean_cols)
    temp = temp.rename(columns=str.capitalize)
    temp = rename_cols(temp)
    temp = temp.rename(columns=replace_wspc)
    if not temp.empty:
        temp.Field_name = temp.Field_name.apply(lambda entry: clean_col_entry(entry))

    return temp


def add_prefix(name, prefix):
    return prefix + "_" + name


def rename_cols_with_prefix(df, prefix):
    temp = df.rename(columns=str.lower)
    temp.columns = [add_prefix(c, prefix) for c in temp.columns]

    return temp


def map_clear_name(field_mapping, name):
    if not isinstance(name, str):
        return name
    temp = field_mapping[field_mapping.name == name.strip()]
    # check if record exists
    if temp.empty:
        return name

    clear_name = temp.iloc[[0]].clear_name.iloc[0]
    # check if record has mapping
    if not isinstance(clear_name, str):
        return name

    return clear_name


def map_clear_name_using_farm_name(field_mapping, name, farm_name):
    # check if farm_name is available
    if not isinstance(farm_name, str) and isinstance(name, str):
        return map_clear_name(field_mapping, name)

    if not isinstance(name, str):
        return name
    temp = field_mapping[
        (field_mapping.name == name.strip())
        & (field_mapping.farm_name == farm_name.strip())
    ]
    # check if record exists
    if temp.empty:
        return name

    clear_name = temp.iloc[[0]].clear_name.iloc[0]
    # check if record has mapping
    if not isinstance(clear_name, str):
        return name

    return clear_name


def map_fert_type(product_mapping, name):
    default = "OTHER"

    if not isinstance(name, str):
        return default

    temp = product_mapping[product_mapping.clear_name == name.strip()]
    # check if record exists
    if temp.empty:
        return default

    clear_name = temp.iloc[[0]].type.iloc[0]
    # check if record has mapping
    if not isinstance(clear_name, str):
        return default

    return clear_name


def convert_to_float(num):
    temp = num
    if isinstance(num, str):
        temp = num.replace(",", "")
    return float(temp)


def clean_entry(num):
    if num == "---" or num == "--":
        return np.nan
    if isinstance(num, str):
        if "%" in num:
            return num.replace("%", "").strip()
    return num


def clean_numeric_col(num):
    n = clean_entry(num)
    n = convert_to_float(n)
    return n


#
# GRANULAR
#
def parse_start_date(date):
    if len(date) > 12:
        temp = date.split(" - ")[0]
        temp = temp[:-5].strip()
        return datetime.strptime(temp, "%b %d, %Y %I:%M %p")
    else:
        return datetime.strptime(date, "%b %d, %Y")


def parse_end_date(date):
    if len(date) > 12:
        temp = date.split(" - ")[1]
        temp = temp[:-5].strip()
        return datetime.strptime(temp, "%b %d, %Y %I:%M %p")  # %Z")
    else:
        return datetime.strptime(date, "%b %d, %Y")


# dictionary of unit translations into our DB's enum
UNIT_ENUM_TRANS = {"LB": "LBS", "TON": "TN", "FL": "FL_OZ"}


def split_applied_val(total_applied):
    if len(total_applied) > 2:
        return float(total_applied.split(" ")[0].replace(",", ""))
    else:
        return total_applied


def split_applied_unit(total_applied):
    if len(total_applied) > 2:
        temp = total_applied.split(" ")[1].upper()
        trans = UNIT_ENUM_TRANS.get(temp, NOT_FOUND)

        if trans != NOT_FOUND:
            return trans
        else:
            return temp
    else:
        return total_applied
