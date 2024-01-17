import os
import pathlib

import numpy as np
import pandas as pd
from fuzzywuzzy import process
from loguru import logger as log

from ..general import read_field_name_mapping
from ..util.cleaners.general import clean_file_by_file_type
from ..util.cleaners.helpers import add_missing_columns
from ..util.readers.comprehensive import create_comprehensive_df
from ..util.readers.general import get_file_type_by_data_aggregator
from .agg_report.agg_report import aggregate_app_data
from .constants import APPLICATION, FERTILIZER, HARVEST_DATES, LIME_REPORT, NOT_FOUND
from .extract_file_types.land_db import read_file_by_file_type
from .harvest_dates.harvest_dates import get_harvest_dates
from .helpers import get_seeding_area
from .lime.lime import extract_lime_info_from_apps
from .split_field.split_field import prepare_split_field_data

# %%
# Use for testing when additional DA onboarded
# gen.unify_cols(a)

# %% [markdown]
# ---
# # Cleaning

# %% [markdown]
# ## Helpers

# %% [markdown]
# ### General


def compare_rows(row1, row2, columns):
    for col in columns:
        log.info(col)
        log.info(row1[col].iloc[0], row2[col].iloc[0])
        log.info(type(row1[col]), type(row2[col]))
        log.info(row1[col].iloc[0] == row2[col].iloc[0])


# def file_exists(path_to_data, grower, file_name):
#     file = pathlib.Path(path_to_data).joinpath(grower).glob(file_name)
#     return next(file, NOT_FOUND)


# %% [markdown]
# ## Cleaning functions

# %% [markdown]
# ### Accepted Units by BE
#
# For **applications**:
# - "LITER"
# - "FL_OZ" -> liquid ounces
# - "PINT"
# - "GAL"
# - "LBS"
# - "FT3"
# - "KWH"
# - "QT" -> quarters
# - "TN" -> tons
# - "G" -> grams
# - "T" -> metric tons
# - "BAG" -> seeds
#
# For **fuel**:
# - "LITER"
# - "GAL"
# - "FT3"
# - "KWH"

# %% [markdown]
# ### JDOps


# %%


# %% [markdown]
# ### General


# %%
# Test whether cleaning function is doing what its supposed to do
# get_cleaned_file_by_file_type(path_to_data, grower='Bornitz', growing_cycle=2022, data_aggregator=DA_LDB, file_type=LDB_APPLICATION, verbose=False)
# %% [markdown]
# ---
# # Generating files from existing data


# %%
# _ = create_Granular_tillage_file(path_to_data, grower, growing_cycle-1)
# _ = create_Granular_tillage_file(path_to_data, grower, growing_cycle)


# %%
# create_LDB_fuel_file(path_to_data, grower, growing_cycle)

# %% [markdown]
# ### Tillage file


# %%


# %%
# create_LDB_tillage_file(path_to_data, grower, growing_cycle)

# %% [markdown]
# ## SMS Ag Leader

# %% [markdown]
# ### Fuel

# %%
# TO-DO

# %% [markdown]
# ---
# # Add new info to existing files


# %%
def file_exists(path_to_data, grower, file_name):
    file = pathlib.Path(path_to_data).joinpath(grower).glob(file_name)
    return next(file, NOT_FOUND)


def add_to_existing_file(
    file,
    path_to_data,
    grower,
    growing_cycle,
    data_aggregator,
    file_type,
    sort_cols,
    file_name,
):
    existing_file = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type
    )

    records = pd.concat([file, existing_file], ignore_index=True).drop_duplicates()
    if records.empty:
        return existing_file

    if sort_cols != []:
        records = records.sort_values(by=sort_cols, ascending=True, ignore_index=True)

    path_to_dest = pathlib.Path(path_to_data).joinpath(grower)

    if records.empty:
        log.warning(f"no data to save for {grower} in {file_name}")
    else:
        records.to_csv(path_to_dest.joinpath(file_name), index=False)

    return existing_file


def add_new_fields_to_mapping(path_to_data, grower, field_list):
    log.info(
        f"adding additional farm-field combinations to field_name_mapping at {path_to_data} for grower {grower}"
    )

    # rename columns to fit field_name_mapping schema
    if "Client" in field_list:
        field_list = field_list.drop(columns="Client")
    field_list = field_list.rename(
        columns={"Field_name": "name", "Area_applied": "system_acres"}
    )
    field_list.columns = field_list.columns.map(str.lower)

    empty_mapping = pd.DataFrame(
        columns=[
            "system",
            "farm_name",
            "name",
            "system_acres",
            "clear_farm_name",
            "clear_name",
            "clear_acres",
        ]
    )

    field_mapping = read_field_name_mapping(path_to_data, grower)

    if field_mapping.empty:
        # if no mapping file is present, create a new file with field_list as a starting point
        temp = pd.concat([empty_mapping, field_list])
    else:
        # else concatenate new fields and drop duplicates
        temp = pd.concat([field_mapping, field_list])
        # will keep first occurences; that way we conserve existing entries in the field_mapping file
        # and drop the new once (in case they duplicate existing farm-field combinations).
        temp = temp.drop_duplicates(
            subset=["farm_name", "name"], ignore_index=True, keep="first"
        )
        temp = temp.dropna(subset="system")
        temp = temp.sort_values(by=["name"]).reset_index(drop=True)

    # save new/extended file to destination
    path_to_dest = pathlib.Path(path_to_data).joinpath(grower)

    if temp.empty:
        log.warning(f"no data to save for {grower}_field_name_mapping.csv")
    else:
        temp.to_csv(
            path_to_dest.joinpath(grower + "_field_name_mapping.csv"), index=False
        )


# %% [markdown]
# ---
# # Field list

# %% [markdown]
# ## Helpers


# %%
def extract_field_list(path_to_data, grower, growing_cycle, data_aggregator):
    temp = create_comprehensive_df(path_to_data, grower, growing_cycle, data_aggregator)

    area = get_seeding_area(path_to_data, grower, growing_cycle, data_aggregator)

    if temp.empty:
        return pd.DataFrame()

    temp = temp.drop_duplicates(subset=["Farm_name", "Field_name"], ignore_index=True)
    temp = temp[["Client", "Farm_name", "Field_name"]]

    temp = pd.merge(temp, area, on=["Farm_name", "Field_name"], how="left")

    temp["System"] = data_aggregator

    return temp


# %% [markdown]
# ## Report generation


# %%
def create_field_list(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
):
    temp = extract_field_list(path_to_data, grower, growing_cycle, data_aggregator)

    if temp.empty:
        log.warning(f"no field list available for grower {grower}")
        return pd.DataFrame()

    add_new_fields_to_mapping(path_to_data, grower, field_list=temp)
    # return temp
    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if temp.empty:
        log.warning(f"no data to save for {grower}_{data_aggregator}_field_list.csv")
    else:
        temp.to_csv(
            path_to_dest.joinpath(grower + "_" + data_aggregator + "_field_list.csv"),
            index=False,
        )

    return temp


# %%
# create_field_list(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)

# %% [markdown]
# ---
# # Product list


# %%
def extract_product_list(path_to_data, grower, growing_cycle, data_aggregator):
    # get file_type for const.APPLICATION data
    file_type = get_file_type_by_data_aggregator(
        data_aggregator, file_category=APPLICATION
    )
    default_cols = [
        "Product",
        "Applied_unit",
        "Manufacturer",
        "Reg_number",
        "Data_source",
    ]

    if file_type == NOT_FOUND:
        log.warning(
            f"unable to generate product list for grower {grower} and system {data_aggregator} "
            f"for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    apps_prev = read_file_by_file_type(
        path_to_data, grower, growing_cycle - 1, data_aggregator, file_type
    )
    apps_curr = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type
    )

    if apps_prev.empty and apps_curr.empty:
        log.warning(
            f"no application data to extract products from for system {data_aggregator} "
            f"for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    apps = pd.concat([apps_prev, apps_curr])

    apps = clean_file_by_file_type(
        apps, path_to_data, grower, growing_cycle, file_type, data_aggregator
    )
    apps = add_missing_columns(apps, default_cols)
    # isolate unique products
    apps = apps.drop_duplicates(
        subset=["Product", "Manufacturer", "Reg_number"], ignore_index=True
    )
    apps = apps.dropna(subset=["Product"]).reset_index()

    apps = apps.sort_values(by="Product", ascending=True, ignore_index=True)
    apps["Data_source"] = data_aggregator

    # Save entire list of products for identification of EEF products
    path_to_dest = pathlib.Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if apps.empty:
        log.warning(f"no data to save for {grower}_{data_aggregator}_product_list.csv")
    else:
        apps[default_cols].to_csv(
            path_to_dest.joinpath(grower + "_" + data_aggregator + "_product_list.csv"),
            index=False,
        )

    return apps[default_cols]


def extract_fertilizer_products(path_to_data):
    product_mapping = pd.read_csv(
        pathlib.Path(path_to_data).joinpath("chemical_input_products_mapping_table.csv")
    )
    product_mapping = product_mapping[product_mapping.type == FERTILIZER]

    unique_names = product_mapping["name"].unique()
    unique_clear_names = product_mapping["clear_name"].unique()
    fert_prods = np.concatenate([unique_names, unique_clear_names], axis=0)
    return fert_prods


def create_unmatched_product_list(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
):
    prod = extract_product_list(path_to_data, grower, growing_cycle, data_aggregator)

    if prod.empty:
        return prod

    product_mapping = pd.read_csv(
        pathlib.Path(path_to_data).joinpath("chemical_input_products_mapping_table.csv")
    )

    # Generate list of unique product names, by both clear name and name
    unique_names = product_mapping["name"].unique()
    unique_clear_names = product_mapping["clear_name"].unique()
    unique_product = np.concatenate([unique_names, unique_clear_names], axis=0)

    # Subset product list to only those not matched in mapping file
    prod["grower"] = prod.Product.isin(unique_product)
    prod = prod[prod["grower"] != True]  # remove '---' in Product
    prod["grower"] = grower  # add grower name for building larger list
    prod = prod[
        ~prod.Product.str.contains("---", na=False)
    ]  # Filter to only unmatched products

    return prod


def match_lists(source_df, list1, list2, ratio):
    # Helper to fuzzy match function below
    matches = []
    probabilities = []

    for i in list1:
        ratios = process.extractOne(i, list2)

        if int(ratios[1]) > ratio:  # can play with this number
            matches.append(ratios[0])
            probabilities.append("Fertilizer")
        else:
            matches.append("no match")
            probabilities.append("NOT fertilizer")

    df = source_df
    df["potential product match"] = matches
    df["likely product type"] = probabilities

    return df


def add_fuzzy_product_match(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, ratio
):
    # Adds a likely product type to unidentified products. 'Ratio' indicates the acceptable
    # partial match rate; suggest 85 or higher (0-100 possible)

    prod = create_unmatched_product_list(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
    )
    fert_prods = extract_fertilizer_products(path_to_data)

    matched_prods = match_lists(prod, prod["Product"], fert_prods, ratio)
    if matched_prods.empty:
        log.info(
            f"no unidentified products for grower {grower} in system {data_aggregator} for cycle {growing_cycle}"
        )

    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if matched_prods.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_unidentified_product_list.csv"
        )
    else:
        matched_prods.to_csv(
            path_to_dest.joinpath(
                grower + "_" + data_aggregator + "_unidentified_product_list.csv"
            ),
            index=False,
        )

    return matched_prods


# %%
# add_fuzzy_product_match(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, partial_match_ratio)

# %% [markdown]
# # Reference acreage

# %% [markdown]
# ## Helpers


# %%
# h = get_harvestes_area(path_to_data, grower, growing_cycle, data_aggregator=DA_GRANULAR)
# h[h.Field_name == 'Boelman 147']
# h[(h.Sub_crop_type.isin(['Grain', np.nan])) & (h.Crop_type.isin([*FDCIC_CROPS]))]


# %%
# create_reference_acreage_report(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=False)


def create_harvest_date_file(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=True
):
    harvest_dates = get_harvest_dates(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose
    )

    if harvest_dates.empty:
        return harvest_dates

    file_name = grower + "_" + data_aggregator + "_harvest_dates.csv"

    if file_exists(path_to_data=path_to_dest, grower=grower, file_name=file_name):
        log.info(f"writing to existing file {file_name} at {path_to_dest}...")
        add_to_existing_file(
            file=harvest_dates,
            path_to_data=path_to_dest,
            grower=grower,
            growing_cycle=growing_cycle,
            data_aggregator=data_aggregator,
            file_type=HARVEST_DATES,
            sort_cols=["Year", "Farm_name", "Field_name"],
            file_name=file_name,
        )
    else:
        path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
        log.info(f"writing new file {file_name} at {path_to_dest}...")
        if not os.path.exists(path_to_dest):
            os.makedirs(path_to_dest)

        if harvest_dates.empty:
            log.warning(f"no data to save for {file_name}")
        else:
            harvest_dates.to_csv(path_to_dest.joinpath(file_name), index=False)

    return harvest_dates


# %%
# harvest = get_cleaned_file_by_file_type(path_to_data, 'Olson', growing_cycle, data_aggregator, file_type=CFV_HARVEST)
# harvest.Operation_start = pd.to_datetime(harvest.Operation_start)
# harvest

# %%
# create_harvest_date_file(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)


def create_split_field_report(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=True
):
    """classifies dominant crop types based on seeded area relative to total planted area. If a crop_type's
    seeded area is greater than or euqal to 99.0% of total planted area, it is considered to be dominant.
    """
    temp = prepare_split_field_data(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose
    )

    if temp.empty:
        log.warning(
            f"no split field data available for grower {grower} in system {data_aggregator} "
            "for growing cycle {growing_cycle}"
        )
        return temp

    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if temp.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_split_field_report_{growing_cycle}.csv"
        )
    else:
        temp.to_csv(
            path_to_dest.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_split_field_report_"
                + str(growing_cycle)
                + ".csv"
            ),
            index=False,
        )

    return temp


# %%
# prepare_split_field_data(path_to_data, grower, growing_cycle, data_aggregator).sort_values(by='Field_name')

# %%
# create_split_field_report(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)


# %%
# create_cc_report(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)


# %%
# create_manure_report(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)


def create_lime_report(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=True
):
    rel_cols = [
        "Client",
        "Farm_name",
        "Field_name",
        "Task_name",
        "Product",
        "Operation_start",
        "Operation_end",
        "Area_applied",
        "Applied_rate",
        "Applied_total",
        "Applied_unit",
        "Operation_type",
        "Harvest_date_prev",
        "Harvest_date_curr",
        "Op_relevance",
        "Growing_cycle",
    ]

    apps = extract_lime_info_from_apps(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose
    )
    # return apps
    if apps.empty:
        log.warning(
            f"no lime data available for grower {grower} in system {data_aggregator} for growing cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=rel_cols)
    # add growing cycle to data set as reference
    apps["Growing_cycle"] = growing_cycle
    # return apps
    file_name = grower + "_" + data_aggregator + "_lime_report.csv"

    if file_exists(path_to_data=path_to_dest, grower=grower, file_name=file_name):
        log.info(f"writing to existing file {file_name} at {path_to_dest}...")
        add_to_existing_file(
            file=apps,
            path_to_data=path_to_dest,
            grower=grower,
            growing_cycle=growing_cycle,
            data_aggregator=data_aggregator,
            file_type=LIME_REPORT,
            sort_cols=["Growing_cycle"],
            file_name=file_name,
        )
    else:
        path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
        log.info(f"writing new file {file_name} at {path_to_dest}...")
        if not os.path.exists(path_to_dest):
            os.makedirs(path_to_dest)

        if apps.empty:
            log.warning(f"no data to save for {file_name}")
        else:
            apps.to_csv(path_to_dest.joinpath(file_name), index=False)

    return apps


# %%
# create_lime_report(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)
# create_lime_report(path_to_data, path_to_dest, grower, growing_cycle-3, data_aggregator)
# create_lime_report(path_to_data, path_to_dest, grower, growing_cycle-2, data_aggregator)
# create_lime_report(path_to_data, path_to_dest, grower, growing_cycle-1, data_aggregator)


# %%
# create_clean_file(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)

# %% [markdown]
# ---
# # Aggregated Report


# %% [markdown]
# ## Aggregation


def create_agg_report(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
):
    agg_report = aggregate_app_data(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
    )
    if agg_report.empty:
        return agg_report
    # return agg_report
    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if agg_report.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_agg_report_{growing_cycle}.csv"
        )
    else:
        agg_report.to_csv(
            path_to_dest.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_agg_report_"
                + str(growing_cycle)
                + ".csv"
            ),
            index=False,
        )

    return agg_report


# %%
# create_agg_report(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator)

# %%
