import os
import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...util.cleaners.general import get_cleaned_file_by_file_type
from ...util.readers.general import read_file_by_file_type
from .. import npk_breakdowns as npk
from ..cleaned_file.cleaned_file import create_clean_file
from ..constants import (
    FERTILIZER,
    FUNGICIDE,
    HERBICIDE,
    INSECTICIDE,
    MANURE_REPORT,
    OTHER,
)
from ..helpers import count_ops

# %% [markdown]
# ## Aggregation Helpers


def get_clean_data(path_to_data, path_to_dest, grower, growing_cycle, data_aggregator):
    clean_data = gen.read_cleaned_file(
        path_to_data=path_to_dest,
        grower=grower,
        growing_cycle=growing_cycle,
        da_name=data_aggregator,
    )

    if clean_data.empty:
        log.info(f"creating cleaned file for {grower}...")
        clean_data = create_clean_file(
            path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
        )

    clean_data = isolate_growing_cycle_relevant_ops(clean_data)
    # return clean_data
    return clean_data


def get_verified_data(path_to_data, grower):
    verified = gen.read_verified_file(path_to_data, grower)
    verified.Field_name.unique()

    if verified.empty:
        return pd.DataFrame()

    return verified


def identify_unmapped_verified_fields(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
):
    # In other words, taking the clean data, filter down to not-matched fields and return a list
    clean_data = get_clean_data(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
    )
    verified = get_verified_data(path_to_data, grower)

    # Filter to only unique names
    clean_fields = clean_data["Field_name"].unique()

    verified["present_in_clean"] = verified.Field_name.isin(clean_fields)
    fields_not_present = verified[verified.present_in_clean != True]

    if fields_not_present.empty:
        return fields_not_present

    # return fields_not_present
    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if fields_not_present.empty:
        log.warning(
            f"No data to save for {grower}_{data_aggregator}_fields_not_in_clean_{growing_cycle}.csv"
        )
    else:
        fields_not_present.to_csv(
            path_to_dest.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_fields_not_in_clean_"
                + str(growing_cycle)
                + ".csv"
            ),
            index=False,
        )
    return fields_not_present


# %% [markdown]
# ## Applications


# %%
def prepare_NPK(apps, path_to_data, grower):
    # rel_npk_cols = ['Farm_name', 'Field_name', 'Crop_type', 'TOTAL_N', 'TOTAL_P', 'TOTAL_K', 'RATE_N', 'RATE_P', 'RATE_K']
    rel_npk_cols = [
        "Farm_name",
        "Field_name",
        "TOTAL_N",
        "TOTAL_P",
        "TOTAL_K",
        "RATE_N",
        "RATE_P",
        "RATE_K",
    ]

    if apps.empty:
        return pd.DataFrame(columns=rel_npk_cols)

    # total_fert = apps.groupby(by=['Farm_name', 'Field_name', 'Product', 'Crop_type'], dropna=False, as_index=False).sum()
    total_fert = apps.groupby(
        by=["Farm_name", "Field_name", "Product"], dropna=False, as_index=False
    ).sum(numeric_only=True)
    npk_rate = npk.get_NPK_rate(total_fert, path_to_data, grower)

    # npk = npk.groupby(by=['Farm_name', 'Field_name', 'Crop_type'], dropna=False, as_index=False).sum()
    npk_rate = npk_rate.groupby(
        by=["Farm_name", "Field_name"], dropna=False, as_index=False
    ).sum(numeric_only=True)

    if npk_rate.empty:
        return pd.DataFrame(columns=rel_npk_cols)

    npk_rate = npk_rate[rel_npk_cols]

    return npk_rate


def get_apps_params(clean_data):
    apps = clean_data[~clean_data.Operation_type.isin(["Harvest", "Tillage"])]

    stub = apps.drop_duplicates(subset=["Farm_name", "Field_name"], ignore_index=True)[
        ["Farm_name", "Field_name"]
    ]

    for product_type in [FERTILIZER, HERBICIDE, FUNGICIDE, INSECTICIDE, OTHER]:
        temp = apps[apps.Product_type == product_type]

        num_ops = count_ops(temp, "Num_ops_" + product_type.lower())

        if num_ops.empty:
            # artificially add empty column if no data exists
            stub["Num_ops_" + product_type.lower()] = 0
        else:
            stub = pd.merge(stub, num_ops, on=["Farm_name", "Field_name"], how="left")
            # dummy df to fill NaN values of specific column
            filler = pd.DataFrame(
                np.zeros((stub.shape[0], 1)),
                columns=["Num_ops_" + product_type.lower()],
            )
            stub = stub.fillna(filler)

    return stub


# %%
# clean_data = read_cleaned_file(path_to_dest, grower, growing_cycle, data_aggregator)
# get_apps_params(clean_data)

# %% [markdown]
# ## Tillage


# %%
def calc_tillage_params(till):
    rel_cols = [
        "Farm_name",
        "Field_name",
        "Total_area_tilled",
        "Applied_unit_till",
        "Till_rate",
    ]

    if till.empty:
        return pd.DataFrame(columns=rel_cols)

    temp = till.groupby(by=["Farm_name", "Field_name"], as_index=False).sum(
        numeric_only=True
    )

    temp = pd.merge(
        temp[["Farm_name", "Field_name", "Area_applied", "Applied_rate"]],
        till[["Farm_name", "Field_name", "Applied_unit"]].drop_duplicates(
            subset=["Farm_name", "Field_name"]
        ),
        on=["Farm_name", "Field_name"],
        how="left",
    )

    temp = temp.rename(columns={"Area_applied": "Total_area_tilled"})
    temp = temp.rename(columns={"Applied_unit": "Applied_unit_till"})
    temp = temp.rename(columns={"Applied_rate": "Till_rate"})

    return temp


def get_tillage_params(clean_data):
    till = clean_data[clean_data.Operation_type == "Tillage"]

    num_ops = count_ops(till, "Num_ops_tillage")
    till_params = calc_tillage_params(till)

    return pd.merge(till_params, num_ops, on=["Farm_name", "Field_name"])


# %%
# clean_data = read_cleaned_file(path_to_dest, grower, growing_cycle, data_aggregator)
# get_tillage_params(clean_data)

# %% [markdown]
# ## Fuel


# %%
def calc_fuel_params(fuel):
    rel_cols = ["Farm_name", "Field_name", "Total_fuel"]

    if fuel.empty:
        return pd.DataFrame(columns=rel_cols)

    temp = fuel.groupby(
        by=["Farm_name", "Field_name"], dropna=False, as_index=False
    ).sum(numeric_only=True)

    temp = pd.merge(
        fuel[["Farm_name", "Field_name"]].drop_duplicates(
            subset=["Farm_name", "Field_name"]
        ),
        temp[["Farm_name", "Field_name", "Total_fuel"]],
        on=["Farm_name", "Field_name"],
        how="left",
    )

    # temp = temp.rename(columns={'Area_applied': 'Total_area_tilled'})
    # temp = temp.rename(columns={'Applied_unit': 'Applied_unit_till'})
    # temp = temp.rename(columns={'Applied_rate': 'Till_rate'})

    return temp


def get_fuel_params(path_to_data, grower, growing_cycle, data_aggregator, file_type):
    default_cols = [
        "Farm_name",
        "Field_name",
        "Total_fuel",
        "Fuel_unit",
        "Num_fuel_records_total",
        "Num_ops_with_fuel",
    ]

    fuel = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type
    )
    # return fuel
    if fuel.empty:
        return pd.DataFrame(columns=default_cols)
    # return fuel
    num_ops = count_ops(fuel, "Num_fuel_records_total")
    num_ops_with_fuel = count_ops(fuel.dropna(subset="Fuel_unit"), "Num_ops_with_fuel")
    # num_ops = count_ops(fuel, 'Num_ops_fuel')
    fuel_params = calc_fuel_params(fuel)
    # return fuel_params

    fuel = pd.merge(fuel_params, num_ops, on=["Farm_name", "Field_name"], how="outer")
    fuel = pd.merge(
        fuel, num_ops_with_fuel, on=["Farm_name", "Field_name"], how="outer"
    )

    field_mapping = gen.read_field_name_mapping(path_to_data, grower)
    fuel.Field_name = fuel.apply(
        lambda x: gen.map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    return fuel


# %%
# get_fuel_params(path_to_data, grower, growing_cycle, data_aggregator, file_type=LDB_FUEL)

# %% [markdown]
# ## Manure


# %%
def calc_manure_params(manure):
    rel_cols = ["Farm_name", "Field_name", "Total_manure", "Total_area_manured"]

    if manure.empty:
        return pd.DataFrame(columns=rel_cols)

    temp = manure.groupby(
        by=["Farm_name", "Field_name", "Applied_unit"], dropna=False, as_index=False
    ).sum(numeric_only=True)
    temp = temp.rename(
        columns={"Applied_total": "Total_manure", "Area_applied": "Total_area_manured"}
    )

    temp = pd.merge(
        manure[["Farm_name", "Field_name", "Applied_unit"]].drop_duplicates(
            subset=["Farm_name", "Field_name", "Applied_unit"]
        ),
        temp[["Farm_name", "Field_name", "Total_manure", "Total_area_manured"]],
        on=["Farm_name", "Field_name"],
        how="left",
    )

    # temp = temp.rename(columns={'Area_applied': 'Total_area_tilled'})
    temp = temp.rename(columns={"Applied_unit": "Applied_unit_manure"})
    # temp = temp.rename(columns={'Applied_rate': 'Till_rate'})

    return temp


def get_manure_params(path_to_data, grower, growing_cycle, data_aggregator):
    default_cols = [
        "Farm_name",
        "Field_name",
        "Total_manure",
        "Total_area_manured",
        "Num_manure_ops",
    ]

    manure = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type=MANURE_REPORT
    )
    manure = manure[(manure.Growing_cycle == growing_cycle)]

    if manure.empty:
        return pd.DataFrame(columns=default_cols)
    # return fuel
    num_ops = count_ops(manure, "Num_manure_ops")

    manure_params = calc_manure_params(manure)
    # return manure_params

    manure = pd.merge(
        manure_params, num_ops, on=["Farm_name", "Field_name"], how="outer"
    )
    # fuel = pd.merge(fuel, num_ops_with_fuel, on=['Farm_name', 'Field_name'], how='outer')

    return manure


# %% [markdown]
# ## Helpers


# %%
def isolate_growing_cycle_relevant_ops(clean_data):
    temp = clean_data[~clean_data.Op_relevance.isin(["irrelevant", "missing_op_date"])]
    return temp


def get_all_farm_field_crop_type_combinations(verified, data_array):
    base_cols = ["Farm_name", "Field_name"]
    result = pd.DataFrame(columns=[*base_cols, "Crop_type"])
    for df in data_array:
        if not df.empty:
            if "Crop_type" in df.columns:
                result = pd.concat([result, df[[*base_cols, "Crop_type"]]])
            else:
                result = pd.concat([result, df[base_cols]])

    result = pd.concat([result, verified[["Field_name"]]])
    # ensure column Crop_type exists
    # result = add_missing_columns(result, ['Crop_type'])
    return result.drop_duplicates(subset=["Field_name", "Crop_type"], ignore_index=True)
