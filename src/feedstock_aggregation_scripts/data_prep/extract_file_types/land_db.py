import os
from pathlib import Path

from loguru import logger as log
from pandas import DataFrame

from ...general import (
    clean_col_entry,
    map_clear_name,
    map_clear_name_using_farm_name,
    map_fert_type,
    read_field_name_mapping,
    read_product_mapping_file,
)
from ...util.cleaners.general import clean_file_by_file_type
from ...util.cleaners.helpers import seeding_planting_params
from ...util.readers.general import read_file_by_file_type
from ..constants import DA_LDB, LDB_APPLICATION
from .helpers import mark_fuel_ops

# %% [markdown]
# ## LDB

# %% [markdown]
# ### Planting file


# %%
def create_LDB_planting_file(path_to_data, grower, growing_cycle):
    log.info(
        f"creating Land.db planting file for grower {grower} and cycle {growing_cycle}"
    )
    product_mapping = read_product_mapping_file(path_to_data)

    data_aggregator = DA_LDB
    apps = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type=LDB_APPLICATION
    )
    if apps.empty:
        log.warning(
            f"unable to create Land.db planting file for grower {grower} and cycle {growing_cycle}; missing application file"
        )
        return DataFrame()

    apps = clean_file_by_file_type(
        apps,
        path_to_data,
        grower,
        growing_cycle,
        file_type=LDB_APPLICATION,
        data_aggregator=DA_LDB,
    )
    # return apps
    # apps = supplement_Granular_client_and_farm(apps, path_to_data, grower, growing_cycle)

    apps.Product = apps.apply(
        lambda x: map_clear_name(product_mapping, x.Product), axis=1
    )
    apps["Product_type"] = apps.apply(
        lambda x: map_fert_type(product_mapping, x.Product), axis=1
    )

    field_mapping = read_field_name_mapping(path_to_data, grower)
    apps.Field_name = apps.apply(
        lambda x: map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    apps = apps[
        (apps.Product.isin(["Planting"]))
        | (apps.Applied_unit.isin(seeding_planting_params))
    ]

    apps.Operation_type = "Planting"
    # return apps
    # apps = apps[apps.Product_type.isin(['OTHER', 'SEED'])]
    # apps['Crop_type'] = apps.Task_name.apply(translate_crop_from_task)

    apps = apps.reset_index(drop=True)
    apps = apps.drop_duplicates(keep="first")

    path_to_data = Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_data):
        os.makedirs(path_to_data)

    if apps.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_planting_{growing_cycle}.csv"
        )
    else:
        apps.to_csv(
            path_to_data.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_planting_"
                + str(growing_cycle)
                + ".csv"
            ),
            # float_format = "%.10f",
            index=False,
        )

    return apps


# %%
# create_LDB_planting_file(path_to_data, grower, growing_cycle)

# %% [markdown]
# ### Fuel file


# %%
def create_LDB_fuel_file(path_to_data, grower, growing_cycle):
    log.info(
        f"creating Land.db fuel file for grower {grower} and cycle {growing_cycle}"
    )
    product_mapping = read_product_mapping_file(path_to_data)

    data_aggregator = DA_LDB
    apps = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type=LDB_APPLICATION
    )
    if apps.empty:
        log.warning(
            f"unable to create Land.db fuel file for grower {grower} and cycle {growing_cycle}; missing application file"
        )
        return DataFrame()

    apps = clean_file_by_file_type(
        apps,
        path_to_data,
        grower,
        growing_cycle,
        file_type=LDB_APPLICATION,
        data_aggregator=DA_LDB,
    )

    apps.Product = apps.apply(
        lambda x: map_clear_name(product_mapping, x.Product), axis=1
    )
    apps["Product_type"] = apps.apply(
        lambda x: map_fert_type(product_mapping, x.Product), axis=1
    )

    field_mapping = read_field_name_mapping(path_to_data, grower)
    apps.Field_name = apps.apply(
        lambda x: map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    apps["Fuel_op"] = apps["Product"].apply(mark_fuel_ops)
    fuel = apps[(apps.Fuel_op == 1)]

    fuel = fuel.rename(
        columns={"Applied_total": "Total_fuel", "Applied_unit": "Fuel_unit"}
    )
    # fuel['Total_fuel'] = fuel.Applied_total
    # fuel['Fuel_unit'] = fuel.Applied_unit

    fuel = fuel.reset_index(drop=True)
    # return apps
    path_to_data = Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_data):
        os.makedirs(path_to_data)

    if fuel.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_fuel_{growing_cycle}.csv"
        )
    else:
        fuel.to_csv(
            path_to_data.joinpath(
                grower + "_" + data_aggregator + "_fuel_" + str(growing_cycle) + ".csv"
            ),
            index=False,
        )

    return fuel


def create_LDB_tillage_file(path_to_data, grower, growing_cycle):
    log.info(
        f"creating Land.db tillage file for grower {grower} and cycle {growing_cycle}"
    )
    product_mapping = read_product_mapping_file(path_to_data)

    data_aggregator = DA_LDB
    apps = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type=LDB_APPLICATION
    )
    if apps.empty:
        log.warning(
            f"unable to create Land.db planting file for grower {grower} and cycle {growing_cycle}; missing application file"
        )
        return DataFrame()

    apps = clean_file_by_file_type(
        apps,
        path_to_data,
        grower,
        growing_cycle,
        file_type=LDB_APPLICATION,
        data_aggregator=DA_LDB,
    )
    # return apps
    # apps = supplement_Granular_client_and_farm(apps, path_to_data, grower, growing_cycle)

    apps.Product = apps.Product.apply(clean_col_entry)
    apps.Product = apps.apply(
        lambda x: map_clear_name(product_mapping, x.Product), axis=1
    )
    apps["Product_type"] = apps.apply(
        lambda x: map_fert_type(product_mapping, x.Product), axis=1
    )

    field_mapping = read_field_name_mapping(path_to_data, grower)
    apps.Field_name = apps.apply(
        lambda x: map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    apps = apps[apps.Product.isin(["Field Cult w/ Harrow", "Tillage"])]
    # return apps
    apps.Operation_type = "Tillage"

    apps = apps.reset_index(drop=True)

    path_to_data = Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_data):
        os.makedirs(path_to_data)

    if apps.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_tillage_{growing_cycle}.csv"
        )
    else:
        apps.to_csv(
            path_to_data.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_tillage_"
                + str(growing_cycle)
                + ".csv"
            ),
            # float_format = "%.10f",
            index=False,
        )

    return apps
