import os
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger as log

from ...general import (
    map_clear_name,
    map_clear_name_using_farm_name,
    map_fert_type,
    read_field_name_mapping,
    read_product_mapping_file,
)
from ...util.cleaners.general import (
    clean_file_by_file_type,
    supplement_Granular_client_and_farm,
)
from ...util.readers.general import read_file_by_file_type
from ..constants import DA_GRANULAR, GRAN_APPLICATION
from .helpers import translate_crop_from_task

# %% [markdown]
# ## Granular
#
# Extracting planting from applications. The filtering for seeding applications depends on the availability of `Product_type` in the `chemical_input_product_mapping` table for all products that are applied during `Planting` operations.

# %% [markdown]
# ### Planting file


# %%
def create_Granular_planting_file(path_to_data, grower, growing_cycle):
    log.info(
        f"creating Granular planting file for grower {grower} and cycle {growing_cycle}"
    )
    product_mapping = read_product_mapping_file(path_to_data)

    data_aggregator = DA_GRANULAR
    file_type = GRAN_APPLICATION
    apps = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type
    )
    if apps.empty:
        log.warning(
            f"unable to create Granular planting file for grower {grower} and cycle {growing_cycle}; missing application file"
        )
        return pd.DataFrame()

    apps = clean_file_by_file_type(
        apps, path_to_data, grower, growing_cycle, file_type, data_aggregator
    )
    apps = supplement_Granular_client_and_farm(
        apps, path_to_data, grower, growing_cycle
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

    apps = apps[apps.Operation_type == "Planting"]
    apps = apps[apps.Product_type.isin(["OTHER", "SEED"])]
    apps["Crop_type"] = apps.Task_name.apply(translate_crop_from_task)

    apps = apps.reset_index(drop=True)

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
            index=False,
        )

    return apps


# %%
# _ = create_Granular_planting_file(path_to_data, grower, growing_cycle-1)
# _ = create_Granular_planting_file(path_to_data, grower, growing_cycle)

# %% [markdown]
# ### Tillage file


# %%
def create_Granular_tillage_file(path_to_data, grower, growing_cycle):
    log.info(
        f"creating Granular tillage file for grower {grower} and cycle {growing_cycle}"
    )
    read_product_mapping_file(path_to_data)

    till = read_file_by_file_type(
        path_to_data,
        grower,
        growing_cycle,
        data_aggregator=DA_GRANULAR,
        file_type=GRAN_APPLICATION,
    )
    if till.empty:
        log.warning(
            f"unable to create Granular tillage file for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame()

    till = clean_file_by_file_type(
        till,
        path_to_data,
        grower,
        growing_cycle,
        file_type=GRAN_APPLICATION,
        data_aggregator=DA_GRANULAR,
    )
    till = supplement_Granular_client_and_farm(
        till, path_to_data, grower, growing_cycle
    )

    field_mapping = read_field_name_mapping(path_to_data, grower)
    till.Field_name = till.apply(
        lambda x: map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    till = till[till.Task_name.isin(["Strip Till", "Strip- Till"])]
    till.Operation_type = "Tillage"
    # overwrite values to avoid double counting
    # return till
    for col in ["Product", "Applied_rate", "Applied_total", "Applied_unit"]:
        till[col] = np.nan

    till["Product_type"] = "OTHER"

    till = till.reset_index(drop=True)
    # return till
    path_to_data = Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_data):
        os.makedirs(path_to_data)

    data_aggregator = DA_GRANULAR

    if till.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_tillage_{growing_cycle}.csv"
        )
    else:
        till.to_csv(
            path_to_data.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_tillage_"
                + str(growing_cycle)
                + ".csv"
            ),
            index=False,
        )

    return till
