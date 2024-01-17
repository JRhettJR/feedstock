import numpy as np
import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...util.cleaners.general import get_cleaned_file_by_file_type
from ...util.readers.general import get_file_type_by_data_aggregator
from ..constants import HARVEST, NOT_FOUND

# %% [markdown]
# ---
# # Harvest dates


# %% [markdown]
# ## Harvest date file

# %%


def get_year_from_timestamp(timestamp):
    return timestamp.year


def get_harvest_dates(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=True
):
    str(growing_cycle - 1)
    str(growing_cycle)
    default_cols = [
        "Client",
        "Farm_name",
        "Field_name",
        "Crop_type",
        "Sub_crop_type",
        "Harvest_date",
        "Year",
    ]

    # get file_type for HARVEST data
    file_type = get_file_type_by_data_aggregator(data_aggregator, file_category=HARVEST)

    if file_type == NOT_FOUND:
        log.warning(
            f"unable to generate harvest date file for grower {grower} and system {data_aggregator} for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    # get data sets for previous and current year
    df_prev = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle - 1, data_aggregator, file_type, verbose
    )
    df_curr = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
    )

    # check if harvest data available
    if df_prev.empty and df_curr.empty:
        return pd.DataFrame(columns=default_cols)

    # `Operation_start` referrs to `Last_harvested` in this case
    rel_cols = [
        "Client",
        "Farm_name",
        "Field_name",
        "Crop_type",
        "Sub_crop_type",
        "Operation_start",
    ]

    temp = pd.concat([df_prev, df_curr])

    for col in rel_cols:
        if col not in temp.columns:
            temp[col] = np.nan

    temp = temp.drop_duplicates(subset=rel_cols, ignore_index=True)
    temp = temp[rel_cols]

    temp["Year"] = temp.Operation_start.apply(get_year_from_timestamp)

    harvest_dates = temp.rename(columns={"Operation_start": "Harvest_date"})

    # map field names (for known fields)
    field_mapping = gen.read_field_name_mapping(path_to_data, grower)
    harvest_dates.Field_name = harvest_dates.apply(
        lambda x: gen.map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    return harvest_dates
