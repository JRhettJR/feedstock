import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...util.cleaners.general import get_cleaned_file_by_file_type
from ...util.readers.general import get_file_type_by_data_aggregator
from ..constants import APPLICATION, NOT_FOUND
from ..helpers import add_op_relevance_by_harvest_dates

# %% [markdown]
# ---
# # Lime report


# %%
def extract_lime_info_from_apps(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=True
):
    default_cols = []

    # get file_type for const.APPLICATION data
    file_type = get_file_type_by_data_aggregator(
        data_aggregator, file_category=APPLICATION
    )

    if file_type == NOT_FOUND:
        log.warning(
            f"unable to generate lime report file for grower {grower} and system {data_aggregator} for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    apps_curr = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
    )
    apps_prev = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle - 1, data_aggregator, file_type, verbose
    )

    if apps_curr.empty and apps_prev.empty:
        return pd.DataFrame(columns=default_cols)

    apps = pd.concat([apps_prev, apps_curr])

    # map field names (for known fields)
    field_mapping = gen.read_field_name_mapping(path_to_data, grower)
    apps.Field_name = apps.apply(
        lambda x: gen.map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    apps = apps[apps.Product.isin(["Lime"])]

    apps = add_op_relevance_by_harvest_dates(
        apps, path_to_dest, grower, growing_cycle, data_aggregator
    )

    return apps
