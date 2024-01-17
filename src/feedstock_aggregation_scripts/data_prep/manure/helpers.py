import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...util.cleaners.general import get_cleaned_file_by_file_type
from ...util.readers.general import get_file_type_by_data_aggregator
from ..constants import APPLICATION, DA_GRANULAR, MANURE_REPORT, NOT_FOUND
from ..helpers import add_op_relevance_by_harvest_dates
from ..reference_acreage.helpers import (
    add_reference_acreage,
    calculate_coverage_relative_to_reference_acreage,
)


def determine_manure_op(task_name: str) -> None | int:
    if not isinstance(task_name, str):
        return np.nan
    if task_name in ["Dry Manure", "Liquid Manure", "Beef", "Spreading"]:
        return 1
    else:
        return np.nan


def extract_manure_info_from_apps(
    path_to_data: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    default_cols = []

    # get file_type for const.APPLICATION data
    file_type = get_file_type_by_data_aggregator(
        data_aggregator, file_category=APPLICATION
    )

    if file_type == NOT_FOUND:
        log.warning(
            f"unable to generate manure report file for grower {grower} and system {data_aggregator} for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    apps_prev = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle - 1, data_aggregator, file_type, verbose
    )
    apps_curr = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
    )

    if apps_prev.empty and apps_curr.empty:
        return pd.DataFrame(columns=default_cols)

    apps = pd.concat([apps_prev, apps_curr], ignore_index=True)

    field_mapping = gen.read_field_name_mapping(path_to_data, grower)
    apps.Field_name = apps.apply(
        lambda x: gen.map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    if data_aggregator == DA_GRANULAR:
        # derive from task name
        apps["Manure_op"] = apps.Task_name.apply(determine_manure_op)
    else:
        # derive from product name
        apps["Manure_op"] = apps.Product.apply(determine_manure_op)
    apps = apps[apps.Manure_op == 1]

    if not apps.empty:
        # clean duplicates independent of farm name (field names are clear names!)
        cols_temp = apps.columns.to_list()
        cols_temp.remove("Farm_name")
        apps = apps.drop_duplicates(subset=cols_temp, ignore_index=True)

        apps = add_op_relevance_by_harvest_dates(
            apps, path_to_dest, grower, growing_cycle, data_aggregator
        )
        # Attach metadata to data frame to identify it
        apps.file_type = MANURE_REPORT
        # adding reference acreage to data
        apps = add_reference_acreage(
            apps=apps,
            path_to_data=path_to_dest,
            grower=grower,
            growing_cycle=growing_cycle,
            data_aggregator=data_aggregator,
        )
        # calculate % coverage
        apps = calculate_coverage_relative_to_reference_acreage(apps)

    return apps
