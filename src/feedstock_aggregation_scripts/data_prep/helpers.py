import os
import pathlib
from datetime import datetime

import numpy as np
import pandas as pd
from loguru import logger as log

from .. import general as gen
from ..util.cleaners.general import clean_file_by_file_type
from ..util.cleaners.helpers import add_missing_columns
from ..util.readers.comprehensive import create_comprehensive_df
from ..util.readers.general import (
    get_file_type_by_data_aggregator,
    read_file_by_file_type,
)
from .constants import (
    DA_LDB,
    DATA_AGGREGATORS,
    HARVEST,
    HARVEST_DATES,
    NOT_FOUND,
    PLANTING,
)


def get_all_available_field_names(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    verbose: bool = True,
) -> pd.Series:
    fields = pd.Series(dtype=str)

    field_mapping = gen.read_field_name_mapping(path_to_data, grower)

    # SUGGESTION:
    # instead of trying to read files from all potential data sources,
    # create overview of data sources by grower and read only files for
    # data sources the grower actually uses.

    for data_aggregator in DATA_AGGREGATORS:
        temp = create_comprehensive_df(
            path_to_data, grower, growing_cycle, data_aggregator, verbose
        )

        if temp.empty:
            continue
        # map field names to clear names for comparison across data sources
        if not field_mapping.empty:
            temp.Field_name = temp.apply(
                lambda x: gen.map_clear_name_using_farm_name(
                    field_mapping, x.Field_name, x.Farm_name
                ),
                axis=1,
            )

        fields = pd.concat([fields, temp.Field_name])

    return fields.unique()


def count_ops(df, count_col_name):
    if df.empty:
        return pd.DataFrame(columns=["Farm_name", "Field_name", count_col_name])
    temp = df.groupby(
        by=["Farm_name", "Field_name", "Operation_start"], dropna=False, as_index=False
    ).count()
    temp = temp.groupby(
        by=["Farm_name", "Field_name"], dropna=False, as_index=False
    ).sum(numeric_only=True)
    temp = temp.rename(columns={"Client": count_col_name})

    return temp[["Farm_name", "Field_name", count_col_name]]


def get_seeding_area(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
) -> pd.DataFrame:
    default_cols = [
        "Farm_name",
        "Field_name",
        "Crop_type",
        # "Sub_crop_type",
        "Area_applied",
    ]

    file_type = get_file_type_by_data_aggregator(
        data_aggregator, file_category=PLANTING
    )

    if file_type == NOT_FOUND:
        log.warning(
            f"unable to retrieve seeding area for grower {grower} and system {data_aggregator} for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    seed = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type
    )

    if seed.empty:
        return pd.DataFrame(columns=default_cols)

    seed = clean_file_by_file_type(
        seed, path_to_data, grower, growing_cycle, file_type, data_aggregator
    )
    if data_aggregator == DA_LDB:
        # seed = seed[~seed.Manufacturer.isin(['Services'])] # we need services to grab planting services
        # seed = seed[~seed.Task_name.isin(['CLEAN UP'])]

        mask3 = ["AC"]
        seed = seed[
            seed.Applied_unit.str.contains("|".join(mask3))
        ]  # limit seeding area to only acres

        if seed.empty:
            return pd.DataFrame(columns=default_cols)

        mask2 = ["Planting", "Ground Application - acre"]
        prod = seed[
            seed.Product.str.contains("|".join(mask2))
        ]  # do we have product called planting?
        if (
            not prod.empty
        ):  # additional filter if we have more than one task and we want to narrow to planting only
            seed = seed[seed.Product.str.contains("|".join(mask2))]

        mask = ["Planting", "plant", "Seed", "SowingAndPlanting"]
        task = seed[seed.Task_name.str.contains("|".join(mask))]
        # if not task.empty and not prod.empty:
        if not task.empty:
            seed = seed[seed.Task_name.str.contains("|".join(mask))]

    seed = seed.drop_duplicates(keep="first", inplace=False, ignore_index=True)
    # add seeding areas per field
    seed = seed.groupby(
        # by=["Farm_name", "Field_name", "Crop_type", "Sub_crop_type"],
        by=[
            "Farm_name",
            "Field_name",
            "Crop_type",
        ],  # error when adding sub_crop_type to grouping (not present)
        dropna=False,
        as_index=False,
    ).sum(numeric_only=True)

    path_to_dest = pathlib.Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if seed.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_seed_check_{growing_cycle}.csv"
        )
    else:
        seed.to_csv(
            path_to_dest.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_seed_check_"
                + str(growing_cycle)
                + ".csv"
            ),
            index=False,
        )

    return seed[default_cols]


def get_harvested_area(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
) -> pd.DataFrame:
    default_cols = [
        "Farm_name",
        "Field_name",
        "Crop_type",
        "Sub_crop_type",
        "Area_applied",
    ]

    file_type = get_file_type_by_data_aggregator(data_aggregator, file_category=HARVEST)

    if file_type == NOT_FOUND:
        log.warning(
            f"unable to retrieve seeding area for grower {grower} and system {data_aggregator} for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

        # read-in harvest data from current calendar year
    harvest = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type
    )

    if harvest.empty:
        return pd.DataFrame(columns=default_cols)

    harvest = clean_file_by_file_type(
        harvest, path_to_data, grower, growing_cycle, file_type, data_aggregator
    )

    # map field names to clear names for comparison across data sources
    field_mapping = gen.read_field_name_mapping(path_to_data, grower)

    if not field_mapping.empty:
        harvest.Field_name = harvest.apply(
            lambda x: gen.map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )

    # add seeding areas per field
    # currently, GRANULAR is the only data source that is providing `Sub_crop_type`.
    # when using GRANULAR harvest data, it needs to be ensured that the relevant info
    # is extracted:
    #
    # - for reference acreage: only FD-CIC crops with sub-crop "Grain"
    harvest = harvest.groupby(
        by=["Farm_name", "Field_name", "Crop_type", "Sub_crop_type"],
        dropna=False,
        as_index=False,
    ).sum(numeric_only=True)

    return harvest[default_cols]


def create_min_max_harvest_dates(clean_data, harvest_dates, growing_cycle):
    """Adds the earliest harvest date from previous season (MIN) and latest harvest date from current season (MAX)
    to `agg_data`.
    This approach is NOT treating split-fields (2+ crops on the same field_name) appropriately. The approach can
    be used, however, to estimate the growing-cycle cut-off dates.
    """
    prev_year = str(growing_cycle - 1)
    curr_year = str(growing_cycle)
    temp = clean_data

    harvest_dates["Crop_type"].fillna("", inplace=True)
    _min = harvest_dates[harvest_dates.Year == int(prev_year)]
    _min = _min.groupby(by=["Farm_name", "Field_name"], as_index=False).min()[
        ["Farm_name", "Field_name", "Harvest_date"]
    ]
    _min = _min.rename(columns={"Harvest_date": "Harvest_date_prev"})

    _max = harvest_dates[harvest_dates.Year == int(curr_year)]
    _max = _max.groupby(by=["Farm_name", "Field_name"], as_index=False).max()[
        ["Farm_name", "Field_name", "Harvest_date"]
    ]
    _max = _max.rename(columns={"Harvest_date": "Harvest_date_curr"})

    for df in [_min, _max]:
        if not df.empty:
            temp = pd.merge(temp, df, on=["Farm_name", "Field_name"], how="left")

    for col in ["Harvest_date_prev", "Harvest_date_curr"]:
        if col not in temp.columns:
            temp[col] = pd.NaT

    return temp


def classify_op_growing_cycle_relevant(
    operation_start, operation_type, harvest_prev, harvest_curr, growing_cycle
):
    """
    Asumptions:
    - if a harvest date is missing in `harvest_prev`, all operations that happened before 1st Sep of the previous year
      are marked 'exclude'
    - if a harvest date is missing in `harvest_curr`, all operations that happened after 31st Oct of the current year
      are marked 'exclude'
    - operations marked 'exclude' will NOT appear in the cleaned file
    """
    if pd.isnull(operation_start):
        return "missing_op_date"

    elif pd.isnull(harvest_prev) and pd.isnull(harvest_curr):
        if operation_start >= datetime.strptime(
            str(growing_cycle - 1) + "-09-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        ) and operation_start < datetime.strptime(
            str(growing_cycle) + "-11-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        ):
            return "likely_relevant"
        else:
            return "exclude"

    # cut-off operations that happened before 1st of September of previous year, when harvest_prev is missing
    elif pd.isnull(harvest_prev) and operation_start <= datetime.strptime(
        str(growing_cycle - 1) + "-09-01 00:00:00", "%Y-%m-%d %H:%M:%S"
    ):
        return "exclude"

        # cut-off operations that happened after 31st of October of current year, when harvest_curr is missing
    elif pd.isnull(harvest_curr) and operation_start >= datetime.strptime(
        str(growing_cycle) + "-11-01 00:00:00", "%Y-%m-%d %H:%M:%S"
    ):
        return "exclude"

    elif pd.isnull(harvest_prev) and operation_start <= harvest_curr:
        return "likely_relevant"

    elif operation_start == harvest_prev:
        return "exclude"

    # all operations that happend from right after previous harvest, when harvest_curr is missing
    elif (
        operation_start >= harvest_prev
        and operation_type != "Harvest"
        and pd.isnull(harvest_curr)
    ):
        return "likely_relevant"

    elif (
        operation_start >= harvest_prev
        and operation_type != "Harvest"
        and operation_start <= harvest_curr
    ):
        return "relevant"

    # include only harvest operations that happended AFTER previous harvest
    elif (
        operation_start > harvest_prev
        and operation_type == "Harvest"
        and operation_start <= harvest_curr
    ):
        return "relevant"

    elif operation_start < harvest_prev or operation_start > harvest_curr:
        return "exclude"


def mark_growing_cycle_relevant_ops(clean_data, harvest_dates, growing_cycle):
    temp = create_min_max_harvest_dates(clean_data, harvest_dates, growing_cycle)

    if temp.empty:
        log.warning(f"no harvest dates available for cycle {growing_cycle}")
        temp = add_missing_columns(
            temp,
            [
                "Farm_name",
                "Field_name",
                "Operation_type",
                "Op_relevance",
                "Growing_cycle",
            ],
        )
        return temp
    # return temp
    harvest_prev = "Harvest_date_prev"  # + str(growing_cycle-1)
    harvest_curr = "Harvest_date_curr"  # + str(growing_cycle)
    temp["Op_relevance"] = temp.apply(
        lambda x: classify_op_growing_cycle_relevant(
            x.Operation_start,
            x.Operation_type,
            x[harvest_prev],
            x[harvest_curr],
            growing_cycle,
        ),
        axis=1,
    )
    temp["Growing_cycle"] = growing_cycle

    return temp


def add_op_relevance_by_harvest_dates(
    file, path_to_dest, grower, growing_cycle, data_aggregator
):
    prev_year = str(growing_cycle - 1)
    curr_year = str(growing_cycle)

    harvest_dates = read_file_by_file_type(
        path_to_dest, grower, growing_cycle, data_aggregator, file_type=HARVEST_DATES
    )
    if not harvest_dates.empty:
        file = create_min_max_harvest_dates(
            clean_data=file, harvest_dates=harvest_dates, growing_cycle=growing_cycle
        )
        file = file.rename(
            columns={
                "Harvest_date_" + prev_year: "Harvest_date_prev",
                "Harvest_date_" + curr_year: "Harvest_date_curr",
            }
        )

    else:
        # when no harvest dates are available, artificially add harvest dates columns
        file["Harvest_date_prev"] = pd.NaT
        file["Harvest_date_curr"] = pd.NaT

    if not file.empty:
        file["Op_relevance"] = file.apply(
            lambda x: classify_op_growing_cycle_relevant(
                x.Operation_start,
                x.Operation_type,
                x["Harvest_date_prev"],
                x["Harvest_date_curr"],
                growing_cycle,
            ),
            axis=1,
        )
    else:
        file["Op_relevance"] = np.nan

    # exclude operations that are outside the harvest dates
    file = file[~file.Op_relevance.isin(["exclude"])]

    return file.reset_index(drop=True)
