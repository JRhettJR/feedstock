import pathlib
from datetime import datetime

import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...util.cleaners.general import get_cleaned_file_by_file_type
from ...util.readers.general import (
    get_file_type_by_data_aggregator,
    read_file_by_file_type,
)
from ..constants import FDCIC_CROPS, HARVEST, HARVEST_DATES, NOT_FOUND, PLANTING
from ..helpers import classify_op_growing_cycle_relevant, create_min_max_harvest_dates


def extract_cc_info_from_planting(
    path_to_data: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    prev_year = str(growing_cycle - 1)
    curr_year = str(growing_cycle)

    default_cols = [
        "Client",
        "Farm_name",
        "Field_name",
        "Planting_date",
        "Product",
        "Crop_type",
        "Area_applied",
        "Applied_rate",
        "Applied_total",
        "Applied_unit",
        "Operation_type",
        "Harvest_date_" + prev_year,
        "Harvest_date_" + curr_year,
        "Op_relevance",
    ]

    # get file_type for PLANTING data
    file_type = get_file_type_by_data_aggregator(
        data_aggregator, file_category=PLANTING
    )

    if file_type == NOT_FOUND:
        log.warning(
            f"[WARNING] unable to generate cover crop file from planting for grower {grower} and system {data_aggregator} for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    # get planting data from previous year
    seed = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle - 1, data_aggregator, file_type, verbose
    )

    if seed.empty:
        # if no seeding data available for previous year, potentially no CC planted
        return pd.DataFrame(columns=default_cols)

    seed = seed.rename(columns={"Operation_start": "Planting_date"})
    # return seed
    field_mapping = gen.read_field_name_mapping(path_to_data, grower)
    if not field_mapping.empty:
        seed.Field_name = seed.apply(
            lambda x: gen.map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )

    harvest_dates = read_file_by_file_type(
        path_to_dest, grower, growing_cycle, data_aggregator, HARVEST_DATES
    )
    # return harvest_dates
    if not harvest_dates.empty:
        temp = create_min_max_harvest_dates(
            clean_data=seed, harvest_dates=harvest_dates, growing_cycle=growing_cycle
        )

    else:
        # when no harvest dates are available, artificially add harvest dates columns
        temp = seed
        temp["Harvest_date_prev"] = pd.NaT
        temp["Harvest_date_curr"] = pd.NaT
    # return temp
    temp["Op_relevance"] = temp.apply(
        lambda x: classify_op_growing_cycle_relevant(
            x.Planting_date,
            x.Operation_type,
            x.Harvest_date_prev,
            x.Harvest_date_curr,
            growing_cycle,
        ),
        axis=1,
    )
    # exclude operations that are outside the harvest dates
    temp = temp[~temp.Op_relevance.isin(["exclude"])]
    # filter out operations that have FD-CIC crops as their crop_type
    temp = temp[(~temp.Crop_type.isin(FDCIC_CROPS)) & (~temp.Crop_type.isnull())]

    return temp


def classify_relevant_cc_harvest_op(
    harvest_date: datetime, planting_date: datetime
) -> str:
    if harvest_date <= planting_date:
        return "relevant"
    else:
        return "exclude"


def extract_cc_info_from_harvesting(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    default_cols = [
        "Client",
        "Farm_name",
        "Field_name",
        "Harvest_date",
        "Crop_type",
        "Area_applied",
        "Total_dry_yield",
        "Total_dry_yield_check",
        "Applied_unit",
        "Operation_type",
        "Planting_date",
        "Op_relevance",
    ]

    # get file_type for PLANTING data
    file_type_planting = get_file_type_by_data_aggregator(
        data_aggregator, file_category=PLANTING
    )

    # get file_type for HARVEST data
    file_type_harvest = get_file_type_by_data_aggregator(
        data_aggregator, file_category=HARVEST
    )

    if file_type_planting == NOT_FOUND and file_type_harvest == NOT_FOUND:
        log.warning(
            f"unable to generate cover crop file from harvesting for grower {grower} and system {data_aggregator} for cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=default_cols)

    harvest = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type_harvest, verbose
    )
    if harvest.empty:
        # if no harvest data available, no potential CC harvest took place
        return pd.DataFrame(columns=default_cols)

    # to determine planting dates as cut-off for CC-harvest ops
    seed = get_cleaned_file_by_file_type(
        path_to_data,
        grower,
        growing_cycle,
        data_aggregator,
        file_type_planting,
        verbose,
    )

    if seed.empty:
        seed = pd.DataFrame(columns=["Farm_name", "Field_name", "Operation_start"])
        seed.Operation_start = pd.NaT

    field_mapping = gen.read_field_name_mapping(path_to_data, grower)
    if not field_mapping.empty:
        harvest.Field_name = harvest.apply(
            lambda x: gen.map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )
        if not seed.empty:
            seed.Field_name = seed.apply(
                lambda x: gen.map_clear_name_using_farm_name(
                    field_mapping, x.Field_name, x.Farm_name
                ),
                axis=1,
            )

    harvest = harvest.rename(columns={"Operation_start": "Harvest_date"})

    planting_dates = seed.rename(columns={"Operation_start": "Planting_date"})[
        ["Farm_name", "Field_name", "Planting_date"]
    ]

    temp = pd.merge(harvest, planting_dates, on=["Farm_name", "Field_name"], how="left")
    temp["Op_relevance"] = temp.apply(
        lambda x: classify_relevant_cc_harvest_op(x.Harvest_date, x.Planting_date),
        axis=1,
    )

    # exclude operations that are outside the harvest dates
    temp = temp[~temp.Op_relevance.isin(["exclude"])]
    # filter out operations that have FD-CIC crops as their crop_type
    temp = temp[(~temp.Crop_type.isin(FDCIC_CROPS)) & (~temp.Crop_type.isnull())]

    return temp
