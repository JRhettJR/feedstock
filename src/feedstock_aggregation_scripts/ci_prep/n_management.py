from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger as log
from pandas import DataFrame, isna, merge

from ..config import settings
from ..data_prep.npk_breakdowns import prepare_NPK
from ..data_prep.split_field.split_field import get_harvest_params
from ..util.readers.generated_reports import read_shp_file_overview
from .constants import INPUT_TYPE_FERTILIZER
from .helpers import get_datetime
from .soil_data_extract.soil_temp import (
    get_hourly_soil_temp_data,
    return_start_4r_timing,
)


def categorize_fertilizer_timing_4r(
    input_type: str,
    operation_start: datetime,
    operation_end: datetime,
    growing_cycle: int,
) -> str | None:
    """categorizes all products where `input_type` == `FERTILIZER` according to 4R timing
    business rules.

    Return values:
    - None --> input_type != FERTILIZER
    - Fall
    - Spring
    - FLAG --> for further investigation
    - NO_4R --> ineligible for 4R
    """
    if input_type != INPUT_TYPE_FERTILIZER:
        return None

    if (
        get_datetime(growing_cycle - 1, month=9, day=1)
        <= operation_start
        < get_datetime(growing_cycle, 1, 1)
        and get_datetime(growing_cycle - 1, 9, 1)
        <= operation_end
        < get_datetime(growing_cycle, 1, 1)
        # cases where no `Operation_end` date is available
        or get_datetime(growing_cycle - 1, 9, 1)
        <= operation_start
        < get_datetime(growing_cycle, 1, 1)
        and isna(operation_end)
    ):
        # time frame for FALL applications
        return "Fall"

    elif (
        get_datetime(growing_cycle, month=3, day=1)
        <= operation_start
        < get_datetime(growing_cycle, 7, 1)
        and get_datetime(growing_cycle, 3, 1)
        <= operation_end
        < get_datetime(growing_cycle, 7, 1)
        # cases where no `Operation_end` date is available
        or get_datetime(growing_cycle, 3, 1)
        <= operation_start
        < get_datetime(growing_cycle, 7, 1)
        and isna(operation_end)
    ):
        # time frame for SPRING applications
        return "Spring"

    elif (
        get_datetime(growing_cycle, month=1, day=1)
        <= operation_start
        < get_datetime(growing_cycle, 3, 1)
        and get_datetime(growing_cycle, 1, 1)
        <= operation_end
        < get_datetime(growing_cycle, 3, 1)
        # cases where no `Operation_end` date is available
        or get_datetime(growing_cycle, 1, 1)
        <= operation_start
        < get_datetime(growing_cycle, 3, 1)
        and isna(operation_end)
    ):
        # time frame in between FALL and SPRING --> flag
        # for additional investigation
        return "FLAG"

    else:
        # unacceptable timing for 4R nitrogen management
        return "NO_4R"


def add_fertilizer_timing_categorization_4r(
    comprehensive_input_list: DataFrame, growing_cycle: int
) -> DataFrame:
    """adds column `Fertilizer_timing` with the categorization of a given `FERTILIZER`
    application (`Input_type`).
    """
    # The timing of fertilizing activities needs to be categorizes
    # broadly into 2 categories:
    #
    # (1) Spring applications --> March 1st to July 1st
    # (2) Fall applications --> October 1st to December 31st
    #
    # Applications in spring, wicalcualte_nitrogen_use_efficiencyll be automatically valid for "4R",
    # applications in fall, will need an additional check of soil
    # temperature.
    #
    # Operations that happen January 1st through February 28th/29th,
    # will be flagged for further investigation
    #
    # All other fertilizer operations are not in line with "4R"
    # timing requirements.
    comprehensive_input_list["Fertilizer_timing"] = comprehensive_input_list.apply(
        lambda x: categorize_fertilizer_timing_4r(
            x.Input_type, x.Operation_start, x.Operation_end, growing_cycle
        ),
        axis=1,
    )

    return comprehensive_input_list


def add_fertilizer_timing_decision_4r(
    field: str,
    grower: str,
    growing_cycle: int,
    comprehensive_input_list: DataFrame,
) -> str:
    fertilizer_input_list = comprehensive_input_list[
        (comprehensive_input_list["Field_name"].isin([field]))
        & (comprehensive_input_list["Input_type"].isin(["FERTILIZER", "EEF"]))
    ]

    no4r_applications = fertilizer_input_list[
        fertilizer_input_list["Fertilizer_timing"].isin(["NO_4R"])
    ]
    fall_applications = fertilizer_input_list[
        fertilizer_input_list["Fertilizer_timing"].isin(["Fall"])
    ]
    flagged_applications = fertilizer_input_list[
        fertilizer_input_list["Fertilizer_timing"].isin(["FLAG"])
    ]

    # If any fertilizing operations are clasified as `NO_4R`, the
    # nitrogen management for that field will be `NO_4R`
    if fertilizer_input_list.empty or not no4r_applications.empty:
        return "NO_4R"

    # If flagged operations are present, return `NO_4R`
    elif not flagged_applications.empty:
        log.warning(f"operation FLAGGED for field {field} and grower {grower}")
        return "NO_4R"

    # If there is at least 1 fertilizing operation that is classified as `Fall`,
    # we need to check the temperature cut-off date for those operations.
    elif not fall_applications.empty:
        shapefile_data = read_shp_file_overview(settings.data_prep.dest_path, grower)
        shapefile_data_by_field = shapefile_data[
            shapefile_data.Field_name.isin([field])
        ]

        if not shapefile_data_by_field.empty:
            soil_temp_data = get_hourly_soil_temp_data(
                f"{growing_cycle - 1}-09-01",
                f"{growing_cycle - 1}-12-31",
                shapefile_data_by_field.Centroid_lat.values[0],
                shapefile_data_by_field.Centroid_long.values[0],
            )
            start_timing_for_4r = return_start_4r_timing(soil_temp_data)

            for fall_application in fall_applications.itertuples(index=False):
                # If at least one of those `Fall` applications happened before the
                # temperature cut-off, we will classify as `NO_4R`.
                if fall_application.Operation_start < pd.to_datetime(
                    start_timing_for_4r
                ):
                    return "NO_4R"

            # If all operation happened after the cut-off date, we can safely categorize
            # nitrogen management as `4R`.
            return "4R"

        # If no shapefile data is present, log error and return `NO_4R`.
        else:
            log.error(
                f"missing shp file location for field {field} and grower {grower}; \
                    unable to determine 4R temperature cut-off date for fall applications"
            )
            return "NO_4R"

        # TODO: reevaluate percentage of 4R applications.
        # 4R is a mechanism to optimize and reduce N fertilizer
        # applications on a given field. This implies to a certain
        # extend that only a fraction of a field may actually be
        # fertilized.

        # percentage_of_4r_applications = (
        #     spring_applied_area + qualifying_fall_applied_area
        # ) / total_applied_area

        # if percentage_of_4r_applications > 0.5:
        #     return "4R"
        # else:
        #     return "NO_4R"

    # If no fertilizer applications are present that are either `NO_4R` or `Fall`
    # applications, we can safely assume, that only operations are present, that
    # are categorized as `Spring`. In this case, we will automatically classify as
    # `4R`.
    else:
        return "4R"


def calculate_nitrogen_use_efficiency(
    decision_matrix: DataFrame,
    comprehensive_input_list: DataFrame,
    path_to_data: str | Path,
    grower: str,
) -> DataFrame:
    # Nitrogen Use Efficiency (NUE) is defined as
    #
    # NUE = total yield harvested per field / total nitrogen applied

    apps = comprehensive_input_list[
        comprehensive_input_list.Input_type == INPUT_TYPE_FERTILIZER
    ]

    total_yield = get_harvest_params(comprehensive_input_list)
    # return total_yield
    total_yield = total_yield.rename(columns={"Total_dry_yield": "Total_dry_yield_agg"})

    # TODO: Calculate and map npk values by `Crop_type`
    npk = prepare_NPK(apps, path_to_data, grower)
    # to avoid multiple lines due to non-unified values
    # in column `Farm_name`
    if not npk.empty:
        npk = npk.groupby(by="Field_name", as_index=False).sum(numeric_only=True)

    # add total yield by crop type and field
    decision_matrix = merge(
        decision_matrix,
        total_yield[["Field_name", "Crop_type", "Total_dry_yield_agg"]],
        on="Field_name",
        how="left",
    )
    # add total nitrogen usage by field
    decision_matrix = merge(
        decision_matrix,
        npk[["Field_name", "TOTAL_N"]],
        on="Field_name",
        how="left",
    )

    # calculate NUE
    decision_matrix["NUE"] = (
        decision_matrix["TOTAL_N"] / decision_matrix["Total_dry_yield_agg"]
    )

    return decision_matrix


def add_nitrogen_use_efficiency(
    decision_matrix: DataFrame,
    comprehensive_input_list: DataFrame,
    path_to_data: str | Path,
    grower: str,
) -> DataFrame:
    """

    @params:
    path_to_data - path where the chemical product breakdown table file
                    is located.
    """
    # The Nitrogen Use Efficiency (NUE) is Verity's metric
    # to classify nitrogen management practices.
    #
    # A NUE < 1.0 signifies the "right amount" of nitrogen
    # fertiliser used. A field with this NUE score is eligible
    # for "4R" nitrogen management practices in regard to
    # the category "right amount".
    # A NUE >= 1.0 is not eligible.
    decisions = calculate_nitrogen_use_efficiency(
        decision_matrix, comprehensive_input_list, path_to_data, grower
    )

    # decisions["Fertilizer_amount"] = comprehensive_input_list.NUE.apply(
    #     lambda n: "4R" if n <= 1.0 else "NO_4R"
    # )

    return decisions


def add_eef_decision(
    field_name: str, bulk_upload_template: DataFrame
) -> DataFrame | None:
    bulk_upload_template_by_field = bulk_upload_template[
        bulk_upload_template["Field_name"] == field_name
    ]
    if bulk_upload_template_by_field["EEF_product"].str.contains("y").any():
        return "EEF"
    else:
        return None


def add_n_management_decision(timing_4r: str, amount_4r: str, eef: str) -> str:
    # 1. check for eligibility for 4R
    if timing_4r == "4R" and amount_4r == "4R":
        return "4R"
    # 2. check for eligibility for EEF
    elif eef == "EEF":
        return "EEF"
    # If no eligibility for 4R or EEF, then classify
    # as `BUSINESS_AS_USUAL`.
    else:
        return "BUSINESS_AS_USUAL"
