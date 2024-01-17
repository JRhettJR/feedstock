import math
import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ...ci_prep.shp_files import fuzzy_match_field_names
from ...general import map_clear_name_using_farm_name, read_field_name_mapping
from ...shp_files.shp_overview import create_shape_file_overview
from ...util.readers.generated_reports import read_reference_acreage_report
from ..constants import CC_REPORT, FDCIC_CROPS
from ..helpers import (
    get_all_available_field_names,
    get_harvested_area,
    get_seeding_area,
)


def determine_reference_acreage(planted, harvested, shp_file):
    threshold = 0.15

    # Immediately check if we have sufficient values to continue
    missing = [
        var_name
        for var_name, var_value in [
            ("planted", planted),
            ("harvested", harvested),
            ("shp_file", shp_file),
        ]
        if var_value is None or math.isnan(var_value)
    ]
    if len(missing) > 1:
        return None, f"Missing: {missing}"

    if planted is None or math.isnan(planted):
        hr_shp = (harvested - shp_file) / shp_file
        if abs(hr_shp) <= threshold or harvested == 0:
            return shp_file, None
        else:
            return (
                None,
                f"Missing: {missing}; Harvested vs shp_file > {threshold} --> potential split field",
            )

    elif harvested is None or math.isnan(harvested):
        if abs((planted - shp_file) / shp_file) <= threshold:
            return planted, None
        else:
            return (
                None,
                f"Missing: {missing}; planted and shp acres > {threshold} --> potentially wrong shp file or missing planting ops",
            )

    elif shp_file is None or math.isnan(shp_file):
        if abs((planted - harvested) / harvested) <= threshold:
            return planted, None
        else:
            return (
                None,
                f"Missing: {missing}; planted and harvested > {threshold} --> potentially missing planting ops or double-counting of harvested acres",
            )
    else:
        pl_hr = (planted - harvested) / harvested
        pl_shp = (planted - shp_file) / shp_file
        hr_shp = (harvested - shp_file) / shp_file

        if (
            abs(pl_hr) > threshold
            and abs(pl_shp) > threshold
            and abs(hr_shp) > threshold
        ):
            return (
                None,
                f"Planted, harvested & shp_file > {threshold} --> potential split field",
            )
        if abs(pl_hr) <= threshold and pl_shp < -threshold and hr_shp < -threshold:
            return planted, None
        if abs(pl_hr) <= threshold < hr_shp and pl_shp > threshold:
            return planted, None
        if abs(pl_shp) > threshold >= abs(hr_shp) and abs(pl_hr) > threshold:
            return shp_file, None
        if abs(pl_shp) <= threshold and hr_shp > threshold:
            return (
                None,
                f"planted to harvested > {threshold} and harvested > shp --> too many harvested acres",
            )
        return planted, None


def add_reference_acreage(
    apps: pd.DataFrame,
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
) -> pd.DataFrame:
    """adds the `Reference_acreage` from an existing reference acreage report file to `apps`.
    The `Reference_acreage` is merged based on (clear) `Field_name` and `Crop_type`.

    !!! Need to monitor if `Crop_type` is always available for the merge !!!
    """
    ref_acres = read_reference_acreage_report(
        path_to_data, grower, growing_cycle, data_aggregator
    )

    if ref_acres.empty:
        return apps

    # For `Cover_crop_report`s we generally don't want to merge
    # using the `Crop_type` as a cover crop by definition does
    # not agree with the cash crop (e.g. Corn or Soybean) planted
    # during main season.
    #
    # For Feikema and JDOps, the `Crop_type` is missing. Hence,
    # we are checking for this before merging.
    if apps.file_type == CC_REPORT or pd.isna(apps["Crop_type"]).all():
        merge_on = ["Field_name"]
        ref_cols = ["Field_name", "Reference_acreage"]

    else:
        merge_on = ["Field_name", "Crop_type"]
        ref_cols = ["Field_name", "Crop_type", "Reference_acreage"]

    apps = pd.merge(
        apps,
        ref_acres[ref_cols],
        on=merge_on,
        how="left",
    )
    return apps


def calculate_coverage_relative_to_reference_acreage(
    apps: pd.DataFrame,
) -> pd.DataFrame:
    if "Reference_acreage" not in apps.columns:
        log.warning("missing column `Reference_acreage`")
        return apps

    agg = (
        apps[["Field_name", "Crop_type", "Area_applied"]]
        .groupby(by=["Field_name", "Crop_type"], as_index=False, dropna=False)
        .sum()
    )
    agg = agg.rename(columns={"Area_applied": "Area_operated"})

    # For Feikema and JDOps, the `Crop_type` is missing. Hence,
    # we are checking for this before merging.
    if pd.isna(apps["Crop_type"]).all():
        merge_on = ["Field_name"]
        ref_cols = ["Field_name", "Area_operated"]
    else:
        merge_on = ["Field_name", "Crop_type"]
        ref_cols = ["Field_name", "Crop_type", "Area_operated"]

    apps = pd.merge(
        apps,
        agg[ref_cols],
        on=merge_on,
        how="left",
    )
    apps["Area_coverage_percent"] = apps.Area_operated / apps.Reference_acreage

    return apps


def generate_reference_acreage_report(
    path_to_data: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    """Reference acreage draws from 3 different data sources:
    - planted acres
    - harvested acres
    - shp-file acres (calculated from geometry)

    Based on availability, the reference acreage will be calculated based on the
    reference acreage BUSINESS RULE.
    """
    """ TODO: Exclude non-corn crop type instead of accepting all FDCIC crops -
    could also carry through crop type and generate the report then in bulk
    upload template filter to corn. Right now, split fields are receiving the same
    reference acreage by crop type which is not the desired functionality.
    """
    # map field names to clear names for comparison across data sources
    field_mapping = read_field_name_mapping(path_to_data, grower)

    seed = get_seeding_area(path_to_data, grower, growing_cycle, data_aggregator)
    seed = seed[seed.Crop_type.isin([*FDCIC_CROPS])]
    seed = seed.rename(columns={"Area_applied": "Planted_acres"})
    if not field_mapping.empty and not seed.empty:
        seed.Field_name = seed.apply(
            lambda x: map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )

    harvest = get_harvested_area(path_to_data, grower, growing_cycle, data_aggregator)
    # filter for relevant crop types (FD-CIC crops)
    # filtering `Sub_crop_type` for "Grain" for the case of Granular
    harvest = harvest[
        (harvest.Sub_crop_type.isin(["Grain", np.nan]))
        & (harvest.Crop_type.isin([*FDCIC_CROPS]))
    ]
    harvest = harvest.rename(columns={"Area_applied": "Harvest_acres"})

    available_fields = pd.DataFrame(
        get_all_available_field_names(path_to_data, grower, growing_cycle, verbose),
        columns=["Field_name"],
    )

    # read-in shp-file acres
    # DONE: Generate shapefile file inplace to avoid csv write/read issues (ie Liebsch)
    #
    # NOTE: This generation is currently done for every data aggregator for each farmer
    shp_overview = create_shape_file_overview(path_to_data, path_to_dest, grower)

    if shp_overview.empty:
        log.error(
            f"shape file overview unavailable for grower {grower} and cycle {growing_cycle}"
        )

    if not field_mapping.empty and not shp_overview.empty:
        shp_overview.Field_name = shp_overview.apply(
            lambda x: map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )

    # combine available field names with planted acres data
    reference_acres = pd.merge(
        available_fields,
        seed[["Field_name", "Crop_type", "Planted_acres"]],
        on="Field_name",
        how="left",
    )
    # add harvested acres data
    reference_acres = pd.merge(
        reference_acres,
        harvest[["Field_name", "Crop_type", "Harvest_acres"]],
        on=["Field_name", "Crop_type"],
        how="outer",
    )

    # do fuzzy matching;
    #
    # The shp_file name is used to map the reference acreage
    # from the shp_file_overview into the reference_acreage_report.
    reference_acres = fuzzy_match_field_names(
        reference_acres,
        reference_acres["Field_name"],
        shp_overview["Field_name"],
        ratio=60,
    )

    # Reorder columns
    columns = reference_acres.columns.tolist()
    column1_index = columns.index("Potential_shp_match")
    column2_index = columns.index("Match_score")
    columns.insert(1, columns.pop(column1_index))
    columns.insert(2, columns.pop(column2_index))
    reference_acres = reference_acres[columns]

    """ merge shp-file acreage. Preserve values from seed and harvest data
    already merged in as those files are already filtered to fields in program
    """
    # DONE: using the shp file name to merge reference acreage
    reference_acres = pd.merge(
        reference_acres,  # .drop(["Field_name"], axis=1),
        shp_overview[["Field_name", "Acreage_calc"]],
        left_on=["Potential_shp_match"],
        right_on=["Field_name"],
        how="left",
    )
    reference_acres.drop(["Field_name_y"], axis=1, inplace=True)
    reference_acres.rename(columns={"Field_name_x": "Field_name"}, inplace=True)

    # add metrics whether planted acres are available
    reference_acres["PLA_available"] = reference_acres.Planted_acres.apply(
        lambda p: True if not pd.isna(p) else False
    )

    # implement decision mechanism for reference acreage
    reference_acres[["Reference_acreage", "Exclusion_reason"]] = reference_acres.apply(
        lambda r: pd.Series(
            determine_reference_acreage(
                r.Planted_acres, r.Harvest_acres, r.Acreage_calc
            )
        ),
        axis=1,
    )

    # Drop rows with missing `Crop_type`.
    #
    # In some cases the merging leads to duplicate rows
    # due to missing `Crop_type` attributions in data
    # (compare Feikema 2022 Manure data). To avoid this,
    # rows with empty `Crop_type` are dropped.
    reference_acres = reference_acres.dropna(
        subset="Crop_type", axis=0, ignore_index=True
    )

    return reference_acres.sort_values(by="Field_name", ignore_index=True)
