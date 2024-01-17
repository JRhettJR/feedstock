import pathlib

import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...util.savers.save_report import save_report
from ..constants import CC_REPORT
from ..reference_acreage.helpers import (
    add_reference_acreage,
    calculate_coverage_relative_to_reference_acreage,
)
from .helpers import extract_cc_info_from_harvesting, extract_cc_info_from_planting


def create_cc_report(
    path_to_data: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    seed_cols = [
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
        "Op_relevance",
    ]
    harvest_cols = [
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
        "Op_relevance",
    ]

    seed = extract_cc_info_from_planting(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose
    )

    harvest = extract_cc_info_from_harvesting(
        path_to_data, grower, growing_cycle, data_aggregator, verbose
    )

    temp = pd.concat(
        [seed[seed_cols], harvest[harvest_cols]], axis=0, ignore_index=True
    )

    if not temp.empty:
        # map field names (for known fields)
        field_mapping = gen.read_field_name_mapping(path_to_data, grower)
        temp["Field_name"] = temp.apply(
            lambda x: gen.map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )

        temp = temp.sort_values(by=["Farm_name", "Field_name"], ignore_index=True)

    if temp.empty:
        log.warning(
            f"no cover crop data available for grower {grower} in system {data_aggregator} for growing cycle {growing_cycle}"
        )

    else:
        # Attach metadata to data frame to identify it
        temp.file_type = CC_REPORT
        # Add reference acreage to data
        temp = add_reference_acreage(
            apps=temp,
            path_to_data=path_to_dest,
            grower=grower,
            growing_cycle=growing_cycle,
            data_aggregator=data_aggregator,
        )
        # calculate % coverage
        temp = calculate_coverage_relative_to_reference_acreage(temp)

    file_name = f"{grower}_{data_aggregator}_cover_crop_report_{str(growing_cycle)}.csv"
    log.info(f"writing file {file_name} to {path_to_dest}")

    save_report(
        report=temp, path_to_dest=path_to_dest, grower=grower, save_name=file_name
    )
    # path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    # if not os.path.exists(path_to_dest):
    #     os.makedirs(path_to_dest)

    # temp.to_csv(
    #     path_to_dest.joinpath(
    #         grower
    #         + "_"
    #         + data_aggregator
    #         + "_cover_crop_report_"
    #         + str(growing_cycle)
    #         + ".csv"
    #     ),
    #     index=False,
    # )

    return temp
