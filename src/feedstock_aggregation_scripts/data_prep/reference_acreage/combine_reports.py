import os
import pathlib

import pandas as pd
from loguru import logger as log

from ...data_prep.constants import NOT_FOUND
from ...util.readers.generated_reports import read_reference_acreage_report
from ..grower_data_agg_mapping import grower_da_mapping


def combine_all_reference_acreage_reports(
    path_to_processed: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame | None:
    data_aggregators = grower_da_mapping.get(grower, NOT_FOUND)
    if data_aggregators == NOT_FOUND:
        log.warning(
            f"no data aggregator info available for grower {grower} in `grower_da_mapping`"
        )
        return

    combined = pd.DataFrame()

    for da in data_aggregators:
        temp = read_reference_acreage_report(
            path_to_data=path_to_processed,
            grower=grower,
            growing_cycle=growing_cycle,
            data_aggregator=da,
        )
        if not temp.empty:
            temp["Data_source"] = da
            combined = pd.concat([combined, temp])

    combined = combined.sort_values(by="Field_name", ascending=True, ignore_index=True)

    path_to_dest = pathlib.Path(path_to_processed).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if combined.empty:
        log.warning(
            f"no data to save for {grower}_reference_acreage_report_{growing_cycle}.csv"
        )
    else:
        combined.to_csv(
            path_to_dest.joinpath(
                grower + "_reference_acreage_report_" + str(growing_cycle) + ".csv"
            ),
            index=False,
        )

    return combined


def get_filtered_combined_reference_acreage_report(
    path_to_processed: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    combined = combine_all_reference_acreage_reports(
        path_to_processed, grower, growing_cycle
    )

    filtered = combined.dropna(subset="Reference_acreage")

    path_to_dest = pathlib.Path(path_to_processed).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if filtered.empty:
        log.warning(
            f"no data to save for {grower}_reference_acreage_report_filtered_{growing_cycle}.csv"
        )
    else:
        filtered.to_csv(
            path_to_dest.joinpath(
                grower
                + "_reference_acreage_report_filtered_"
                + str(growing_cycle)
                + ".csv"
            ),
            index=False,
        )

    return filtered
