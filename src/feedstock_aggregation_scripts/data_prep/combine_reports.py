import pathlib

import pandas as pd
from loguru import logger as log

from ..data_prep.constants import NOT_FOUND
from ..util.readers.generated_reports import read_generated_report
from ..util.savers.save_report import save_report
from .grower_data_agg_mapping import grower_da_mapping
from .helpers import add_missing_columns


def combine_all_reports_by_type(
    report_type: str,
    path_to_processed: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
) -> pd.DataFrame | None:
    data_aggregators = grower_da_mapping.get(grower, NOT_FOUND)
    if data_aggregators == NOT_FOUND:
        log.warning(
            f"no data aggregator info available for grower {grower} in `grower_da_mapping`"
        )
        return

    combined = pd.DataFrame(columns=[])

    for da in data_aggregators:
        temp = read_generated_report(
            report_type=report_type,
            path_to_data=path_to_processed,
            grower=grower,
            growing_cycle=growing_cycle,
            data_aggregator=da,
        )

        if not temp.empty:
            temp["Data_source"] = da
            temp = add_missing_columns(temp, ["Reference_acreage"])
            combined = pd.concat([combined, temp])
        elif not list(combined.columns):
            combined = pd.DataFrame(columns=temp.columns)

    if not combined.empty:
        combined = combined.sort_values(
            by="Field_name", ascending=True, ignore_index=True
        )

    if list(combined.columns):
        save_name = f"{grower}_{report_type}_{str(growing_cycle)}.csv"

        save_report(
            report=combined,
            path_to_dest=path_to_processed,
            grower=grower,
            save_name=save_name,
        )

    return combined


def get_filtered_combined_report(
    report_type: str,
    path_to_processed: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
) -> pd.DataFrame:
    combined = combine_all_reports_by_type(
        report_type=report_type,
        path_to_processed=path_to_processed,
        grower=grower,
        growing_cycle=growing_cycle,
    )
    if not combined.empty:
        filtered = combined.dropna(subset="Reference_acreage")
    else:
        filtered = combined

    if list(filtered.columns):
        save_name = f"{grower}_{report_type}_filtered_{str(growing_cycle)}.csv"
        save_report(
            report=filtered,
            path_to_dest=path_to_processed,
            grower=grower,
            save_name=save_name,
        )

    return filtered
