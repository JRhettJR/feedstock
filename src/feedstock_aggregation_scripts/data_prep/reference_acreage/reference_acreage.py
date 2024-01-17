import pathlib

import pandas as pd
from loguru import logger as log

from ...util.savers.save_report import save_report
from .helpers import generate_reference_acreage_report


def create_reference_acreage_report(
    path_to_data: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    """The reference acreage report will be generated for now by `data_aggregator`
    and `growing_cycle`.

    ### This needs to be generalized / aggregated in the future
    ### to have consistent reference acreages per field.
    """
    reference_acres = generate_reference_acreage_report(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose
    )

    file_name = (
        f"{grower}_{data_aggregator}_reference_acreage_report_{str(growing_cycle)}.csv"
    )

    log.info(f"writing file {file_name} to {path_to_dest}")
    save_report(
        report=reference_acres,
        path_to_dest=path_to_dest,
        grower=grower,
        save_name=file_name,
    )
    # path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    # if not os.path.exists(path_to_dest):
    #     os.makedirs(path_to_dest)

    # reference_acres.to_csv(
    #     path_to_dest.joinpath(
    #         grower
    #         + "_"
    #         + data_aggregator
    #         + "_reference_acreage_report_"
    #         + str(growing_cycle)
    #         + ".csv"
    #     ),
    #     index=False,
    # )

    return reference_acres
