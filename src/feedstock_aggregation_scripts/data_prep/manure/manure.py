import pathlib

import pandas as pd
from loguru import logger as log

from ...util.savers.save_report import save_report
from .helpers import extract_manure_info_from_apps


def create_manure_report(
    path_to_data: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    rel_cols = [
        "Client",
        "Farm_name",
        "Field_name",
        "Task_name",
        "Product",
        "Operation_start",
        "Operation_end",
        "Area_applied",
        "Applied_rate",
        "Applied_total",
        "Applied_unit",
        "Operation_type",
        "Manure_op",
        "Harvest_date_prev",
        "Harvest_date_curr",
        "Op_relevance",
        "Growing_cycle",
    ]

    apps = extract_manure_info_from_apps(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose
    )

    if apps.empty:
        log.warning(
            f"no manure data available for grower {grower} in system {data_aggregator} for growing cycle {growing_cycle}"
        )
        return pd.DataFrame(columns=rel_cols)
    # add growing cycle to data set as reference
    apps["Growing_cycle"] = growing_cycle
    file_name = f"{grower}_{data_aggregator}_manure_report_{str(growing_cycle)}.csv"

    log.info(f"writing file {file_name} to {path_to_dest}")
    save_report(
        report=apps, path_to_dest=path_to_dest, grower=grower, save_name=file_name
    )

    # if file_exists(path_to_data=path_to_dest, grower=grower, file_name=file_name):
    #     print(f"writing to existing file {file_name} at {path_to_dest}...")

    #     add_to_existing_file(
    #         file=apps,
    #         path_to_data=path_to_dest,
    #         grower=grower,
    #         growing_cycle=growing_cycle,
    #         data_aggregator=data_aggregator,
    #         file_type=MANURE_REPORT,
    #         sort_cols=["Farm_name", "Field_name", "Growing_cycle"],
    #         file_name=file_name,
    #     )
    # else:
    #     # path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    #     print(f"[INFO] writing new file {file_name} at {path_to_dest}...")
    #     save_report(
    #         report=apps,
    #         path_to_dest=path_to_dest,
    #         grower=grower,
    #         save_name=file_name,
    #     )
    # if not os.path.exists(path_to_dest):
    #     os.makedirs(path_to_dest)

    # apps.to_csv(path_to_dest.joinpath(file_name), index=False)

    return apps
