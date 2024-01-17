import os
import pathlib

import pandas as pd
from loguru import logger as log


def save_report(
    report: pd.DataFrame, path_to_dest: str | pathlib.Path, grower: str, save_name: str
) -> None:
    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if report.empty:
        log.warning(f"no data to save for {grower} in {save_name}")
    else:
        report.to_csv(
            path_to_dest.joinpath(save_name),
            index=False,
        )
