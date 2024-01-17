from pathlib import Path
from typing import Callable

from loguru import logger as log

from ..data_prep.grower_data_agg_mapping import grower_da_mapping


def execute_for_all(
    path_to_data: str | Path,
    path_to_processed: str | Path,
    path_to_dest: str | Path,
    growing_cycle: int,
    function: Callable,
) -> None:
    for grower in grower_da_mapping:
        log.info(f"running {function} for grower {grower}:")
        function(path_to_data, path_to_processed, path_to_dest, grower, growing_cycle)
