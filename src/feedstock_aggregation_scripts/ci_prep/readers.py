import pathlib

from loguru import logger as log

from ..data_prep.constants import DATA_AGGREGATORS
from ..general import read_cleaned_file


def read_cleaned_files(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> dict:
    cleaned_files = {}

    # loop through all data aggregators and detect files
    for da in DATA_AGGREGATORS:
        temp = read_cleaned_file(path_to_data, grower, growing_cycle, da_name=da)
        if not temp.empty:
            log.info(f"successfully read file from {da}")
            temp["Data_source"] = da
            cleaned_files[da] = temp

    return cleaned_files
