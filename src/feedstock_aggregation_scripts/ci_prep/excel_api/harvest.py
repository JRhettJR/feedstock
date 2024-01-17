import pathlib

import pandas as pd
from loguru import logger as log

from ..readers import read_cleaned_files


def get_total_yield_info(path_to_data: str | pathlib.Path, grower: str):
    cleaned_files = read_cleaned_files(path_to_data, grower)

    if len(cleaned_files) == 0:
        log.info(f"no cleaned files available at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    rel_cols = ["Farm_name", "Field_name", "Crop_type", "Total_dry_yield", "System"]
    yield_info = pd.DataFrame(columns=["Farm_name", "Field_name"])

    for da, file in cleaned_files.items():
        if "Crop_type" not in file.columns and "Total_dry_yield" not in file.columns:
            log.info(
                f"missing `Crop_type` AND `Total_dry_yield` in cleaned file from system {da}"
            )
            continue

        elif "Crop_type" not in file.columns:
            temp = file.dropna(subset=["Total_dry_yield"])
            temp = temp.drop_duplicates(
                subset=["Farm_name", "Field_name", "Total_dry_yield"], ignore_index=True
            )

        elif "Total_dry_yield" not in file.columns:
            temp = file.dropna(subset=["Crop_type"])
            temp = temp.drop_duplicates(
                subset=["Farm_name", "Field_name", "Crop_type"], ignore_index=True
            )

        else:
            temp = file.dropna(subset=["Crop_type", "Total_dry_yield"])
            temp = temp.drop_duplicates(
                subset=["Farm_name", "Field_name", "Crop_type", "Total_dry_yield"],
                ignore_index=True,
            )

        temp["System"] = da

        yield_info = pd.concat([yield_info, temp], ignore_index=True)

    return yield_info[rel_cols]
