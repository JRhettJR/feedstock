import pathlib

import pandas as pd
from loguru import logger as log

from ...data_prep.helpers import count_ops
from ...general import read_manure_report


def determine_product_state_by_unit(unit: str) -> str | None:
    default = None
    if not isinstance(unit, str):
        return default
    if unit in ["LBS"]:
        return "DRY"

    elif unit in ["GAL"]:
        return "LIQUID"

    else:
        return default


def get_manure_inputs(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    manure = read_manure_report(path_to_data, grower)

    if manure.empty:
        log.info(f"no manure records available for grower {grower}")
        return pd.DataFrame()

    manure["Manure_type"] = manure.Applied_unit.apply(determine_product_state_by_unit)
    # return manure
    rel_manure = manure[manure.Growing_cycle == growing_cycle]
    if manure.empty:
        log.info(
            f"no manure records available for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame()

    manure_ops = count_ops(rel_manure, "Num_manure_ops")

    manure_total = rel_manure.groupby(
        by=["Field_name", "Manure_type"], as_index=False
    ).sum()
    manure_total = manure_total.rename(columns={"Applied_total": "Total_manure"})

    manure_total = pd.merge(manure_total, manure_ops, on="Field_name", how="left")

    return manure_total[["Field_name", "Total_manure", "Manure_type", "Num_manure_ops"]]
