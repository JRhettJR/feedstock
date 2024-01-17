import numpy as np
import pandas as pd
from loguru import logger as log

from ..config import settings
from ..util.conversion import convert_quantity_by_unit
from ..util.readers.general import get_breakdown_list

GREET_ELEMENT_COLS = [
    "% Ammonia",
    "% Urea",
    "% AN",
    "% AS",
    "% UAN",
    "% MAP N",
    "% DAP N",
    "% MAP P2O5",
    "% DAP P2O5",
    "% K2O",
    "% CaCO3",
]


def get_greet_element(
    product: str,
    element: str,
    total: float,
    unit: str,
    reference_acreage: float,
    fert_list: pd.DataFrame,
) -> float:
    temp = fert_list[fert_list.product_name == product]
    if temp.empty:
        return float(np.nan)
    n = temp[element].iloc[0] if not np.isnan(temp[element].iloc[0]) else 0
    conversion = (
        temp["lbs / gal"].iloc[0] if not np.isnan(temp["lbs / gal"].iloc[0]) else 0
    )
    total, _ = convert_quantity_by_unit(total, unit)
    return n * conversion * total / reference_acreage


def add_greet_elements(
    breakdown_list: pd.DataFrame, element: str, column_name: str, bulk: pd.DataFrame
):
    bulk[column_name] = bulk.apply(
        lambda x: get_greet_element(
            x["INPUT_NAME"],
            element,
            x["INPUT_RATE"],
            x["INPUT_UNIT"],
            x["REFERENCE_ACREAGE"],
            breakdown_list,
        ),
        axis=1,
    )
    return bulk


def add_elements(bulk: pd.DataFrame, breakdown_list: pd.DataFrame):
    for col in GREET_ELEMENT_COLS:
        col_name = col.split("%")[-1].strip()
        log.info(f"adding {col_name} to bulk")
        add_greet_elements(breakdown_list, col, col_name, bulk)
    return bulk


def add_breakdowns(bulk: pd.DataFrame):
    path_to_data = settings.data_prep.source_path

    breakdown_list = get_breakdown_list(path_to_data)

    bulk = add_elements(bulk, breakdown_list)
    return bulk
