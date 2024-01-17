import pathlib

import pandas as pd

from ..config import settings

path_to_data = settings.data_prep.source_path

qu_converter = pd.read_csv(pathlib.Path(path_to_data).joinpath("unit_conversions.csv"))


def convert_quantity_by_unit(quantity: float | int, unit: str) -> tuple[float, str]:
    """Converts `quantity` based on respective `unit` according to the following convention:

    - Converts LIQUID quantities to GAL.
    - Converts DRY quantities to LBS.
    - Converts seed product quantities to BAG.
    """
    if not isinstance(unit, str):
        return quantity, unit

    temp = qu_converter[qu_converter.unit == unit.lower()]
    if temp.empty:
        return quantity * 1, unit

    conversion_factor = temp.conversion_factor.iloc[0]
    u = temp.target_unit.iloc[0]

    return quantity * conversion_factor, u
