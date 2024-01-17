import pathlib

import pandas as pd

from ...data_prep.helpers import count_ops
from ...general import read_lime_report


def get_lime_inputs(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    lime = read_lime_report(path_to_data, grower)

    # take lime from 3 years, incl. current
    rel_years = list(range(growing_cycle - 2, growing_cycle + 1, 1))
    lime = lime[lime.Growing_cycle.isin(rel_years)]

    lime.Client = lime.Client.apply(lambda x: grower if pd.isna(x) else x)

    lime.Applied_total = lime.Applied_total / 3.0
    lime_ops = count_ops(lime, "Num_lime_ops")
    # return lime_ops
    lime_total = lime.groupby(by=["Field_name"], as_index=False).sum()
    lime_total = lime_total.rename(columns={"Applied_total": "Total_lime"})

    lime_total = pd.merge(lime_total, lime_ops, on="Field_name", how="left")

    return lime_total[["Field_name", "Total_lime", "Num_lime_ops"]]
