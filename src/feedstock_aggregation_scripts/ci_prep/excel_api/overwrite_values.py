import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ...data_prep.constants import NOT_FOUND
from ...fd_cic.defaults_2021 import get_fdcic_2021_defaults
from ..constants import (
    FDCIC_DIESEL,
    FDCIC_ELECTRICITY,
    FDCIC_GAS,
    FDCIC_HERBICIDE,
    FDCIC_INPUTS,
    FDCIC_INSECTICIDE,
    FDCIC_K2O_FERTILISER,
    FDCIC_LIME,
    FDCIC_LPG,
    FDCIC_N_FERTILISER,
    FDCIC_N_FERTILISERS,
    FDCIC_NG,
    FDCIC_P2O5_FERTILISER,
    FDCIC_P2O5_FERTILISERS,
)


def read_overwrites_file(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_FDCIC_overwrites_" + str(growing_cycle) + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"no overwrites file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    overwrites = pd.read_csv(path)

    return overwrites


def set_fdcic_default(col: str) -> float:
    fdcic_2021_defaults = get_fdcic_2021_defaults()
    col_trans = col.replace("Total_", "").replace("_", " ").upper()

    if col_trans in FDCIC_INPUTS:
        if col_trans in FDCIC_N_FERTILISERS:
            n_ferts = fdcic_2021_defaults.get(FDCIC_N_FERTILISER, -1)
            val = n_ferts.get(col_trans, -1)

        elif col_trans in FDCIC_P2O5_FERTILISERS:
            p_ferts = fdcic_2021_defaults.get(FDCIC_P2O5_FERTILISER, -1)
            val = p_ferts.get(col_trans, -1)

        else:
            val = fdcic_2021_defaults.get(col_trans, -1)

    if val == -1:
        log.warning(f"unable to read default values for {col_trans}")
        return np.nan
    else:
        return val


def overwrite_values_using_matrix(
    inputs: pd.DataFrame,
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
):
    overwrites = read_overwrites_file(path_to_data, grower, growing_cycle)

    non_input_cols = ["Client", "Field_name", "Area_applied", "Crop_type"]
    overwrite_cols = overwrites.columns.drop(non_input_cols)

    for field, crop in zip(overwrites.Field_name, overwrites.Crop_type, strict=False):
        log.info(field, crop)
        temp = inputs[
            (inputs.Field_name.isin([field])) & (inputs.Crop_type.isin([crop]))
        ]

        if not temp.empty:
            # relevant values to overwrite exist
            for col in overwrite_cols:
                row = overwrites[
                    (overwrites.Field_name.isin([field]))
                    & (overwrites.Crop_type.isin([crop]))
                ]
                val = row[col].iloc[0]
                idx = temp.index[0]
                log.info(
                    "idx:",
                    idx,
                    "| orig_val:",
                    temp.loc[idx, col],
                    "| col:",
                    col,
                    "| val:",
                    val,
                )

                if col in ["Manure", "Cover_crop"]:
                    if isinstance(val, str):
                        inputs.loc[idx, col] = col.replace("_", " ")

                elif col == "Tillage":
                    inputs.loc[idx, col] = val

                else:
                    if isinstance(val, str):
                        # set to default
                        inputs.loc[idx, col] = set_fdcic_default(col)

                    elif not pd.isnull(val):
                        # overwrite with given value if not null
                        inputs.loc[idx, col] = val
        else:
            # room to manually add complete fields from matrix
            continue

        break
    return inputs


def overwrite_default_categories(
    inputs: pd.DataFrame, default_categories: list[str]
) -> pd.DataFrame:
    fdcic_2021_defaults = get_fdcic_2021_defaults()
    # Fuel
    if FDCIC_DIESEL in default_categories:
        inputs["Total_diesel"] = fdcic_2021_defaults.get(FDCIC_DIESEL, -1)

    if FDCIC_GAS in default_categories:
        inputs["Total_gas"] = fdcic_2021_defaults.get(FDCIC_GAS, -1)

    if FDCIC_LPG in default_categories:
        inputs["Total_lpg"] = fdcic_2021_defaults.get(FDCIC_LPG, -1)

    if FDCIC_NG in default_categories:
        inputs["Total_ng"] = fdcic_2021_defaults.get(FDCIC_NG, -1)

    if FDCIC_ELECTRICITY in default_categories:
        inputs["Total_electricity"] = fdcic_2021_defaults.get(FDCIC_ELECTRICITY, -1)

    # Fertilisers
    if FDCIC_N_FERTILISER in default_categories:
        n_fert = fdcic_2021_defaults.get(FDCIC_N_FERTILISER, -1)
        if n_fert == -1:
            log.warning("unable to read default values for n fertiliser")
        else:
            for cat, val in n_fert.items():
                inputs["Total_" + cat] = val

    if FDCIC_P2O5_FERTILISER in default_categories:
        p_fert = fdcic_2021_defaults.get(FDCIC_P2O5_FERTILISER, -1)
        if p_fert == -1:
            log.warning("unable to read default values for p2o5 fertiliser")
        else:
            for cat, val in p_fert.items():
                inputs["Total_" + cat] = val

    if FDCIC_K2O_FERTILISER in default_categories:
        inputs["Total_K"] = fdcic_2021_defaults.get(FDCIC_K2O_FERTILISER, -1)

    # Lime
    if FDCIC_LIME in default_categories:
        inputs["Total_lime"] = fdcic_2021_defaults.get(FDCIC_LIME, -1)

    # Pesticides
    if FDCIC_HERBICIDE in default_categories:
        inputs["Total_AI_H"] = fdcic_2021_defaults.get(FDCIC_HERBICIDE, -1)

    if FDCIC_INSECTICIDE in default_categories:
        inputs["Total_AI_I"] = fdcic_2021_defaults.get(FDCIC_INSECTICIDE, -1)

    return inputs
