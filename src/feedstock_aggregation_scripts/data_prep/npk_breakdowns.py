import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

# -------------------------------------------------------------------------------------------
# General functions - NPK CALCULATION
#


def get_fert_list(path_to_data: str | pathlib.Path) -> pd.DataFrame:
    """DUPLICATE FUNCTION | DEPRECATED

    Should use `get_breakdown_list()` from
        src/feedstock_aggregation_scripts/util/readers/general.py
    """
    try:
        fert_list = pd.read_csv(
            pathlib.Path(path_to_data).joinpath(
                "verity_chemical_product_breakdown_table - Sheet1.csv"
            )
        )

    except FileNotFoundError:
        log.exception(
            f"no `verity_chemical_product_breakdown_table - Sheet1.csv` at {path_to_data}"
        )
        return pd.DataFrame()

    rel_cols = [
        "product_name",
        "product_type",
        "% N",
        "% P2O5",
        "% K2O",
        "lbs / gal",
        "EEF product (y/n) - Fert only",
    ]

    fert_list = fert_list[(fert_list["product_type"].isin(["Fertilizer", "EEF"]))][
        rel_cols
    ]
    fert_list = fert_list.dropna(subset="product_name", axis=0)

    return fert_list.reset_index(drop=True)


def get_n(product: str, total: float, fert_list: pd.DataFrame) -> float:
    temp = fert_list[fert_list.product_name == product]
    n = temp["% N"].iloc[0] if not np.isnan(temp["% N"].iloc[0]) else 0
    conversion = (
        temp["lbs / gal"].iloc[0] if not np.isnan(temp["lbs / gal"].iloc[0]) else 0
    )

    return n * conversion * total


def get_p(product: str, total: float, fert_list: pd.DataFrame) -> float:
    temp = fert_list[fert_list.product_name == product]
    p = temp["% P2O5"].iloc[0] if not np.isnan(temp["% P2O5"].iloc[0]) else 0
    conversion = (
        temp["lbs / gal"].iloc[0] if not np.isnan(temp["lbs / gal"].iloc[0]) else 0
    )

    return p * conversion * total


def get_k(product: str, total: float, fert_list: pd.DataFrame) -> float:
    temp = fert_list[fert_list.product_name == product]
    k = temp["% K2O"].iloc[0] if not np.isnan(temp["% K2O"].iloc[0]) else 0
    conversion = (
        temp["lbs / gal"].iloc[0] if not np.isnan(temp["lbs / gal"].iloc[0]) else 0
    )

    return k * conversion * total


def get_NPK_rate(
    apps: pd.DataFrame, path_to_data: str | pathlib.Path, grower="DUMMY"
) -> pd.DataFrame:
    if apps.empty:
        return apps

    fert_list = get_fert_list(path_to_data)

    a = apps[apps.Product.isin(fert_list.product_name)].reset_index(drop=True)

    if a.empty:
        log.warning(
            "missing fertiliser product breakdowns or no chemical input products to break down"
        )
        return a

    try:
        a["TOTAL_N"] = a.apply(
            lambda x: get_n(x.Product, x.Applied_total, fert_list), axis=1
        )
        a["TOTAL_P"] = a.apply(
            lambda x: get_p(x.Product, x.Applied_total, fert_list), axis=1
        )
        a["TOTAL_K"] = a.apply(
            lambda x: get_k(x.Product, x.Applied_total, fert_list), axis=1
        )

        for col in ["TOTAL_N", "TOTAL_P", "TOTAL_K"]:
            col_name = col.replace("TOTAL", "RATE")

            a[col_name] = a[col] / a.Area_applied

    except Exception as e:
        log.exception(str(e))
        a["RATE_N"] = a.apply(
            lambda x: get_n(x.Product, x.Applied_rate, fert_list), axis=1
        )
        a["RATE_P"] = a.apply(
            lambda x: get_p(x.Product, x.Applied_rate, fert_list), axis=1
        )
        a["RATE_K"] = a.apply(
            lambda x: get_k(x.Product, x.Applied_rate, fert_list), axis=1
        )

        for col in ["RATE_N", "RATE_P", "RATE_K"]:
            col_name = col.replace("RATE", "TOTAL")

            a[col_name] = a[col] * a.Area_applied

    return a


def prepare_NPK(
    apps: pd.DataFrame, path_to_data: str | pathlib.Path, grower: str
) -> pd.DataFrame:
    # rel_npk_cols = ['Farm_name', 'Field_name', 'Crop_type', 'TOTAL_N', 'TOTAL_P', 'TOTAL_K', 'RATE_N', 'RATE_P', 'RATE_K']
    rel_npk_cols = [
        "Farm_name",
        "Field_name",
        # "Crop_type",
        "TOTAL_N",
        "TOTAL_P",
        "TOTAL_K",
        "RATE_N",
        "RATE_P",
        "RATE_K",
    ]

    if apps.empty:
        return pd.DataFrame(columns=rel_npk_cols)

    # total_fert = apps.groupby(by=['Farm_name', 'Field_name', 'Product', 'Crop_type'], dropna=False, as_index=False).sum()
    total_fert = apps.groupby(
        by=["Farm_name", "Field_name", "Product"], dropna=False, as_index=False
    ).sum(numeric_only=True)
    npk = get_NPK_rate(total_fert, path_to_data, grower)

    # npk = npk.groupby(by=['Farm_name', 'Field_name', 'Crop_type'], dropna=False, as_index=False).sum()
    npk = npk.groupby(
        by=["Farm_name", "Field_name"], dropna=False, as_index=False
    ).sum()

    if npk.empty:
        return pd.DataFrame(columns=rel_npk_cols)

    npk = npk[rel_npk_cols]

    return npk


# def prepare_NPK(apps, path_to_data):

#     total_fert = apps.groupby(by=['Farm_name', 'Field_name', 'Product'], dropna=False, as_index=False).sum()
#     npk = get_NPK_rate(total_fert, path_to_data)

#     npk = npk.groupby(by=['Farm_name', 'Field_name'], dropna=False, as_index=False).sum()


#     rel_npk_cols = ['Farm_name', 'Field_name', 'TOTAL_N', 'TOTAL_P', 'TOTAL_K', 'RATE_N', 'RATE_P', 'RATE_K']
#     npk = npk[rel_npk_cols]

#     return npk
