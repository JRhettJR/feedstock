import pandas as pd
from loguru import logger as log

from .npk import add_breakdowns


def add_field_practices(bulk_agg: pd.DataFrame, bulk: pd.DataFrame):
    # initialize columns
    for practice in ["TILL_PRACTICE", "N_MGT_PRACTICE"]:
        bulk_agg[practice] = None

    for idx, row in bulk_agg.iterrows():
        temp = bulk[bulk["FIELD_NAME"].isin([row["FIELD_NAME"]])]
        for practice in ["TILL_PRACTICE", "N_MGT_PRACTICE"]:
            bulk_agg.iloc[idx, bulk_agg.columns.get_loc(practice)] = temp[
                practice
            ].iloc[0]


def prepare_for_excel_workbook(bulk: pd.DataFrame):
    rel_cols = [
        "FIELD_NAME",
        "CROP_TYPE",
        "GROWING_CYCLE",
        "YIELD",
        "DIESEL",
        "GASOLINE",
        "NATURAL GAS",
        "LIQUEFIED PETROLEUM GAS",
        "ELECTRICITY",
        "Ammonia",
        "Urea",
        "AN",
        "AS",
        "UAN",
        "MAP N",
        "DAP N",
        "MAP P2O5",
        "DAP P2O5",
        "K2O",
        "CaCO3",
        "N_MGT_PRACTICE",
        "GREEN_AMMONIA",
        "TILL_PRACTICE",
        "MANURE_USE",
        "COVER_CROP_USE",
        "Total_N",
        "Total_P",
        "Total_K",
    ]
    if bulk.empty:
        log.error("empty bulk upload file")
        return pd.DataFrame()

    bulk = add_breakdowns(bulk)

    bulk_agg = bulk.groupby(
        by=["FIELD_NAME", "CROP_TYPE", "GROWING_CYCLE"], as_index=False, dropna=False
    ).sum(numeric_only=True)
    # add non-numeric field practices
    add_field_practices(bulk_agg, bulk)
    # correct boolean columns
    for col in ["GREEN_AMMONIA", "MANURE_USE", "COVER_CROP_USE"]:
        bulk_agg[col] = bulk_agg[col].apply(lambda c: True if c else False)

    # Add metrics
    bulk_agg["Total_N"] = (
        bulk_agg["Ammonia"]
        + bulk_agg["Urea"]
        + bulk_agg["AN"]
        + bulk_agg["AS"]
        + bulk_agg["UAN"]
        + bulk_agg["MAP N"]
        + bulk_agg["DAP N"]
    )
    bulk_agg["Total_P"] = bulk_agg["MAP P2O5"] + bulk_agg["DAP P2O5"]
    bulk_agg["Total_K"] = bulk_agg["K2O"]

    return bulk_agg[rel_cols]
