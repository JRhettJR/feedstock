import os
import pathlib

import pandas as pd
from loguru import logger as log

from ...general import read_verified_file
from ..constants import COL_ORDER
from ..max_total_input_merge.max_total_input_merge import (
    create_comprehensive_inputs_from_cleaned_files,
)
from .helpers import add_missing_columns, set_soc_inputs_to_default
from .lime import get_lime_inputs
from .manure import get_manure_inputs
from .npk_extraction import calculate_inputs_by_row, extract_product_breakdown_info
from .overwrite_values import overwrite_values_using_matrix


def prepare_GREET_inputs(
    df: pd.DataFrame,
    path_to_data: str | pathlib.Path,
    path_to_processed: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
):
    pb = extract_product_breakdown_info(df.Product.unique())

    verified = read_verified_file(path_to_data, grower)

    verified = verified[["Field_name", "Area_applied"]]

    temp = pd.merge(df, verified, on="Field_name", how="outer")
    # return temp
    temp = pd.merge(temp, pb, on="Product", how="outer")
    # return temp
    temp = calculate_inputs_by_row(temp)
    # return temp
    temp = temp.groupby(by=["Client", "Field_name"], dropna=False, as_index=False).sum()
    # return temp

    rel_col = ["Client", "Field_name"]
    rel_col.extend([c for c in temp.columns if "Total_" in c])

    temp = temp[rel_col]
    temp = pd.merge(temp, verified, on="Field_name", how="outer")
    # return temp

    #
    # yield
    #
    # yields = get_total_yield_info(path_to_data, grower)
    # yields = yields.dropna(subset=['Total_dry_yield'])
    # return yields
    # temp = pd.merge(temp, yields, on=['Field_name'], how='outer')
    # temp['Total_yield'] = temp.Total_dry_yield / temp.Area_applied

    #
    # lime
    #
    lime = get_lime_inputs(path_to_data, grower, growing_cycle)
    temp = pd.merge(
        temp, lime[["Field_name", "Total_lime"]], on=["Field_name"], how="left"
    )
    # adjust lime to lbs / acre
    temp.Total_lime = temp.Total_lime / temp.Area_applied

    #
    # manure
    #
    manure = get_manure_inputs(path_to_processed, grower, growing_cycle)
    temp = pd.merge(
        temp, manure[["Field_name", "Num_manure_ops"]], on=["Field_name"], how="left"
    )
    temp["Manure"] = temp.Num_manure_ops.apply(
        lambda m: "Manure" if m >= 1 else "No Manure"
    )

    return temp


def create_FDCIC_input_overview(
    path_to_data: str | pathlib.Path,
    path_to_processed: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
) -> pd.DataFrame:
    df = create_comprehensive_inputs_from_cleaned_files(
        path_to_processed, grower, growing_cycle
    )
    inputs = prepare_GREET_inputs(
        df, path_to_data, path_to_processed, grower, growing_cycle
    )

    # filter for verified fields
    verified = read_verified_file(path_to_data, grower)
    inputs = inputs[inputs.Field_name.isin(verified.Field_name)]
    inputs = inputs.reset_index(drop=True)
    # artificially add missing columns
    inputs = add_missing_columns(inputs, COL_ORDER)
    inputs = set_soc_inputs_to_default(inputs)
    # inputs = inputs.drop_duplicates(ignore_index=True)

    inputs = overwrite_values_using_matrix(
        inputs, path_to_processed, grower, growing_cycle
    )
    # sort columns to desired order
    inputs = inputs[COL_ORDER]
    # return inputs
    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if inputs.empty:
        log.warning(f"No data to save for {grower}_FDCIC_inputs.csv")
    else:
        inputs.to_csv(path_to_dest.joinpath(grower + "_FDCIC_inputs.csv"), index=False)

    return inputs
