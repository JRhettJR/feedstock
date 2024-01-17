import os
import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...util.cleaners.helpers import add_missing_columns
from ...util.readers.comprehensive import create_comprehensive_df
from ...util.readers.general import read_file_by_file_type
from ..constants import HARVEST_DATES
from ..helpers import mark_growing_cycle_relevant_ops

# %% [markdown]
# ---
# # Cleaned file

# %% [markdown]
# ## Helpers


# %%
def fill_crop_type_by_dominant_crop_type(crop_type, dominant_crop_type):
    if pd.isnull(dominant_crop_type):
        return crop_type
    else:
        return dominant_crop_type


def merge_crop_types(crop_type1, crop_type2):
    if pd.isnull(crop_type1):
        return crop_type2
    else:
        return crop_type1


def supplement_crop_type_attribution(clean_file, split_field_report):
    if not split_field_report.empty:
        prep = pd.merge(
            clean_file,
            split_field_report[
                [
                    "Farm_name",
                    "Field_name",
                    "Split_field_likelihood",
                    "Dominant_crop_type",
                ]
            ],
            on=["Farm_name", "Field_name"],
            how="left",
        )
    else:
        prep = add_missing_columns(
            clean_file, ["Split_field_likelihood", "Dominant_crop_type"]
        )

    temp = prep[prep.Split_field_likelihood == "likely"]
    # drop cases where no dominant crop type present
    temp = temp.dropna(subset=["Dominant_crop_type"])

    if temp.empty:
        # no split field cases present according to split-field report
        temp = prep

    temp = temp.drop_duplicates(subset=["Field_name", "Crop_type"]).dropna(
        subset="Crop_type"
    )

    temp.Crop_type = temp.apply(
        lambda x: fill_crop_type_by_dominant_crop_type(
            x.Crop_type, x.Dominant_crop_type
        ),
        axis=1,
    )
    temp = temp.drop_duplicates(subset=["Farm_name", "Field_name", "Crop_type"])[
        [
            "Farm_name",
            "Field_name",
            "Crop_type",
            "Split_field_likelihood",
            "Dominant_crop_type",
        ]
    ]

    supplemented = pd.merge(
        clean_file, temp, on=["Farm_name", "Field_name"], how="left"
    )

    supplemented = supplemented.rename(columns={"Crop_type_x": "Crop_type"})
    supplemented.Crop_type = supplemented.apply(
        lambda c: merge_crop_types(c.Crop_type, c.Crop_type_y), axis=1
    )

    supplemented = supplemented.drop(columns="Crop_type_y")

    return supplemented


# %% [markdown]
# ## File generation


# %%
def create_clean_file(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=True
):
    product_mapping = gen.read_product_mapping_file(path_to_data)
    field_mapping = gen.read_field_name_mapping(path_to_data, grower)

    prev = create_comprehensive_df(
        path_to_data, grower, growing_cycle - 1, data_aggregator, verbose
    )
    curr = create_comprehensive_df(
        path_to_data, grower, growing_cycle, data_aggregator, verbose
    )
    temp = pd.concat([prev, curr], axis=0, ignore_index=True)

    if temp.empty:
        log.warning(
            f"no data available at {path_to_data} for grower {grower} in "
            f"system {data_aggregator} and cycle {growing_cycle}"
        )

    if not field_mapping.empty:
        temp.Field_name = temp.apply(
            lambda x: gen.map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )

    if "Product" in temp.columns:
        temp.Product = temp.apply(
            lambda x: gen.map_clear_name(product_mapping, x.Product), axis=1
        )
        temp["Product_type"] = temp.apply(
            lambda x: gen.map_fert_type(product_mapping, x.Product), axis=1
        )

    for col in ["Operation_start", "Operation_end", "Product", "Product_type"]:
        if col not in temp.columns:
            temp[col] = np.nan
    # return temp
    harvest_dates = read_file_by_file_type(
        path_to_data=path_to_dest,
        grower=grower,
        growing_cycle=growing_cycle,
        data_aggregator=data_aggregator,
        file_type=HARVEST_DATES,
    )
    # return harvest_dates
    # if not harvest_dates.empty:
    temp = mark_growing_cycle_relevant_ops(
        clean_data=temp, harvest_dates=harvest_dates, growing_cycle=growing_cycle
    )
    temp = temp[~temp.Op_relevance.isin(["exclude"])].reset_index(drop=True)

    # SOMETHING IS GOING WRONG IN THE 'SUPPLEMENT_CROP_TYPE' FUCNTION.
    # DUPLICATED RECORDS ARE CREATED FOR PLANTED AND HARVEST ACRES AND BUSHELS HARVESTED.
    # OUTPUT LOOKS PERFECT WITHOUT IT. PLEASE REWORK IF THIS FUNCTION IS NEEDED FOR SPECIFIC USE CASES.

    # temp = supplement_crop_type_attribution(clean_file=temp, split_field_report=split_field_report)
    temp = temp.sort_values(
        by=["Field_name", "Operation_start"], ascending=True, ignore_index=True
    )

    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if temp.empty:
        log.warning(
            f"no data to save for {grower}_{data_aggregator}_cleaned_{growing_cycle}.csv"
        )
    else:
        temp.to_csv(
            path_to_dest.joinpath(
                grower
                + "_"
                + data_aggregator
                + "_cleaned_"
                + str(growing_cycle)
                + ".csv"
            ),
            index=False,
        )

    return temp
