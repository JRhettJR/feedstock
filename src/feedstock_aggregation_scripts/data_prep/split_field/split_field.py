import numpy as np
import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...general import read_verified_file
from ...util.cleaners.general import get_cleaned_file_by_file_type
from ...util.readers.general import get_file_type_by_data_aggregator
from ..constants import HARVEST, NOT_FOUND, PLANTING
from ..helpers import count_ops

# %% [markdown]
# ---
# # Split field report

# %% [markdown]
# ## Harvest


# %%
split_field_threshold = float(0.05)


def calc_yield_params(harvest):
    rel_cols = [
        "Farm_name",
        "Field_name",
        "Crop_type",
        "Total_dry_yield",
        "Total_area_harvested",
    ]
    if harvest.empty:
        return pd.DataFrame(columns=rel_cols)

    temp = harvest.groupby(
        by=["Farm_name", "Field_name", "Crop_type"], dropna=False, as_index=False
    ).sum(numeric_only=True)
    temp = temp.rename(columns={"Area_applied": "Total_area_harvested"})

    # set yield values that are 0 to NaN
    temp.Total_dry_yield = temp.Total_dry_yield.apply(
        lambda y: np.nan if y == 0.0 else y
    )

    return temp[rel_cols]


def get_harvest_params(clean_data):
    rel_cols = [
        "Farm_name",
        "Field_name",
        "Crop_type",
        "Total_dry_yield",
        "Total_area_harvested",
        "Num_ops_harvest",
    ]
    if clean_data.empty:
        return pd.DataFrame(columns=rel_cols)
    harvest = clean_data[clean_data.Operation_type == "Harvest"]
    if harvest.empty:
        return pd.DataFrame(columns=rel_cols)

    # harvest = harvest.dropna(subset=['Area_applied', 'Total_dry_yield']) # eliminate phantom records
    harvest = harvest.drop(
        harvest[
            (harvest["Area_applied"] == np.nan) & (harvest["Total_dry_yield"] == np.nan)
        ].index
    )
    # return harvest
    num_ops = count_ops(harvest, "Num_ops_harvest")
    yield_params = calc_yield_params(harvest)
    # return yield_params
    return pd.merge(yield_params, num_ops, on=["Farm_name", "Field_name"], how="left")


# %%
# clean_data = read_cleaned_file(path_to_dest, grower, growing_cycle, data_aggregator)
# clean_data[clean_data.Operation_type == 'Harvest']
# get_harvest_params(clean_data).iloc[50:100]

# %% [markdown]
# ## Seeding


# %%
def calc_seeding_params(seed):
    if seed.empty:
        return pd.DataFrame(
            columns=[
                "Farm_name",
                "Field_name",
                "Crop_type",
                "Total_area_seeded",
                "Applied_total",
                "Applied_unit",
            ]
        )

    if "Crop_type" in seed.columns:
        temp = seed.groupby(
            by=["Farm_name", "Field_name", "Crop_type"], dropna=False, as_index=False
        ).sum(numeric_only=True)
    else:
        temp = seed.groupby(
            by=["Farm_name", "Field_name"], dropna=False, as_index=False
        ).sum()
        temp["Crop_type"] = np.nan

    temp = temp.rename(columns={"Area_applied": "Total_area_seeded"})
    temp = pd.merge(
        temp[
            [
                "Farm_name",
                "Field_name",
                "Crop_type",
                "Total_area_seeded",
                "Applied_total",
            ]
        ],
        seed[["Farm_name", "Field_name", "Applied_unit"]].drop_duplicates(
            subset=["Farm_name", "Field_name"]
        ),
        on=["Farm_name", "Field_name"],
        how="left",
    )
    return temp


def get_seeding_params(clean_data):
    seed = clean_data[clean_data.Operation_type == "Planting"]

    num_ops = count_ops(seed, "Num_ops_seeding")
    seed_params = calc_seeding_params(seed)

    return pd.merge(seed_params, num_ops, on=["Farm_name", "Field_name"])


# %%
# pl = get_cleaned_file_by_file_type(path_to_data, grower, growing_cycle, data_aggregator, file_type=CFV_PLANTING)
# read_cleaned_file(path_to_dest, grower, data_aggregator)

# %% [markdown]
# ## Helpers


# %%
def add_crop_type_count(clean_data):
    rel_cols = ["Farm_name", "Field_name", "Crop_type"]

    temp = clean_data.drop_duplicates(subset=rel_cols, ignore_index=True)
    temp = temp.groupby(by=["Farm_name", "Field_name"], as_index=False).count()[
        rel_cols
    ]
    temp = temp.rename(columns={"Crop_type": "Num_crops_planted"})

    clean_data = pd.merge(clean_data, temp, on=["Farm_name", "Field_name"], how="left")
    return clean_data


def determine_split_field_likelihood(relative_area_operated, applied_total):
    # if np.isnan(relative_area_operated) or isinstance(relative_area_operated, str):
    if np.isnan(relative_area_operated):
        return "unable_to_determine"
    if (
        (applied_total > 0)
        and (relative_area_operated >= split_field_threshold)
        and (relative_area_operated < (1 - split_field_threshold))
    ):
        return "likely"
    else:
        return "unlikely"


def determine_dominant_crop_type(
    crop_type,
    relative_area_operated,
    split_field_likelihood,
):
    if not isinstance(crop_type, str):
        return np.nan

    if relative_area_operated > (1 - split_field_threshold):
        return crop_type
    # using PLUG threshold to capture cases that were cleaned to 'unlikely' but are not fulfilling the above condition
    elif relative_area_operated >= 0.5 and split_field_likelihood == "unlikely":
        return crop_type
    else:
        return np.nan


def clean_split_field_likelihood(split_field_data):
    # before adding the dominant crop type clean cases where 'likely' appears only once as the likelihood for split field
    temp = split_field_data.groupby(
        by=["Field_name", "Split_field_likelihood"], as_index=False
    ).count()
    # filter for 'likely' == 1
    temp = temp[(temp.Operated_acres == 1) & (temp.Split_field_likelihood == "likely")]

    if temp.empty:
        return split_field_data

    for field in temp.Field_name.unique():
        idx = split_field_data[split_field_data.Field_name == field].index
        split_field_data.loc[idx, "Split_field_likelihood"] = "unlikely"

    return split_field_data


def add_dominant_crop_type(split_field_data):
    if split_field_data.empty:
        split_field_data["Dominant_crop_type"] = np.nan
        return split_field_data

    split_field_data = clean_split_field_likelihood(split_field_data)
    split_field_data["Dominant_crop_type"] = split_field_data.apply(
        lambda x: determine_dominant_crop_type(
            x.Crop_type,
            x.Relative_area_operated,
            x.Split_field_likelihood,
        ),
        axis=1,
    )
    temp = split_field_data.dropna(subset="Dominant_crop_type")[
        ["Farm_name", "Field_name", "Dominant_crop_type"]
    ]
    split_field_data = split_field_data.drop(columns=["Dominant_crop_type"])

    split_field_data = pd.merge(
        split_field_data, temp, on=["Farm_name", "Field_name"], how="left"
    )
    split_field_data = split_field_data.drop_duplicates(ignore_index=True)

    return split_field_data


# %% [markdown]
# ## Split field cases


# %%
def prepare_split_field_data_planting(
    path_to_data, grower, growing_cycle, data_aggregator, verbose=True
):
    default_cols = [
        "Farm_name",
        "Field_name",
        "Crop_type",
        "Total_area_seeded",
        "Applied_total",
        "Applied_unit",
        "Total_dry_yield",
        "Total_area_harvested",
        "Num_crops_planted",
        "Operated_acres",
        "Relative_area_operated",
        "Split_field_likelihood",
        "Dominant_crop_type",
    ]

    # get file_type for PLANTING data
    file_type = get_file_type_by_data_aggregator(
        data_aggregator, file_category=PLANTING
    )

    if file_type == NOT_FOUND:
        log.warning(
            f"system {data_aggregator} not included in `prepare_split_field_data_planting`"
        )
        return pd.DataFrame(columns=default_cols)

    seed = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
    )

    if seed.empty:
        return pd.DataFrame(columns=default_cols)

    temp = calc_seeding_params(seed)
    # exclude operations whithout input
    # temp = temp[temp.Applied_total > 0]

    temp = add_crop_type_count(clean_data=temp)
    # return temp
    temp = temp[temp.Num_crops_planted > 1]

    if temp.empty:
        # no potential split fields in planting data
        return pd.DataFrame(columns=default_cols)

    # determine total planted acres from data
    planted_acres = temp.groupby(
        by=["Farm_name", "Field_name"], dropna=False, as_index=False
    ).sum()[["Farm_name", "Field_name", "Total_area_seeded"]]

    planted_acres = planted_acres.rename(
        columns={"Total_area_seeded": "Operated_acres"}
    )

    temp = pd.merge(temp, planted_acres, on=["Farm_name", "Field_name"], how="left")

    # add metrics to determine likelihood of split-field
    temp["Relative_area_operated"] = temp.Total_area_seeded / temp.Operated_acres
    temp["Split_field_likelihood"] = temp.apply(
        lambda r: determine_split_field_likelihood(
            r.Relative_area_operated, r.Applied_total
        ),
        axis=1,
    )

    return temp


def prepare_split_field_data_harvesting(
    path_to_data, grower, growing_cycle, data_aggregator, verbose=True
):
    default_cols = [
        "Farm_name",
        "Field_name",
        "Crop_type",
        "Total_area_seeded",
        "Applied_total",
        "Applied_unit",
        "Total_dry_yield",
        "Total_area_harvested",
        "Num_crops_planted",
        "Operated_acres",
        "Relative_area_operated",
        "Split_field_likelihood",
        "Dominant_crop_type",
    ]

    # get file_type for HARVEST data
    file_type = get_file_type_by_data_aggregator(data_aggregator, file_category=HARVEST)

    if file_type == NOT_FOUND:
        log.warning(
            f"system {data_aggregator} not included in `prepare_split_field_data_harvesting`"
        )
        return pd.DataFrame(columns=default_cols)

    harvest = get_cleaned_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
    )
    if harvest.empty:
        return pd.DataFrame(columns=default_cols)

    temp = calc_yield_params(harvest)
    # exclude operations whithout input
    # temp = temp[temp.Applied_total > 0]

    temp = add_crop_type_count(clean_data=temp)
    temp = temp[temp.Num_crops_planted > 1]

    if temp.empty:
        # no potential split fields in harvesting data
        return pd.DataFrame(columns=default_cols)

    # determine total planted acres from data
    harvested_acres = temp.groupby(
        by=["Farm_name", "Field_name"], dropna=False, as_index=False
    ).sum()[["Farm_name", "Field_name", "Total_area_harvested"]]
    harvested_acres = harvested_acres.rename(
        columns={"Total_area_harvested": "Operated_acres"}
    )

    temp = pd.merge(temp, harvested_acres, on=["Farm_name", "Field_name"])

    # add metrics to determine likelihood of split-field
    temp["Relative_area_operated"] = temp.Total_area_harvested / temp.Operated_acres
    temp["Split_field_likelihood"] = temp.apply(
        lambda r: determine_split_field_likelihood(
            r.Relative_area_operated, r.Total_dry_yield
        ),
        axis=1,
    )

    return temp


# %% [markdown]
# ## Report generation


# %%
def prepare_split_field_data(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator, verbose=True
):
    planting = prepare_split_field_data_planting(
        path_to_data, grower, growing_cycle, data_aggregator, verbose
    )
    harvesting = prepare_split_field_data_harvesting(
        path_to_data, grower, growing_cycle, data_aggregator, verbose
    )

    temp = pd.concat([planting, harvesting], axis=0).reset_index(drop=True)

    if not temp.empty:
        # map field names (for known fields)
        field_mapping = gen.read_field_name_mapping(path_to_data, grower)
        temp.Field_name = temp.apply(
            lambda x: gen.map_clear_name_using_farm_name(
                field_mapping, x.Field_name, x.Farm_name
            ),
            axis=1,
        )

    temp = temp.loc[temp["Split_field_likelihood"] == "likely"]

    temp = add_dominant_crop_type(temp)
    # to avoid double listing in cases, where all 3 cases result in split-field entries
    # temp = pd.concat([temp, plant_harvest], axis=0)  # .reset_index(drop=True)
    temp = temp.sort_values(by="Field_name", ignore_index=True)

    verified = read_verified_file(path_to_data, grower)
    # mark verified fields
    temp["Verified_field"] = temp.Field_name.apply(
        lambda f: True if verified.Field_name.isin([f]).any() else False
    )

    # else:
    #     temp = temp[
    #         [
    #             "Farm_name",
    #             "Field_name",
    #             "Crop_type",
    #             "Total_area_seeded",
    #             "Applied_total",
    #             "Applied_unit",
    #             "Total_dry_yield",
    #             "Total_area_harvested",
    #             "Num_crops_planted",
    #             "Operated_acres",
    #             "Relative_area_operated",
    #             "Split_field_likelihood",
    #         ]
    #     ]

    return temp
