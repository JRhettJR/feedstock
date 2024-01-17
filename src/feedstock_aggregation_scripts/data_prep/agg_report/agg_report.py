import pandas as pd

from ...util.readers.general import read_file_by_file_type
from ..constants import DA_GRANULAR, DA_JDOPS, GRAN_PLANTING, JD_FUEL
from ..split_field.split_field import get_harvest_params, get_seeding_params
from .helpers import (
    get_all_farm_field_crop_type_combinations,
    get_apps_params,
    get_clean_data,
    get_fuel_params,
    get_manure_params,
    get_tillage_params,
    get_verified_data,
    prepare_NPK,
)

# %%


def aggregate_app_data(
    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
):
    clean_data = get_clean_data(
        path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
    )
    verified = get_verified_data(path_to_data, grower)

    if data_aggregator == DA_GRANULAR:
        harvest = get_harvest_params(clean_data[clean_data.Sub_crop_type == "Grain"])
        planting = read_file_by_file_type(
            path_to_data,
            grower,
            growing_cycle,
            data_aggregator,
            file_type=GRAN_PLANTING,
        )
        seed = get_seeding_params(planting)
    else:
        harvest = get_harvest_params(clean_data)
        seed = get_seeding_params(clean_data)
    # return seed
    till = get_tillage_params(clean_data)
    # return harvest
    # apps = clean_data[clean_data.Operation_type == 'Application']
    apps = clean_data[~clean_data.Operation_type.isin(["Harvest", "Tillage"])]
    # return apps
    npk = prepare_NPK(apps, path_to_data, grower)
    # return npk
    apps_params = get_apps_params(clean_data)
    # return apps_params

    if data_aggregator == DA_JDOPS:
        file_type = JD_FUEL
    else:
        file_type = ""

    fuel = get_fuel_params(
        path_to_data, grower, growing_cycle, data_aggregator, file_type=file_type
    )
    # return fuel
    manure = get_manure_params(path_to_dest, grower, growing_cycle, data_aggregator)

    rel_combinations = get_all_farm_field_crop_type_combinations(
        verified, data_array=[seed, harvest, npk, apps_params]
    )
    # return rel_combinations
    agg = rel_combinations[rel_combinations.Field_name.isin(verified.Field_name)]
    # return agg
    agg = agg.sort_values(by=["Field_name", "Crop_type"], ignore_index=True)

    # add acres from verified acres file
    agg = pd.merge(
        agg, verified[["Field_name", "Area_applied"]], on="Field_name", how="left"
    )

    # return npk
    for df in [seed, harvest, till, npk, apps_params, fuel, manure]:
        # avoid duplicate columns
        if "Farm_name" in agg.columns:
            df = df.drop(columns="Farm_name")
        if "Crop_type" in agg.columns and "Crop_type" in df.columns:
            # df = df.drop(columns='Crop_type')
            agg = pd.merge(agg, df, on=["Field_name", "Crop_type"], how="left")
        else:
            agg = pd.merge(agg, df, on=["Field_name"], how="left")

        # supplement additional parameters
        if "Total_dry_yield" in df.columns:
            # return agg
            agg["Mean_yield"] = agg.Total_dry_yield / agg.Area_applied
        #     # return agg
        if "Till_rate" in df.columns:
            agg["Mean_till_coverage"] = agg.Total_area_tilled / agg.Area_applied

        if "Total_fuel" in df.columns:
            agg["Gal_per_acre"] = agg.Total_fuel / agg.Area_applied

        if "RATE_N" in df.columns:
            # return agg
            agg["N_efficiency"] = agg.RATE_N / agg.Mean_yield

    agg = agg.drop_duplicates()
    agg = agg.sort_values(by="Field_name", ignore_index=True)

    return agg
