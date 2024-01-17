import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ... import general as gen
from ...data_prep.constants import (
    APPLICATION_COLUMNS,
    BASE_COLUMNS,
    CFV_APPLICATION,
    CFV_FILE_TYPES,
    CFV_HARVEST,
    CFV_PLANTING,
    DA_CFV,
    DA_FM,
    DA_GRANULAR,
    DA_JDOPS,
    DA_LDB,
    DA_PAP,
    DA_SMS,
    FM_APPLICATION,
    FM_FILE_TYPES,
    FM_HARVEST,
    FM_TILLAGE,
    FUEL_COLUMNS,
    GRAN_APPLICATION,
    GRAN_FILE_TYPES,
    GRAN_GENERATED,
    GRAN_HARVEST,
    GRAN_PLANTING,
    GRAN_TILLAGE,
    HARVEST_COLUMNS,
    JD_APPLICATION,
    JD_FILE_TYPES,
    JD_FUEL,
    JD_HARVEST,
    JD_PLANTING,
    JD_TILLAGE,
    LDB_APPLICATION,
    LDB_FILE_TYPES,
    LDB_GENERATED,
    LDB_HARVEST,
    LDB_PLANTING,
    LDB_TILLAGE,
    PAP_APPLICATION,
    PAP_FILE_TYPES,
    PAP_HARVEST,
    PLANTING_COLUMNS,
    SMS_APPLICATION,
    SMS_FILE_TYPES,
    SMS_HARVEST,
    SMS_PLANTING,
    TILLAGE_COLUMNS,
)
from ...general import unify_cols
from ..readers.general import read_file_by_file_type
from .helpers import (
    PLANTING_UNITS_RAW,
    add_missing_columns,
    clean_Granular_crop_type_in_harvest,
    clean_units,
    convert_quantity_by_unit,
    filter_Granular_apps,
    generate_Granular_sub_crop_type_in_harvest,
    seeding_planting_params,
)


def supplement_Granular_client_and_farm(apps, path_to_data, grower, growing_cycle):
    """Granular application file does not have `Client` and `Farm_name` information. These columns
    will be supplemented from the harvesting data
    """
    if apps.empty:
        return pd.DataFrame()

    rel_cols = ["Client", "Farm_name", "Field_name"]
    harvest = read_file_by_file_type(
        path_to_data,
        grower,
        growing_cycle,
        data_aggregator=DA_GRANULAR,
        file_type=GRAN_HARVEST,
    )
    if not harvest.empty:
        harvest = clean_file_by_file_type(
            harvest,
            path_to_data,
            grower,
            growing_cycle,
            file_type=GRAN_HARVEST,
            data_aggregator=DA_GRANULAR,
        )
    else:
        harvest = pd.DataFrame(columns=rel_cols)

    harvest = harvest[rel_cols].drop_duplicates()

    # apps = read_file_by_file_type(path_to_data, grower, growing_cycle, file_type=GRAN_APPLICATION)
    apps = apps.drop(columns=["Client", "Farm_name"])

    return pd.merge(harvest, apps, on="Field_name", how="right")


# %% [markdown]
# ### Climate FieldView (CFV)


# %%
def clean_CFV_unit(unit):
    return unit.split("/")[0]


def add_CFV_farm_name_to_apps(apps, path_to_data, grower, growing_cycle):
    rel_cols = ["Client", "Farm_name", "Field_name"]
    # farm name can be found in planting and/or harvesting data
    seed = get_cleaned_file_by_file_type(
        path_to_data,
        grower,
        growing_cycle,
        data_aggregator=DA_CFV,
        file_type=CFV_PLANTING,
    )
    harvest = get_cleaned_file_by_file_type(
        path_to_data,
        grower,
        growing_cycle,
        data_aggregator=DA_CFV,
        file_type=CFV_HARVEST,
    )

    if seed.empty and harvest.empty:
        log.warning(
            f"unable to add farm name to apps file for grower {grower} and system CFV"
        )
        df = pd.DataFrame(columns=rel_cols)
    #         harvest = get_cleaned_file_by_file_type(path_to_data, grower, growing_cycle, data_aggregator=DA_CFV, file_type=CFV_HARVEST)

    #         if harvest.empty:
    #             print(f'unable to farm name for grower {grower} and system CFV')
    #             df = pd.DataFrame(columns=rel_cols)
    #         else:
    #             df = harvest
    else:
        df = pd.concat([seed, harvest])

    df = df[rel_cols].drop_duplicates()

    apps = apps.drop(columns=["Client", "Farm_name"])

    return pd.merge(df, apps, on="Field_name", how="right")


# %% [markdown]
# ### Land.db (LDB)


# %%
def clean_LDB_crop_type(crop_type):
    if not isinstance(crop_type, str):
        return crop_type

    temp = crop_type.split(":")[0]
    return temp.strip()


def supplement_LDB_harvest_data(
    harvest, path_to_data, grower, growing_cycle, data_aggregator, verbose=True
):
    apps = read_file_by_file_type(
        path_to_data,
        grower,
        growing_cycle,
        data_aggregator,
        file_type=LDB_APPLICATION,
        verbose=verbose,
    )
    # in case no LDB application data is available, no supplementation will be done.
    # this probably is mostly a cosmetic to make `get_all_available_field_names()` work
    # (see below).
    if apps.empty:
        return harvest

    apps = clean_file_by_file_type(
        apps,
        path_to_data,
        grower,
        growing_cycle,
        file_type=LDB_APPLICATION,
        data_aggregator=DA_LDB,
        verbose=verbose,
    )

    harvest_from_apps = apps[(apps.Product.isin(["Harvesting"]))]
    if harvest.empty:
        return pd.concat([harvest, harvest_from_apps])

    temp = pd.merge(
        harvest.drop(columns=["Operation_start", "Operation_end"]),
        # [['Farm_name', 'Field_name', 'Operation_start', 'Operation_end']],
        harvest_from_apps,
        on=["Farm_name", "Field_name"],
        how="outer",
    )

    return temp


def filter_LDB_apps(apps, path_to_data, grower, growing_cycle, verbose=True):
    """the filtered apps does not include planting data as contained in the internally generated planting file"""
    data_aggregator = DA_LDB
    temp = apps

    product_mapping = gen.read_product_mapping_file(path_to_data)
    temp.Product = temp.apply(
        lambda x: gen.map_clear_name(product_mapping, x.Product), axis=1
    )

    field_mapping = gen.read_field_name_mapping(path_to_data, grower)
    temp.Field_name = temp.apply(
        lambda x: gen.map_clear_name_using_farm_name(
            field_mapping, x.Field_name, x.Farm_name
        ),
        axis=1,
    )

    for file_type in LDB_GENERATED:
        df = read_file_by_file_type(
            path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
        )

        cols_to_drop = [col for col in df.columns if col not in temp.columns]
        df = df.drop(columns=cols_to_drop)

        df.Reg_number = df.Reg_number.astype(object)

        if df.empty:
            if verbose:
                log.warning(
                    f"missing {file_type} file for system {data_aggregator} for grower {grower} and cycle {growing_cycle}. Application file will contain planting info"
                )
        else:
            # avoid duplicate columns / Operation_type naturally differs across the files to merge
            df = df.drop(columns="Operation_type")
            # return pd.merge(temp, df, on=cols, indicator=True, how='outer').query('_merge=="left_only"')
            temp = (
                pd.merge(
                    temp,
                    df,
                    # on=cols,
                    indicator=True,
                    how="outer",
                )
                .query('_merge=="left_only"')
                .drop("_merge", axis=1)
            )

    # exclude harvesting operations
    temp = temp[~temp.Product.isin(["Harvesting"])]
    temp = temp.reset_index(drop=True)
    return temp


def clean_JDOps_file(df, file_type):
    temp = df

    if file_type == JD_HARVEST:
        temp = temp.rename(columns={"Unit_0": "Area_unit", "Unit_3": "Applied_unit"})

        temp.Total_dry_yield = temp.Total_dry_yield.apply(gen.clean_numeric_col)
        temp.Yield = temp.Yield.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)
        temp["Total_dry_yield_check"] = temp.Yield * temp.Area_applied

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = temp.dropna(
            subset=["Area_applied", "Total_dry_yield"]
        )  # eliminate phantom records

        temp["Operation_type"] = "Harvest"
        temp = add_missing_columns(temp, HARVEST_COLUMNS)
        temp = temp[HARVEST_COLUMNS]

    if file_type == JD_TILLAGE:
        temp = temp.rename(columns={"Unit_0": "Area_unit", "Unit_1": "Applied_unit"})

        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        temp["Applied_total"] = np.nan
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp["Operation_type"] = "Tillage"

        temp = add_missing_columns(temp, TILLAGE_COLUMNS)

        temp = temp[TILLAGE_COLUMNS]

    if file_type == JD_PLANTING:
        temp = temp.rename(
            columns={
                "Unit_0": "Area_unit",
                "Unit_1": "Rate_unit",
                "Unit_2": "Applied_unit",
            }
        )

        temp.Applied_total = temp.Applied_total.apply(gen.clean_numeric_col)
        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        # convert seed related units to BAG Or AC (required for planting file to feed into seeded area)
        cleaned_apps_params = temp.apply(
            lambda x: convert_quantity_by_unit(
                x.Applied_total,
                x.Applied_unit,
                params_to_convert=seeding_planting_params,
            ),
            axis=1,
            result_type="expand",
        )
        # index 0 refers to quantity / index 2 refers to unit
        temp.Applied_total, temp.Applied_unit = (
            cleaned_apps_params[0],
            cleaned_apps_params[1],
        )

        temp["Operation_type"] = "Planting"
        temp = add_missing_columns(temp, PLANTING_COLUMNS)
        temp = temp[PLANTING_COLUMNS]

    if file_type == JD_APPLICATION:
        temp = temp.rename(
            columns={
                "Unit_0": "Area_unit",
                "Unit_1": "Rate_unit",
                "Unit_2": "Applied_unit",
            }
        )
        # return temp
        temp.Applied_total = temp.Applied_total.apply(gen.clean_numeric_col)
        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp["Operation_type"] = "Application"
        temp = add_missing_columns(temp, APPLICATION_COLUMNS)
        temp = temp[APPLICATION_COLUMNS]

    if file_type == JD_FUEL:
        temp.Total_fuel = temp.Total_fuel.apply(gen.clean_numeric_col)
        temp = temp.rename(columns={"Task_type": "Operation_type"})
        # drop columns to avoid double counting of inputs in comprehensive df (cleaned file)
        temp = temp.drop(columns=["Product", "Applied_total", "Applied_unit"])

        # convert fuel to gallons
        cleaned_fuel_params = temp.apply(
            lambda x: convert_quantity_by_unit(x.Total_fuel, x.Fuel_unit),
            axis=1,
            result_type="expand",
        )
        # index 0 refers to quantity / index 2 refers to unit
        temp.Total_fuel, temp.Fuel_unit = cleaned_fuel_params[0], cleaned_fuel_params[1]

        temp = add_missing_columns(temp, FUEL_COLUMNS)
        temp = temp[FUEL_COLUMNS]

    return temp


# %% [markdown]
# ### Granular


# %%
def clean_Granular_file(df, file_type):
    temp = df

    if file_type == GRAN_HARVEST:
        temp.Total_dry_yield = temp.Total_dry_yield.apply(gen.clean_numeric_col)
        temp.Yield = temp.Yield.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)
        temp["Total_dry_yield_check"] = temp.Yield * temp.Area_applied

        # clean `Crop_type`
        temp["Sub_crop_type"] = temp.Crop_type.apply(
            generate_Granular_sub_crop_type_in_harvest
        )
        temp["Crop_type"] = temp.Crop_type.apply(clean_Granular_crop_type_in_harvest)
        temp["Operation_type"] = "Harvest"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, HARVEST_COLUMNS)

        temp = temp[HARVEST_COLUMNS]

    if file_type == GRAN_APPLICATION:
        # split-up OPS_DATES
        temp["Operation_start"] = temp.Ops_dates.apply(gen.parse_start_date)
        temp["Operation_end"] = temp.Ops_dates.apply(gen.parse_end_date)

        # split-up units and inputs
        temp["Applied_unit"] = temp.Applied_total.apply(gen.split_applied_unit)
        temp["Applied_total"] = temp.Applied_total.apply(gen.split_applied_val)

        temp["Area_unit"] = temp.Area_applied.apply(gen.split_applied_unit)
        temp["Area_applied"] = temp.Area_applied.apply(gen.split_applied_val)

        temp["Applied_rate"] = temp.Applied_rate.apply(gen.split_applied_val)

        # clean `Crop_type`, i.e. removing 'Commercial' from denomination. This makes this table
        # comparable to the internally generated files for TILLAGE and PLANTING.
        temp["Crop_type"] = temp.Crop_type.apply(
            lambda ct: clean_Granular_crop_type_in_harvest(ct)
        )

        temp.Applied_total = temp.Applied_total.apply(gen.clean_numeric_col)
        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp.Applied_rate = temp.Applied_total / temp.Area_applied

        temp = add_missing_columns(temp, APPLICATION_COLUMNS)

        # Added for duplicate elimantion mechanism in `filter_Granular_apps()`
        # in `src/feedstock_aggregation_scripts/util/cleaners/helpers.py`
        temp["Manufacturer"] = temp["Manufacturer"].astype(object)

        temp = temp[APPLICATION_COLUMNS]

    if file_type == GRAN_PLANTING:
        # convert seed related units to BAG
        cleaned_apps_params = temp.apply(
            lambda x: convert_quantity_by_unit(
                x.Applied_total, x.Applied_unit, params_to_convert=PLANTING_UNITS_RAW
            ),
            axis=1,
            result_type="expand",
        )
        # index 0 refers to quantity / index 2 refers to unit
        temp.Applied_total, temp.Applied_unit = (
            cleaned_apps_params[0],
            cleaned_apps_params[1],
        )

        temp = add_missing_columns(temp, PLANTING_COLUMNS)
        temp = temp[PLANTING_COLUMNS]

    if file_type == GRAN_TILLAGE:
        temp = add_missing_columns(temp, TILLAGE_COLUMNS)
        temp = temp[TILLAGE_COLUMNS]

    return temp


# %%
# u = get_cleaned_file_by_file_type(path_to_data, grower, growing_cycle, data_aggregator, file_type=GRAN_TILLAGE)
# u.Applied_unit.unique()
# u
# u[u.Applied_unit == 'ton']

# %% [markdown]
# ### Climate FieldView (CFV)


def clean_CFV_file(df, file_type):
    temp = gen.unify_cols(df)

    if file_type == CFV_HARVEST:
        temp.Total_dry_yield = temp.Total_dry_yield.apply(gen.clean_numeric_col)
        temp.Moisture = temp.Moisture.apply(gen.clean_numeric_col)
        temp["Total_dry_yield_check"] = temp.Yield * temp.Area_applied
        temp["Applied_unit"] = "bu"
        temp["Operation_type"] = "Harvest"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, HARVEST_COLUMNS)
        temp = temp[HARVEST_COLUMNS]

    if file_type == CFV_PLANTING:
        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        temp["Applied_total"] = temp.Area_applied * temp.Applied_rate
        temp["Applied_unit"] = "seeds"
        temp["Operation_type"] = "Planting"

        # convert seed related units to BAG
        cleaned_apps_params = temp.apply(
            lambda x: convert_quantity_by_unit(
                x.Applied_total, x.Applied_unit, params_to_convert=PLANTING_UNITS_RAW
            ),
            axis=1,
            result_type="expand",
        )
        # index 0 refers to quantity / index 2 refers to unit
        temp.Applied_total, temp.Applied_unit = (
            cleaned_apps_params[0],
            cleaned_apps_params[1],
        )

        temp = add_missing_columns(temp, PLANTING_COLUMNS)
        temp = temp[PLANTING_COLUMNS]

    if file_type == CFV_APPLICATION:
        print(temp)
        temp.Applied_unit = temp.Applied_unit.apply(clean_CFV_unit)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)
        temp["Applied_total"] = temp.Applied_rate * temp.Area_applied
        temp["Operation_type"] = "Application"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, APPLICATION_COLUMNS)
        temp = temp[APPLICATION_COLUMNS]

        # temp = temp.sort_values(by='Field_name', ascending=True, ignore_index=True)

    return temp


# %% [markdown]
# ### Prairie Ag Partners (PAP)


# %%
def aggregate_PAP_scale_tickets(data):
    # create total yield
    total_yield = data.groupby(
        by=["Client", "Farm_name", "Field_name"], as_index=False
    ).sum()
    total_yield = total_yield.rename(columns={"Yield": "Total_dry_yield"})
    temp = pd.merge(
        data,
        total_yield[["Client", "Farm_name", "Field_name", "Total_dry_yield"]],
        on=["Client", "Farm_name", "Field_name"],
        how="left",
    )

    # calculate average moisture
    temp["Moisture_fraction"] = temp.Moisture * temp.Yield / temp.Total_dry_yield
    avg_moisture = temp.groupby(
        by=["Client", "Farm_name", "Field_name"], as_index=False
    ).sum()
    avg_moisture.Moisture = avg_moisture.Moisture_fraction

    temp = pd.merge(
        total_yield[["Client", "Farm_name", "Field_name", "Total_dry_yield"]],
        avg_moisture[["Client", "Farm_name", "Field_name", "Moisture"]],
        on=["Client", "Farm_name", "Field_name"],
        how="outer",
    )

    crop_operation_start = data.drop_duplicates(
        subset=["Client", "Farm_name", "Field_name", "Operation_start", "Crop_type"],
        ignore_index=True,
    )

    temp = pd.merge(
        temp,
        crop_operation_start[
            ["Client", "Farm_name", "Field_name", "Operation_start", "Crop_type"]
        ],
        on=["Client", "Farm_name", "Field_name"],
        how="left",
    )

    return temp


def clean_PAP_file(df, file_type):
    temp = gen.unify_cols(df)

    if file_type == PAP_HARVEST:
        temp.Yield = temp.Yield.apply(gen.clean_numeric_col)
        temp = aggregate_PAP_scale_tickets(temp)
        # temp['Total_dry_yield_check'] = temp.Yield * temp.Area_applied
        temp["Applied_unit"] = "bu"
        temp["Operation_type"] = "Harvest"

        temp = add_missing_columns(temp, HARVEST_COLUMNS)
        temp = temp[HARVEST_COLUMNS]

    if file_type == PAP_APPLICATION:
        temp.Applied_total = temp.Applied_total.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp.Applied_rate = temp.Applied_total / temp.Area_applied

        temp["Applied_total"] = temp.Applied_rate * temp.Area_applied
        temp["Operation_type"] = "Application"

        temp = add_missing_columns(temp, APPLICATION_COLUMNS)
        temp = temp[APPLICATION_COLUMNS]

    return temp


# %% [markdown]
# ### Land.db (LDB)


# %%
def clean_LDB_file(df, file_type):
    temp = df

    if file_type == LDB_HARVEST:
        temp = add_missing_columns(temp, HARVEST_COLUMNS)

        temp.Total_dry_yield = temp.Total_dry_yield.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        # clean CROP_TYPE
        temp["Crop_type"] = temp.Crop_type.apply(clean_LDB_crop_type)
        temp["Operation_type"] = "Harvest"
        temp = temp[HARVEST_COLUMNS]

    if file_type == LDB_APPLICATION:
        # clean up crop type
        temp["Crop_type"] = temp.Crop_type.apply(clean_LDB_crop_type)

        temp.Applied_total = temp.Applied_total.apply(gen.clean_numeric_col)
        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        # return temp
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)
        # need to convert seed applications
        cleaned_apps_params = temp.apply(
            lambda x: convert_quantity_by_unit(
                x.Applied_total, x.Applied_unit, params_to_convert=PLANTING_UNITS_RAW
            ),
            axis=1,
            result_type="expand",
        )
        # index 0 refers to quantity / index 2 refers to unit
        temp.Applied_total, temp.Applied_unit = (
            cleaned_apps_params[0],
            cleaned_apps_params[1],
        )

        temp.Applied_rate = temp.Applied_total / temp.Area_applied

        if "Operation_type" not in temp.columns:
            temp["Operation_type"] = "Application"

        temp = add_missing_columns(temp, APPLICATION_COLUMNS)
        temp = temp[APPLICATION_COLUMNS]

    if file_type == LDB_PLANTING:
        # convert seed related units to BAG
        cleaned_apps_params = temp.apply(
            lambda x: convert_quantity_by_unit(
                x.Applied_total, x.Applied_unit, params_to_convert=PLANTING_UNITS_RAW
            ),
            axis=1,
            result_type="expand",
        )
        # index 0 refers to quantity / index 2 refers to unit
        temp.Applied_total, temp.Applied_unit = (
            cleaned_apps_params[0],
            cleaned_apps_params[1],
        )

        temp = add_missing_columns(temp, PLANTING_COLUMNS)
        temp = temp[PLANTING_COLUMNS]

    if file_type == LDB_TILLAGE:
        temp = add_missing_columns(temp, TILLAGE_COLUMNS)
        temp = temp[TILLAGE_COLUMNS]

    return temp


# %% [markdown]
# ### FarMobile (FM)
def clean_FM_file(df, file_type):
    temp = gen.unify_cols(df)

    if file_type == FM_HARVEST:
        temp.Yield = temp.Yield.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)
        temp.Total_dry_yield = temp.Total_dry_yield.apply(gen.clean_numeric_col)

        temp["Total_dry_yield_check"] = temp.Yield * temp.Area_applied
        temp["Applied_unit"] = "bu"

        temp["Operation_type"] = "Harvest"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, HARVEST_COLUMNS)
        temp = temp[HARVEST_COLUMNS]

    if file_type == FM_TILLAGE:
        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        temp["Applied_total"] = np.nan

        temp["Operation_type"] = "Tillage"
        temp["Applied_unit"] = "in"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, TILLAGE_COLUMNS)
        temp = temp[TILLAGE_COLUMNS]

    if file_type == FM_APPLICATION:
        temp.Applied_rate = temp.Applied_rate.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)

        temp["Applied_total"] = temp.Applied_rate * temp.Area_applied
        temp["Applied_unit"] = "GAL"

        temp["Operation_type"] = "Application"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, APPLICATION_COLUMNS)
        temp = temp[APPLICATION_COLUMNS]

    temp = temp.sort_values(
        by=["Farm_name", "Field_name"], ascending=True, ignore_index=True
    )

    temp.Operation_start = pd.to_datetime(temp.Operation_start)
    temp.Operation_end = pd.to_datetime(temp.Operation_end)

    return temp


# %% [markdown]
# ### SMS Ag Leader (SMS)
def clean_SMS_file(df, file_type):
    temp = gen.unify_cols(df)

    if file_type == SMS_HARVEST:
        temp.Yield = temp.Yield.apply(gen.clean_numeric_col)
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)
        temp.Total_dry_yield = temp.Total_dry_yield.apply(gen.clean_numeric_col)

        temp["Total_dry_yield_check"] = temp.Yield * temp.Area_applied
        temp["Applied_unit"] = "bu"

        temp["Operation_type"] = "Harvest"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, HARVEST_COLUMNS)
        temp = temp[HARVEST_COLUMNS]

    if file_type == SMS_PLANTING:
        temp.Area_applied = temp.Area_applied.apply(gen.clean_numeric_col)
        temp.Applied_total = temp.Applied_total.apply(gen.clean_numeric_col)

        temp["Applied_rate"] = temp.Applied_total / temp.Area_applied
        temp["Operation_type"] = "Planting"

        # convert seed related units to BAG
        cleaned_apps_params = temp.apply(
            lambda x: convert_quantity_by_unit(
                x.Applied_total, x.Applied_unit, params_to_convert=PLANTING_UNITS_RAW
            ),
            axis=1,
            result_type="expand",
        )
        # index 0 refers to quantity / index 2 refers to unit
        temp.Applied_total, temp.Applied_unit = (
            cleaned_apps_params[0],
            cleaned_apps_params[1],
        )

        temp = add_missing_columns(temp, PLANTING_COLUMNS)
        temp = temp[PLANTING_COLUMNS]

    if file_type == SMS_APPLICATION:
        temp.Applied_total = temp.Applied_total.apply(gen.clean_numeric_col)
        temp["Operation_type"] = "Application"

        # convert units to backend accepted units
        temp.Applied_unit = temp.Applied_unit.apply(clean_units)

        temp = add_missing_columns(temp, APPLICATION_COLUMNS)
        temp = temp[APPLICATION_COLUMNS]

    temp = temp.sort_values(
        by=["Farm_name", "Field_name"], ascending=True, ignore_index=True
    )

    temp.Operation_start = pd.to_datetime(temp.Operation_start, format="mixed")
    temp.Operation_end = pd.to_datetime(temp.Operation_end, format="mixed")

    return temp


def clean_file_by_file_type(
    df, path_to_data, grower, growing_cycle, file_type, data_aggregator, verbose=True
):
    temp = unify_cols(df)

    temp = add_missing_columns(temp, BASE_COLUMNS)

    if file_type in JD_FILE_TYPES and data_aggregator == DA_JDOPS:
        temp = clean_JDOps_file(temp, file_type)

    if (
        file_type in [*GRAN_FILE_TYPES, *GRAN_GENERATED]
        and data_aggregator == DA_GRANULAR
    ):
        # For Shawn Feikema (GP22) we observe identical harvesting operations on the same `Field_name`
        # but with different `Farm_name` attributes.
        #
        # ASSUMPTION: those operations happened on the same field
        #
        # SOLUTION: to avoid double entries in subsequent merging operations, we exclude those duplicate entries
        # and keep those associated to the first occurence of `Farm_name`.
        if file_type == GRAN_HARVEST:
            temp = temp.drop_duplicates(subset=temp.columns.difference(["Farm_name"]))
        temp = clean_Granular_file(temp, file_type)

    if file_type in CFV_FILE_TYPES and data_aggregator == DA_CFV:
        temp = clean_CFV_file(temp, file_type)

    if file_type in PAP_FILE_TYPES and data_aggregator == DA_PAP:
        temp = clean_PAP_file(temp, file_type)

    if file_type in [*LDB_FILE_TYPES, *LDB_GENERATED] and data_aggregator == DA_LDB:
        # added `LDB_GENERATED` file types to clean units of `LDB_PLANTING`
        # TODO: Needs to be confirmed? This is causing issues and we believe it's unnecessary at this time
        # if file_type == LDB_HARVEST:
        #     temp = supplement_LDB_harvest_data(
        #         temp, path_to_data, grower, growing_cycle, data_aggregator, verbose
        #     )
        temp = clean_LDB_file(temp, file_type)

    if file_type in FM_FILE_TYPES and data_aggregator == DA_FM:
        temp = clean_FM_file(temp, file_type)

    if file_type in SMS_FILE_TYPES and data_aggregator == DA_SMS:
        temp = clean_SMS_file(temp, file_type)

    temp = temp.sort_values(
        by=["Farm_name", "Field_name"], ascending=True, ignore_index=True
    )

    for col in ["Operation_start", "Operation_end"]:
        temp[col] = pd.to_datetime(temp[col], format="mixed")

    return temp


# %%
def get_cleaned_file_by_file_type(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    file_type: str,
    verbose: bool = True,
) -> pd.DataFrame:
    file = read_file_by_file_type(
        path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
    )

    if not file.empty or file_type == LDB_HARVEST:
        file = clean_file_by_file_type(
            file,
            path_to_data,
            grower,
            growing_cycle,
            file_type,
            data_aggregator,
            verbose,
        )
        if file_type == GRAN_APPLICATION:
            file = supplement_Granular_client_and_farm(
                file, path_to_data, grower, growing_cycle
            )
            # filter out planting operations
            file = filter_Granular_apps(file, path_to_data, grower, growing_cycle)

        if file_type == CFV_APPLICATION:
            file = add_CFV_farm_name_to_apps(file, path_to_data, grower, growing_cycle)

        if file_type == LDB_APPLICATION:
            file = filter_LDB_apps(file, path_to_data, grower, growing_cycle, verbose)

        # supplement Client name as grower where not given
        file.Client = file.Client.apply(
            lambda client: grower if pd.isnull(client) else client
        )
        return file

    return pd.DataFrame()
