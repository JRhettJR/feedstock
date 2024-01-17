import itertools
import os
import pathlib

import chardet
import pandas as pd

from ...data_prep.constants import (
    CORN,
    DA_SMS,
    NOT_FOUND,
    SMS_FERTILISER_RAW,
    SMS_HARVEST_RAW,
    SMS_PLANTING_RAW,
)
from ...util.cleaners.general import unify_cols

path_to_data = "01_data/"
path_to_dest = "02_analysis/"
grower = "Osvog"
# data_aggregator = DA_SMS
# GROWERS = ['Liebsch', 'Wilkinson', 'Aughenbaugh']
# growing_cycle = 2022


def get_encoding(path: pathlib.Path) -> str:
    with open(path, "rb") as rawdata:
        result = chardet.detect(rawdata.read(50000))

    return result["encoding"]


def read_pbp_files_by_type(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int, file_type: str
) -> pd.DataFrame:
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower, file_type + "_" + str(growing_cycle))
        .glob("*.csv")
    )

    path_csv = next(path, NOT_FOUND)

    if path_csv == NOT_FOUND:
        print(
            f"no LDB {file_type} file at {path_to_data} for grower {grower} for cycle {growing_cycle}"
        )
        return pd.DataFrame()

    else:
        path = itertools.chain([path_csv], path)
        df = pd.DataFrame()

        for path_csv in path:
            # print(path_csv)
            encoding = get_encoding(path_csv)
            # print(encoding)
            temp = pd.read_csv(path_csv, encoding=encoding)
            # add field name
            if file_type == SMS_PLANTING_RAW:
                temp["Field_name"] = path_csv.stem

            df = pd.concat([df, temp])

    return df


def get_aggregated_params_harvest(sms_pbp_file: pd.DataFrame) -> pd.DataFrame:
    d = {
        "Field_name": [],
        "Operation_start": [],
        "Operation_end": [],
        "Product": [],
        "Crop_type": [],
        "Total_yield": [],
        "Total_moisture": [],
        "Yield_bu_ac": [],
        "Acreage": [],
    }
    # add metrics
    sms_pbp_file["Total_yield"] = (
        sms_pbp_file["Yld_vol(dry)(bu_ac)"]
        * sms_pbp_file["Prod(ac_h)"]
        / 3600
        * sms_pbp_file["Duration(s)"]
    )
    sms_pbp_file["Moisture"] = (
        sms_pbp_file["Moisture(%)"] / 100 * sms_pbp_file["Yld_vol(dry)(bu_ac)"]
    )
    sms_pbp_file["Yield_bu_ac_s"] = (
        sms_pbp_file["Yld_vol(dry)(bu_ac)"] / sms_pbp_file["Duration(s)"]
    )

    # by field and product
    for field in sms_pbp_file.Field_name.unique():
        temp_f = sms_pbp_file[sms_pbp_file.Field_name.isin([field])]

        for prod in temp_f.Product.unique():
            temp = temp_f[(temp_f.Product.isin([prod]))]
            if not temp.empty:
                d["Field_name"].append(field)
                d["Product"].append(prod)
                d["Operation_start"].append(temp.Operation_start.min())
                d["Operation_end"].append(temp.Operation_start.max())

                d["Crop_type"].append(CORN)

                d["Total_yield"].append(temp.Total_yield.sum())
                d["Total_moisture"].append(
                    temp.Moisture.sum() / temp["Yld_vol(dry)(bu_ac)"].sum()
                )

                yield_bu_ac = temp.Yield_bu_ac_s.sum() / temp["Duration(s)"].sum()
                d["Yield_bu_ac"].append(yield_bu_ac)
                d["Acreage"].append(temp.Total_yield.sum() / yield_bu_ac)

    return pd.DataFrame(d)


def create_SMS_harvest_file(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    sms_pbp_file = read_pbp_files_by_type(
        path_to_data, grower, growing_cycle, file_type=SMS_HARVEST_RAW
    )
    if sms_pbp_file.empty:
        return pd.DataFrame()
    sms_pbp_file = unify_cols(sms_pbp_file)

    harvest = get_aggregated_params_harvest(sms_pbp_file)
    # return harvest
    path_to_data = pathlib.Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_data):
        os.makedirs(path_to_data)

    data_aggregator = DA_SMS

    harvest.to_csv(
        path_to_data.joinpath(
            grower + "_" + data_aggregator + "_harvest_" + str(growing_cycle) + ".csv"
        ),
        # float_format = "%.10f",
        index=False,
    )

    return harvest


def get_aggregated_params_planting(sms_pbp_file: pd.DataFrame) -> pd.DataFrame:
    d = {
        "Field_name": [],
        "Operation_start": [],
        "Operation_end": [],
        "Crop_type": [],
        # 'Product': list(),
        "Applied_total": [],
        "Applied_unit": [],
        "Area_applied": [],
    }

    for field in sms_pbp_file.Field_name.unique():
        d["Field_name"].append(field)

        temp = sms_pbp_file[sms_pbp_file.Field_name.isin([field])]
        d["Operation_start"].append(temp.Time.min())
        d["Operation_end"].append(temp.Time.max())

        d["Crop_type"].append(CORN)

        d["Applied_total"].append(temp["Seed_cnt((1))"].sum())
        d["Applied_unit"].append("Seeds")

        d["Area_applied"].append(None)

    return pd.DataFrame(d)


def create_SMS_planting_file(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    sms_pbp_file = read_pbp_files_by_type(
        path_to_data, grower, growing_cycle, file_type=SMS_PLANTING_RAW
    )
    if sms_pbp_file.empty:
        return pd.DataFrame()

    sms_pbp_file = unify_cols(sms_pbp_file)

    planting = get_aggregated_params_planting(sms_pbp_file)

    path_to_data = pathlib.Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_data):
        os.makedirs(path_to_data)

    planting.to_csv(
        path_to_data.joinpath(
            grower + "_" + DA_SMS + "_planting_" + str(growing_cycle) + ".csv"
        ),
        # float_format = "%.10f",
        index=False,
    )

    return planting


def get_aggregated_params_fertiliser(sms_pbp_file: pd.DataFrame) -> pd.DataFrame:
    d = {
        "Field_name": [],
        "Operation_start": [],
        "Operation_end": [],
        "Crop_type": [],
        "Product": [],
        "Applied_total": [],
        "Applied_unit": [],
        "Area_applied": [],
        "Total_fuel": [],
        "Fuel_unit": [],
    }
    # add metrics
    sms_pbp_file["Applied_total"] = (
        sms_pbp_file["Rt_apd_ms(lb_ac)"]
        * sms_pbp_file["Prod(ac_h)"]
        / 3600
        * sms_pbp_file["Duration(s)"]
    )
    sms_pbp_file["Total_fuel"] = (
        sms_pbp_file["Fuel_con(a)(gal(us)_ac)"]
        * sms_pbp_file["Prod(ac_h)"]
        / 3600
        * sms_pbp_file["Duration(s)"]
    )

    # sms_pbp_file['Acres_applied'] = 1 / sms_pbp_file['Duration(s)'] * sms_pbp_file['Prod(ac_h)'] / 3600

    for field in sms_pbp_file.Field_name.unique():
        temp = sms_pbp_file[sms_pbp_file.Field_name.isin([field])]

        for prod in temp.Product.unique():
            temp_p = temp[temp.Product.isin([prod])]

            d["Field_name"].append(field)

            d["Operation_start"].append(temp_p.Operation_start.min())
            d["Operation_end"].append(temp_p.Operation_start.max())

            d["Crop_type"].append(CORN)
            d["Product"].append(prod)

            d["Area_applied"].append(None)
            # d['Acres_applied'].append(temp_p.Acres_applied.sum())
            # d['Acres_applied'].append(temp_p[temp_p['Rt_apd_ms(lb_ac)'] != 0].Acres_applied.sum())

            d["Applied_total"].append(temp_p.Applied_total.sum())
            d["Applied_unit"].append("LBS")

            d["Total_fuel"].append(temp_p.Total_fuel.sum())
            d["Fuel_unit"].append("GAL")

    return pd.DataFrame(d)


def create_SMS_application_file(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    sms_pbp_file = read_pbp_files_by_type(
        path_to_data, grower, growing_cycle, file_type=SMS_FERTILISER_RAW
    )
    if sms_pbp_file.empty:
        return pd.DataFrame()
    sms_pbp_file = unify_cols(sms_pbp_file)

    application = get_aggregated_params_fertiliser(sms_pbp_file)

    path_to_data = pathlib.Path(path_to_data).joinpath(grower)
    if not os.path.exists(path_to_data):
        os.makedirs(path_to_data)

    application.to_csv(
        path_to_data.joinpath(
            grower + "_" + DA_SMS + "_application_" + str(growing_cycle) + ".csv"
        ),
        # float_format = "%.10f",
        index=False,
    )

    return application
