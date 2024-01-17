import itertools
import os
from pathlib import Path

import pandas as pd
from data_aggregators.factory import AggregatorOperationFactory, DataAggregators
from loguru import logger as log

from ... import general as gen
from ...config import settings
from ...data_prep.constants import (
    APPLICATION,
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
    DATA_AGGREGATORS,
    FILE_CATEGORIES,
    FM_APPLICATION,
    FM_FILE_TYPES,
    FM_HARVEST,
    FM_PLANTING,
    GENERIC_REPORTS,
    GRAN_APPLICATION,
    GRAN_FILE_TYPES,
    GRAN_GENERATED,
    GRAN_HARVEST,
    GRAN_PLANTING,
    GRAN_TILLAGE,
    HARVEST,
    HARVEST_DATES,
    HARVEST_DATES_COLUMNS,
    JD_APPLICATION,
    JD_FILE_TYPES,
    JD_FUEL,
    JD_HARVEST,
    JD_PLANTING,
    LDB_APPLICATION,
    LDB_FILE_TYPES,
    LDB_GENERATED,
    LDB_HARVEST,
    LDB_PLANTING,
    LIME_REPORT,
    LIME_REPORT_COLUMNS,
    MANURE_REPORT,
    MANURE_REPORT_COLUMNS,
    NOT_FOUND,
    PAP_APPLICATION,
    PAP_FILE_TYPES,
    PAP_HARVEST,
    PLANTING,
    SMS_APPLICATION,
    SMS_FILE_TYPES,
    SMS_HARVEST,
    SMS_PLANTING,
    SPLIT_FIELD,
)

# Setting Google Project for GCS access
os.environ["GOOGLE_CLOUD_PROJECT"] = settings.gcs_dev.project_id
# Set bucket name for feedstock data
BUCKET_NAME = settings.gcs_dev.bucket_name


def get_columns_by_file_type(file_type: str) -> list[str]:
    if file_type == HARVEST_DATES:
        return HARVEST_DATES_COLUMNS

    elif file_type == LIME_REPORT:
        return LIME_REPORT_COLUMNS

    elif file_type == MANURE_REPORT:
        return MANURE_REPORT_COLUMNS

    else:
        return []


def read_JDOps_excel_export(
    path_to_data, grower, growing_cycle, file_type, verbose=True
):
    folder_path = f"{settings.bucket_folders.raw_data}/{grower}/{growing_cycle}"

    combined_jdops = AggregatorOperationFactory.process_file_by_type_in_gcs(
        DataAggregators.DA_JDOPS, BUCKET_NAME, folder_path, file_type
    )

    df = pd.DataFrame([model.model_dump(by_alias=True) for model in combined_jdops])

    # path = (
    #     Path(path_to_data)
    #     .joinpath(grower)
    #     .glob("*" + file_type + "_" + str(growing_cycle) + "*.xlsx")
    # )
    # path_xlsx = next(path, gen.NOT_FOUND)

    # if path_xlsx == gen.NOT_FOUND:
    #     path = (
    #         Path(path_to_data)
    #         .joinpath(grower)
    #         .glob("*" + file_type + "_" + str(growing_cycle) + "*.csv")
    #     )
    #     path_csv = next(path, gen.NOT_FOUND)

    #     if path_csv == gen.NOT_FOUND:
    #         if verbose:
    #             log.warning(
    #                 f"no {file_type} file from {growing_cycle} at {path_to_data} for grower {grower}"
    #             )
    #         return pd.DataFrame()
    #     else:
    #         df = pd.read_csv(path_csv, parse_dates=["Operation_start", "Operation_end"])

    # else:
    #     df = pd.read_excel(path_xlsx)

    df = df.rename(columns={"Unit": "Unit.0"})

    return df


# %% [markdown]
# ## Readers

# ### JDOps


def get_file_type_by_data_aggregator(
    data_aggregator: str, file_category: str
) -> str | bool:
    if data_aggregator not in DATA_AGGREGATORS:
        log.warning(f"unknown data aggregator: {data_aggregator}")
        return gen.NOT_FOUND

    if file_category not in FILE_CATEGORIES:
        log.warning(
            f"unknown file type {file_category} for data aggregator {data_aggregator}"
        )
        return gen.NOT_FOUND

    if file_category == APPLICATION:
        if data_aggregator == DA_JDOPS:
            file_type = JD_APPLICATION

        elif data_aggregator == DA_GRANULAR:
            file_type = GRAN_APPLICATION

        elif data_aggregator == DA_CFV:
            file_type = CFV_APPLICATION

        elif data_aggregator == DA_PAP:
            file_type = PAP_APPLICATION

        elif data_aggregator == DA_LDB:
            file_type = LDB_APPLICATION

        elif data_aggregator == DA_FM:
            file_type = FM_APPLICATION

        elif data_aggregator == DA_SMS:
            file_type = SMS_APPLICATION

        else:
            log.warning(
                f"missing implementation to get application data for system {data_aggregator}"
            )
            return NOT_FOUND

    elif file_category == HARVEST:
        if data_aggregator == DA_JDOPS:
            file_type = JD_HARVEST

        elif data_aggregator == DA_GRANULAR:
            file_type = GRAN_HARVEST

        elif data_aggregator == DA_CFV:
            file_type = CFV_HARVEST

        elif data_aggregator == DA_PAP:
            file_type = PAP_HARVEST

        elif data_aggregator == DA_LDB:
            file_type = LDB_HARVEST

        elif data_aggregator == DA_FM:
            file_type = FM_HARVEST

        elif data_aggregator == DA_SMS:
            file_type = SMS_HARVEST

        else:
            log.warning(
                f"missing implementation to get harvest data for system {data_aggregator}"
            )
            return NOT_FOUND

    elif file_category == PLANTING:
        if data_aggregator == DA_JDOPS:
            file_type = JD_PLANTING

        elif data_aggregator == DA_GRANULAR:
            file_type = GRAN_PLANTING

        elif data_aggregator == DA_CFV:
            file_type = CFV_PLANTING

        elif data_aggregator == DA_LDB:
            file_type = LDB_PLANTING

        elif data_aggregator == DA_FM:
            file_type = FM_PLANTING

        elif data_aggregator == DA_SMS:
            file_type = SMS_PLANTING

        else:
            log.warning(
                f"missing implementation to get planting data for system {data_aggregator}"
            )
            return NOT_FOUND

    else:
        log.warning(
            f"missing implementation to get {file_category} data for system {data_aggregator}"
        )
        return NOT_FOUND

    return file_type


# %% [markdown]
# ### Granular


def read_Granular_export(path_to_data, grower, growing_cycle, file_type, verbose=True):
    folder_path = f"{settings.bucket_folders.raw_data}/{grower}/{growing_cycle}"

    granular_export = AggregatorOperationFactory.process_file_by_type_in_gcs(
        DataAggregators.DA_GRANULAR, BUCKET_NAME, folder_path, file_type
    )

    df = pd.DataFrame([model.model_dump(by_alias=True) for model in granular_export])

    # path = (
    #     Path(path_to_data)
    #     .joinpath(grower)
    #     .glob("*" + file_type + "*_" + str(growing_cycle) + "*.csv")
    # )
    # path_csv = next(path, NOT_FOUND)

    # if path_csv == NOT_FOUND:
    #     if verbose:
    #         log.warning(
    #             f"no {file_type} file from {growing_cycle} at {path_to_data} for grower {grower}"
    #         )
    #     return pd.DataFrame()

    # df = pd.read_csv(path_csv)

    df = df.rename(columns={"Field Name": "Sub Field Name", "Field": "Sub Field Name"})

    return df


# %% [markdown]
# ### Climate FieldView (CFV)


def read_CFV_data(path_to_data, grower, growing_cycle, file_type, verbose):
    folder_path = f"{settings.bucket_folders.raw_data}/{grower}/{growing_cycle}"

    cfv = AggregatorOperationFactory.process_file_by_type_in_gcs(
        DataAggregators.DA_CFV, BUCKET_NAME, folder_path, file_type
    )

    results = pd.DataFrame([model.model_dump(by_alias=True) for model in cfv])

    # initialize variables
    # results = pd.DataFrame()
    # log.debug(f"Reading CFV files: {grower} {file_type} {growing_cycle}")
    # # find all documents to process
    # path = (
    #     Path(path_to_data)
    #     .joinpath(grower)
    #     .glob("*_" + file_type + "_*_" + str(growing_cycle) + "*")
    # )
    # files = next(path, NOT_FOUND)

    # if files == NOT_FOUND:
    #     log.info(
    #         f"No CFV {file_type} file at {path_to_data} for grower {grower} for cycle {growing_cycle}"
    #     )
    #     return results

    # path_files = itertools.chain([files], path)
    # for file in path_files:
    #     log.debug(f"Processing: {file}")
    #     file_extension = file.name.split(".")[-1]
    #     if file_extension == "csv":
    #         df = pd.read_csv(file)
    #     elif file_extension == "xls":
    #         df = pd.read_excel(file)
    #     else:
    #         log.error(f"Unable to read CFV {file_extension} files")

    #     if file_type not in CFV_APPLICATION:
    #         crop_type = file.stem.split("_")[-1]
    #         df["Crop_type"] = crop_type if crop_type in CROP_TYPES else np.nan
    #         df = df.rename(
    #             columns={
    #                 "Unnamed: 0": "Client",
    #                 "Unnamed: 1": "Farm_name",
    #             }
    #         )
    #     results = pd.concat([results, df])
    return results


# %% [markdown]
# ### Land.db (LDB)


# %%
def read_LDB_data(
    path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose=True
):
    path = (
        Path(path_to_data)
        .joinpath(grower)
        .glob(f"[A-Za-z0-9]*_{data_aggregator}_{str(growing_cycle)}.xls*")
    )
    path_xls = next(path, NOT_FOUND)

    if path_xls == NOT_FOUND:
        if verbose:
            log.warning(
                f"no LDB {file_type} file at {path_to_data} for grower {grower} for cycle {growing_cycle}"
            )
        return pd.DataFrame()

    else:
        path = itertools.chain([path_xls], path)
        df = pd.DataFrame()

        for path_xls in path:
            temp = pd.read_excel(
                path_xls,
                sheet_name=file_type,
                engine="openpyxl",
            )
            if file_type == LDB_HARVEST and temp.empty:
                # try a different sheet
                temp = pd.read_excel(
                    path_xls,
                    sheet_name="Yield Field To Storage",
                    engine="openpyxl",
                )

            df = pd.concat([df, temp])

    if file_type == LDB_HARVEST:
        # rename to avoid clash
        df = df.rename(columns={"Unit": "Unit.1"})
    if file_type == LDB_APPLICATION:
        df = df.drop(columns=["Crop Zone Area"])

    return df


# %% [markdown]
# ### FarmMobile (FM)


def read_FM_export(
    path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose=True
):
    folder_path = f"{settings.bucket_folders.raw_data}/{grower}/{growing_cycle}"

    cfv = AggregatorOperationFactory.process_file_by_type_in_gcs(
        DataAggregators.DA_FARMMOBILE, BUCKET_NAME, folder_path, file_type
    )

    df = pd.DataFrame([model.model_dump(by_alias=True) for model in cfv])

    # path = (
    #     Path(path_to_data)
    #     .joinpath(grower)
    #     .glob(f"*_{file_type}*_{str(growing_cycle)}_*.csv")
    # )

    # path = next(path, NOT_FOUND)

    # if path == NOT_FOUND:
    #     if verbose:
    #         log.warning(
    #             f"no {data_aggregator} {file_type} file from {growing_cycle} at {path_to_data} for grower {grower}"
    #         )
    #     return pd.DataFrame()

    # df = pd.read_csv(path)

    # df = df.rename(columns={'Unit': 'Unit.0'})

    return df


# %% [markdown]
# ### General


def read_csv_report(
    path_to_data: pd.DataFrame,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    file_type: str,
    verbose: bool = True,
) -> pd.DataFrame:
    path = (
        Path(path_to_data)
        .joinpath(grower)
        .glob(
            "*" + data_aggregator + "_" + file_type + "_" + str(growing_cycle) + "*.csv"
        )
    )

    if file_type in [LIME_REPORT, HARVEST_DATES]:
        path = (
            Path(path_to_data)
            .joinpath(grower)
            .glob(grower + "_" + data_aggregator + "_" + file_type + ".csv")
        )

    path_csv = next(path, NOT_FOUND)

    if path_csv == NOT_FOUND and file_type not in [*GRAN_GENERATED]:
        if verbose:
            log.warning(
                f"no {file_type} file from {growing_cycle} at {path_to_data} for grower {grower}"
            )
        cols = get_columns_by_file_type(file_type)
        return pd.DataFrame(columns=cols)

    else:
        if file_type == HARVEST_DATES:
            df = pd.read_csv(path_csv, parse_dates=["Harvest_date"])
        elif file_type == SPLIT_FIELD:
            df = pd.read_csv(path_csv)
        elif file_type in [MANURE_REPORT, LIME_REPORT]:
            df = pd.read_csv(
                path_csv,
                parse_dates=[
                    "Operation_start",
                    "Operation_end",
                    "Harvest_date_prev",
                    "Harvest_date_curr",
                ],
            )
        elif file_type in [*GRAN_GENERATED]:
            # Granular generated files are now read from GCS
            folder_path = f"{settings.bucket_folders.raw_data}/{grower}/{growing_cycle}"

            granular_generated = AggregatorOperationFactory.process_file_by_type_in_gcs(
                DataAggregators.DA_GRANULAR, BUCKET_NAME, folder_path, file_type
            )

            df = pd.DataFrame(
                [model.model_dump(by_alias=True) for model in granular_generated]
            )

        elif file_type in [*LDB_GENERATED]:
            df = pd.read_csv(
                path_csv,
                parse_dates=["Operation_start", "Operation_end"],
            )
        else:
            df = pd.read_csv(path_csv)

    return df


def read_file_by_file_type(
    path_to_data: pd.DataFrame,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    file_type: str,
    verbose: bool = True,
) -> pd.DataFrame:
    #
    # JDOps
    #
    if file_type in JD_FILE_TYPES and data_aggregator == DA_JDOPS:
        if file_type in [JD_FUEL]:
            df = read_csv_report(
                path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
            )
        else:
            df = read_JDOps_excel_export(
                path_to_data, grower, growing_cycle, file_type, verbose
            )

    #
    # Granular
    #
    elif file_type in GRAN_FILE_TYPES and data_aggregator == DA_GRANULAR:
        if file_type in [GRAN_PLANTING, GRAN_TILLAGE]:
            df = read_csv_report(
                path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
            )
        else:
            df = read_Granular_export(
                path_to_data, grower, growing_cycle, file_type, verbose
            )

        if file_type == GRAN_HARVEST and not df.empty:
            df = df.dropna(subset="Task Completed Date").reset_index(drop=True)

    #
    # CFV
    #
    elif file_type in CFV_FILE_TYPES and data_aggregator == DA_CFV:
        df = read_CFV_data(path_to_data, grower, growing_cycle, file_type, verbose)

    #
    # PAP
    #
    elif file_type in PAP_FILE_TYPES and data_aggregator == DA_PAP:
        df = read_csv_report(
            path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
        )

    #
    # Land.db
    #
    elif file_type in LDB_FILE_TYPES and data_aggregator == DA_LDB:
        # if file_type in [LDB_FUEL]:
        #     print('check')
        #     df = read_csv_report(path_to_data, grower, growing_cycle, data_aggregator, file_type)
        # else:
        df = read_LDB_data(
            path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
        )

    #
    # SMS Ag Leader
    #
    elif file_type in SMS_FILE_TYPES and data_aggregator == DA_SMS:
        df = read_csv_report(
            path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
        )

    #
    # FM
    #
    elif file_type in FM_FILE_TYPES and data_aggregator == DA_FM:
        df = read_FM_export(
            path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
        )

    #
    # Internally generated
    #
    elif file_type in GENERIC_REPORTS:
        df = read_csv_report(
            path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
        )

    else:
        df = pd.DataFrame()

    return df.reset_index(drop=True)


def get_breakdown_list(path_to_data: str | Path) -> pd.DataFrame:
    try:
        fert_list = pd.read_csv(
            Path(path_to_data).joinpath(
                "verity_chemical_product_breakdown_table - Sheet1.csv"
            )
        )

    except FileNotFoundError:
        log.exception(
            f"no `verity_chemical_product_breakdown_table - Sheet1.csv` at {path_to_data}"
        )
        return pd.DataFrame()

    """
    #####################################################################################
    ##### GP23 ADDITIONS - FD-CIC2022 ###################################################
    #####################################################################################
    """
    rel_cols = [
        "product_name",
        "product_state",
        "product_type",
        "% Ammonia",
        "% Urea",
        "% AN",
        "% AS",
        "% UAN",
        "% MAP N",
        "% DAP N",
        "% MAP P2O5",
        "% DAP P2O5",
        "% K2O",
        "% CaCO3",
        "lbs / gal",
        "manure_type",
        "lbs AI / gal",
    ]

    fert_list = fert_list[
        (
            fert_list["product_type"].isin(
                [
                    "EEF",
                    "Fertilizer",
                    "fertilizer",
                    "fungicide",
                    "herbicide",
                    "insecticide",
                    "Lime",
                    "manure",
                ]
            )
        )
    ][rel_cols]
    fert_list = fert_list.dropna(subset="product_name", axis=0)

    return fert_list.reset_index(drop=True)


def get_cover_crop_list(path_to_data: str | Path) -> pd.DataFrame:
    try:
        cc_list = pd.read_csv(
            Path(path_to_data).joinpath(
                "FD-CIC-22_cover_crop_table_11.1a_template_including_yield.csv"
            )
        )

    except FileNotFoundError:
        log.exception(
            f"no `FD-CIC-22_cover_crop_table_11.1a_template_including_yield.csv` at {path_to_data}"
        )
        return pd.DataFrame()

    return cc_list


""" ### END GP23 ADDITIONS ###################################################### """
