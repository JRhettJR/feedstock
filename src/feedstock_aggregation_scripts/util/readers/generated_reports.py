import itertools
import pathlib

import pandas as pd
from loguru import logger as log

from ...data_prep.constants import (
    LIME_REPORT,
    MANURE_REPORT,
    NOT_FOUND,
    REFERENCE_ACREAGE_REPORT,
    SPLIT_FIELD,
)


def read_generated_report(
    report_type: str,
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
):
    file_name = f"{grower}_{data_aggregator}_{report_type}"
    if file_name != LIME_REPORT:
        file_name += f"_{str(growing_cycle)}.csv"
    else:
        file_name += ".csv"

    path = pathlib.Path(path_to_data).joinpath(grower).glob(file_name)

    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(
            f"no {report_type} file at {path_to_data} for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame()

    report = pd.read_csv(path)

    return report


def read_lime_report(path_to_data, grower):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_" + LIME_REPORT + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"no lime report file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    lime = pd.read_csv(path)

    return lime


def read_manure_report(path_to_data, grower):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_" + MANURE_REPORT + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"no manure report file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    manure = pd.read_csv(path)

    return manure


def read_combined_report(
    report_type: str, path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
) -> pd.DataFrame:
    file_name = f"{grower}_{report_type}"
    if file_name != LIME_REPORT:
        file_name += f"_{str(growing_cycle)}.csv"
    else:
        file_name += ".csv"

    path = pathlib.Path(path_to_data).joinpath(grower).glob(file_name)

    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(
            f"no {report_type} file at {path_to_data} for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame()

    report = pd.read_csv(path)

    return report


def read_reference_acreage_report(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(
            grower
            + "_"
            + data_aggregator
            + "_"
            + REFERENCE_ACREAGE_REPORT
            + "_"
            + str(growing_cycle)
            + ".csv"
        )
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(
            f"no {REFERENCE_ACREAGE_REPORT} file at {path_to_data} for grower {grower} and cycle {growing_cycle} and system {data_aggregator}"
        )
        return pd.DataFrame()

    ref_acreage = pd.read_csv(path)

    return ref_acreage


def read_split_field_reports(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
):
    report = pd.DataFrame()
    file_name = f"{grower}_*_{SPLIT_FIELD}_{str(growing_cycle)}.csv"
    path = pathlib.Path(path_to_data).joinpath(grower).glob(file_name)

    files = next(path, NOT_FOUND)

    if files == NOT_FOUND:
        log.warning(
            f"no {SPLIT_FIELD} file at {path_to_data} for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame()

    path_files = itertools.chain([files], path)
    for file in path_files:
        df = pd.read_csv(file)
        report = pd.concat([report, df])
    return report


def read_shp_file_overview(path_to_data: str | pathlib.Path, grower: str):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_shp_file_overview.csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No shp file overview file at {path_to_data} for grower {grower}")
        return pd.DataFrame(columns=["Field_name", "Acreage_calc"])

    shp = pd.read_csv(path)

    return shp


def read_bulk_upload_template(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_bulk_upload_template_" + str(growing_cycle) + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(f"No bulk upload file at {path_to_data} for grower {grower}")
        return pd.DataFrame()

    bulk = pd.read_csv(path)

    return bulk


def read_attestation_overwrite(
    path_to_data: str | pathlib.Path, grower: str, growing_cycle: int
):
    path = (
        pathlib.Path(path_to_data)
        .joinpath(grower)
        .glob(grower + "_attestation_overwrite_" + str(growing_cycle) + ".csv")
    )
    path = next(path, NOT_FOUND)

    if path == NOT_FOUND:
        log.warning(
            f"No attestation overwrite file at {path_to_data} for grower {grower} and cycle {growing_cycle}"
        )
        return pd.DataFrame()

    attest = pd.read_csv(path)

    return attest
