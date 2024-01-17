import os
import pathlib

import pandas as pd
from loguru import logger as log

from ..config import settings
from ..data_prep.grower_data_agg_mapping import grower_da_mapping
from ..shp_files import shp_overview

path_to_data = settings.data_prep.source_path
path_to_dest = settings.data_prep.dest_path

# grouped by grower, feed each data aggregator into function lists


def run():
    for grower in grower_da_mapping:
        shp_overview.create_shape_file_overview(path_to_data, path_to_dest, grower)


def create_shapefile_overview_combined(
    path_to_data=path_to_dest, path_to_dest=path_to_dest
):
    overview = pd.DataFrame()

    for grower in grower_da_mapping:
        path = pathlib.Path(path_to_data).joinpath(
            grower, grower + "_shp_file_overview.csv"
        )

        df = pd.read_csv(path)

        overview = pd.concat([overview, df])

    path_to_dest = pathlib.Path(path_to_dest)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if overview.empty:
        log.warning("no data to save for shp_file_overview_combined.csv")
    else:
        overview.to_csv(
            path_to_dest.joinpath("shp_file_overview_combined.csv"), index=False
        )

    return overview


def create_shapes_per_county_overview(
    path_to_data=path_to_dest, path_to_dest=path_to_dest
):
    path_to_data = pathlib.Path(path_to_data).joinpath("shp_file_overview_combined.csv")
    temp = pd.read_csv(path_to_data)

    temp = temp.groupby(by=["Grower", "County"], as_index=False, dropna=False).count()[
        ["Grower", "County", "FIPS"]
    ]

    path_to_dest = pathlib.Path(path_to_dest)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if temp.empty:
        log.warning("no data to save for shp_file_overview_shapes_per_county.csv")
    else:
        temp.to_csv(
            path_to_dest.joinpath("shp_file_overview_shapes_per_county.csv"),
            index=False,
        )

    return temp


def filter_shapes_in_multiple_counties(
    path_to_data=path_to_dest, path_to_dest=path_to_dest
):
    path_to_data = pathlib.Path(path_to_data).joinpath("shp_file_overview_combined.csv")
    temp = pd.read_csv(path_to_data)

    temp = temp[temp["Intersection_percent"] < 100.0]

    path_to_dest = pathlib.Path(path_to_dest)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if temp.empty:
        log.warning(
            "no data to save for shp_file_overview_shapes_in_multiple_counties.csv"
        )
    else:
        temp.to_csv(
            path_to_dest.joinpath("shp_file_overview_shapes_in_multiple_counties.csv"),
            index=False,
        )

    return temp
