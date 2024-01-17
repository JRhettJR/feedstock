import os
import pathlib
import shutil

import numpy as np
import shapefile
from loguru import logger as log

from ..data_prep.constants import NOT_FOUND
from .geometry import get_polygon_from_shp_features


def read_shapefiles_and_names(path_to_data, grower):
    """yields 3 values:
    - the shp file (read-in)
    - the field name derived from the file name --> may need adjustment when file name is only "boundary"
    - the file path as reference for the shp file's origin
    """
    for folder in (
        pathlib.Path(path_to_data).joinpath(grower, "shp-files").glob("[!.]*")
    ):
        # this field name serves as a reference to traceback to files
        # and in cases, where no attributional data is present within
        # the shp file(s).
        field_name = folder.stem

        path = next(folder.glob("*.shp"))
        file_path = "01_data" + str(path).split("01_data")[-1]

        shp = shapefile.Reader(path)
        # extract geo features
        shp_features = shp.__geo_interface__.get("features", NOT_FOUND)
        if shp_features == NOT_FOUND:
            log.warning(f'no item "features" in {shp}.')
            return np.nan

        # return all feature combinations present in the shape file
        for feature in shp_features:
            yield feature, field_name, file_path


def sort_unsorted_shp_files(path_to_data, grower):
    """bundles unsorted shape files into their own folders based on the file name.
    Expects unsorted files to live in folder `shp-unsorted` at `path_to_data/grower`.
    Will save sorted files into folder `shp-files` at `path_to_data/grower`.
    """
    path_to_dest = pathlib.Path(path_to_data).joinpath(grower, "shp-files")
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    for file in sorted(
        pathlib.Path(path_to_data).joinpath(grower, "shp-unsorted").glob("*")
    ):
        path_to_dest_temp = path_to_dest.joinpath(file.stem)
        if not os.path.exists(path_to_dest_temp):
            os.makedirs(path_to_dest_temp)

        shutil.move(file, path_to_dest.joinpath(file.stem, file.name))


def get_county_polygon_and_features(counties: dict):
    for county_features in counties["features"]:
        poly = get_polygon_from_shp_features(county_features)
        yield poly, county_features
