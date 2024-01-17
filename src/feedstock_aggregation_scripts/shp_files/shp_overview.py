import os
import pathlib
import shutil

import pandas as pd
from loguru import logger as log

from ..data_prep.complete import add_new_fields_to_mapping
from ..data_prep.constants import NOT_FOUND
from .constants import ACREAGE_COLS, FARM_NAME_COLS, FIELD_NAME_COLS, GROWER_NAME_COLS
from .geometry import get_acreage_from_shp_features, get_centroid
from .helpers import get_property_attribute, init_info_extract, mark_majority_county
from .readers import read_shapefiles_and_names
from .state_county_extractor import extract_state_county_info


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


def list_all_shp_file_properties_by_file(path_to_data, grower):
    """this function helps to identify property names for extraction.
    Newly identified property names need to be added to above constants.
    (see section "Set up / Constants")
    """
    df = pd.DataFrame()

    for shp_features, fld_nm, _ in read_shapefiles_and_names(path_to_data, grower):
        # properties = shp.__geo_interface__["features"][0]["properties"]
        properties = shp_features.get("properties", NOT_FOUND)
        if properties == NOT_FOUND:
            log.warning(
                f"no attribute 'properties' in shape file features {shp_features}"
            )
            return

        columns = properties.keys()
        column_vals = [properties[c] for c in columns]
        # fill table with values (as examples)
        temp = pd.DataFrame(
            data=[[fld_nm, *column_vals]], columns=["File_name", *columns]
        )

        df = temp if df.empty else pd.concat([df, temp], axis=0)

    return df.reset_index(drop=True)


def extract_info_from_shape_files(path_to_data, grower) -> pd.DataFrame:
    d = init_info_extract()

    # Store all available state county data
    state_county = pd.DataFrame()

    for shp_features, field_name, file_path in read_shapefiles_and_names(
        path_to_data, grower
    ):
        d["File_path"].append(file_path)
        d["Grower"].append(grower)
        d["Field_name_folder"].append(field_name)

        geometry = shp_features.get("geometry", NOT_FOUND)
        if geometry != NOT_FOUND:
            geo_type = geometry.get("type", NOT_FOUND)
            geo_coord = geometry.get("coordinates", NOT_FOUND)

        d["Geo_type"].append(geo_type) if geometry != NOT_FOUND else d[
            "Geo_type"
        ].append(None)
        d["Geo_coord"].append(geo_coord) if geometry != NOT_FOUND else d[
            "Geo_coord"
        ].append(None)

        acres = get_acreage_from_shp_features(shp_features)
        d["Acreage_calc"].append(acres)

        centroid = get_centroid(shp_features)
        d["Centroid_lat"].append(centroid[1])
        d["Centroid_long"].append(centroid[0])

        state_county_info = extract_state_county_info(shp_features, field_name)
        state_county = pd.concat([state_county, state_county_info])

        props = shp_features.get("properties", NOT_FOUND)
        if props != NOT_FOUND:
            # extract attributional data where available
            props_grower_name = get_property_attribute(props, GROWER_NAME_COLS)

            props_field_name = get_property_attribute(props, FARM_NAME_COLS)

            props_farm_name = get_property_attribute(props, FIELD_NAME_COLS)

            props_field_area = get_property_attribute(props, ACREAGE_COLS)

            props_township = props.get("Township", NOT_FOUND)
            props_section = props.get("Section", NOT_FOUND)
            props_range = props.get("Range", NOT_FOUND)

            props_list = [
                props_grower_name,
                props_field_name,
                props_farm_name,
                props_field_area,
                props_township,
                props_section,
                props_range,
            ]

        props_fields = [
            "Grower_name",
            "Farm_name",
            "Field_name",
            "Acreage",
            "Township",
            "Section",
            "Range",
        ]

        for item, prop in zip(props_fields, props_list, strict=False):
            d[item].append(prop) if props != NOT_FOUND else d[item].append(None)

    attrib_data = pd.DataFrame(d)
    temp = pd.merge(attrib_data, state_county, on="Field_name_folder")

    # add majority county mark
    temp = mark_majority_county(temp)

    return temp


def create_shape_file_overview(path_to_data, path_to_dest, grower):
    # check whether unsorted files are present
    if os.path.exists(pathlib.Path(path_to_data).joinpath(grower, "shp-unsorted")):
        sort_unsorted_shp_files(path_to_data, grower)

    # if no shp-files are present, exit function
    if not os.path.exists(pathlib.Path(path_to_data).joinpath(grower, "shp-files")):
        log.warning(f"no folder 'shp-files' present at {path_to_data}/{grower}")
        return pd.DataFrame()

    log.info(f"extracting shp-file overview for grower {grower}...")
    overview = extract_info_from_shape_files(path_to_data, grower)

    # Add any field mapping values detected here
    fields = overview[["Farm_name", "Field_name", "Acreage_calc"]].copy()
    fields.loc[:, "system"] = "shp"
    fields[["Farm_name", "Field_name"]] = fields[["Farm_name", "Field_name"]].astype(
        str
    )
    fields.rename(columns={"Acreage_calc": "system_acres"}, inplace=True)
    add_new_fields_to_mapping(path_to_data, grower, field_list=fields)

    path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
    if not os.path.exists(path_to_dest):
        os.makedirs(path_to_dest)

    if overview.empty:
        log.warning(f"no data to save for {grower}_shp_file_overview.csv")
    else:
        overview.to_csv(
            path_to_dest.joinpath(grower + "_shp_file_overview.csv"), index=False
        )

    return overview
