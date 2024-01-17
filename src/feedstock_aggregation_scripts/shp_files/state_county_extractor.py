import json
from urllib.request import urlopen

import pandas as pd
from loguru import logger as log
from shapely.geometry import Polygon, box

from .geometry import (
    get_acreage_from_polygon,
    get_acreage_from_shp_features,
    get_polygon_from_shp_features,
)
from .helpers import get_state_code_dict, init_state_county_extraction_vals
from .readers import get_county_polygon_and_features

NOT_FOUND = False

# read in geojson file containing all states and counties in USA
with urlopen(
    "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
) as response:
    counties = json.load(response)

# read in state codes from file
state_codes = get_state_code_dict()


def extract_state_county_info(shp_features, field_name):
    """returns a dictionary that contains the FIPS code, State, and County
    information for `shp`. Any information not present within `shp`
    properties, will result in a `None` entry for that field.

    @params:
    shp_features - GeoJSON features of a read-in shape file
    """

    # Extract shape file geometry as (Multi-)Polygon. This
    # geometry is used to check each County for intersections
    # with the given shape. However, this check fails for some
    # shape files.
    # This issue needs to be investigated further to understand
    # what causes this behaviour.
    shp_geometry = get_polygon_from_shp_features(shp_features)

    # The shape box is used in cases where the intersection fails
    # on the shape geometry.
    shp_box = Polygon(box(*shp_geometry.bounds))

    # The shape's acres are used to calculate the intersection %
    # between the shape geometry and the County geometry.
    shp_acres = get_acreage_from_shp_features(shp_features)

    values = init_state_county_extraction_vals()

    for p_county, feature in get_county_polygon_and_features(counties):
        try:
            intersects = p_county.intersects(shp_geometry)
            intersection_poly = p_county.intersection(shp_geometry)
        except Exception as e:
            log.error(str(e))
            log.error(f"using shape file box for {field_name}")
            intersects = p_county.intersects(shp_box)
            intersection_poly = p_county.intersection(shp_box)
            # Overwriting the shp acres reference value to produce correct
            # intersection percentages
            shp_acres = get_acreage_from_polygon(shp_box)

        if intersects:
            values["Field_name_folder"].append(field_name)
            properties = feature.get("properties", NOT_FOUND)

            intersection_acres = get_acreage_from_polygon(intersection_poly)
            intersection_percent = intersection_acres / shp_acres * 100
            values["Intersection_percent"].append(intersection_percent)

            if properties == NOT_FOUND:
                # if no properties available, append unknown
                values["FIPS"].append("unknown")
                values["State"].append("unknown")
                values["State_id"].append("unknown")
                values["County"].append("unknown")
                values["County_id"].append("unknown")
                continue

            state_id = properties.get("STATE", NOT_FOUND)
            if state_id == NOT_FOUND:
                state = None
                state_id = None
            else:
                state = state_codes[state_id]

            county_id = properties.get("COUNTY", NOT_FOUND)
            if county_id == NOT_FOUND:
                county_id = None

            county = properties.get("NAME", NOT_FOUND)
            if county == NOT_FOUND:
                county = None

            fips = feature.get("id", NOT_FOUND)
            if fips == NOT_FOUND:
                log.warning("no fips code found -> `id` not in features")
                fips = None

            values["FIPS"].append(fips)
            values["State"].append(state)
            values["State_id"].append(state_id)
            values["County"].append(county)
            values["County_id"].append(county_id)

    values_df = pd.DataFrame(values)
    values_df = values_df.groupby("Field_name_folder", as_index=False).agg(
        {"Intersection_percent": "max"}
    )

    return values_df
