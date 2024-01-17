import pathlib

import pandas as pd
from loguru import logger as log

NOT_FOUND = False

# set current directory to read in files
if __name__ == "__main__":
    CURRENT_DIR = pathlib.Path("./")
else:
    CURRENT_DIR = pathlib.Path(__file__).resolve().parent


def init_info_extract():
    d = {
        "File_path": [],
        "Grower": [],
        "Field_name_folder": [],
        "Geo_type": [],
        "Geo_coord": [],
        "Centroid_lat": [],
        "Centroid_long": [],
        "Grower_name": [],
        "Farm_name": [],
        "Field_name": [],
        "Acreage": [],
        "Acreage_calc": [],
        "Township": [],
        "Section": [],
        "Range": [],
        # "FIPS": list(),
        # "State": list(),
        # "County": list(),
        # "Intersection_percent": list(),
        # '': list(),
    }

    return d


def init_state_county_extraction_vals():
    return {
        "Field_name_folder": [],
        "FIPS": [],
        "State": [],
        "State_id": [],
        "County": [],
        "County_id": [],
        "Intersection_percent": [],
    }


def get_property_attribute(properties, col_names):
    for col_name in col_names:
        temp = properties.get(col_name, NOT_FOUND)

        if temp != NOT_FOUND:
            return temp

    return NOT_FOUND


def get_state_code_dict(country="US"):
    if country == "US":
        df = pd.read_csv(
            pathlib.Path(CURRENT_DIR).joinpath("usa-state-codes.csv"),
            dtype={"state_code": str},
        )
        df.set_index("state_code", inplace=True)
    else:
        log.info(f"No data available for country {country}")
        return

    return df.to_dict()["state"]


def mark_majority_county(shp_file_overview):
    # add new column and set default values
    shp_file_overview["Majority_county"] = 0

    for field in shp_file_overview["Field_name_folder"].unique():
        temp = shp_file_overview[shp_file_overview["Field_name_folder"].isin([field])]

        col = shp_file_overview.columns.get_loc("Majority_county")

        if len(temp.Field_name_folder) == 1:
            row = shp_file_overview.index[
                shp_file_overview["Field_name_folder"] == field
            ][0]

        else:
            # shape file intersects more than 1 County
            # find maximum intersection
            max_val = temp["Intersection_percent"].max()
            row = shp_file_overview.index[
                (shp_file_overview["Field_name_folder"] == field)
                & (shp_file_overview["Intersection_percent"] == max_val)
            ][0]

        shp_file_overview.iloc[row, col] = 1
    return shp_file_overview


# def get_polygon(shp):
#     # extracting relevant parameters out of the GeoJSON file
#     if isinstance(shp, shapefile.Reader):
#         features = shp.__geo_interface__.get("features", NOT_FOUND)
#     else:
#         features = shp.get("features", NOT_FOUND)

#     if features == NOT_FOUND:
#         print(f'no item "features" in {shp}.')
#         return np.nan
#     else:
#         features = features[0]

#     geometry = features.get("geometry", NOT_FOUND)
#     if geometry == NOT_FOUND:
#         print(f'no item "geometry" in {features}.')
#         return np.nan
#     else:
#         shp_coords = geometry.get("coordinates", NOT_FOUND)
#         geo_type = geometry.get("type", NOT_FOUND)

#         if shp_coords == NOT_FOUND or geo_type == NOT_FOUND:
#             print(f"either coordinates or shape type missing in {geometry}")
#             return np.nan

#     # building Polygons / MultiPolygons from the given coordinates
#     if geo_type == "Polygon":
#         p_shp = Polygon(shp_coords[0])

#     elif geo_type == "MultiPolygon":
#         poly_list = list()

#         for s in shp_coords:
#             # catching cases, where there are multiple Polygons within each
#             # `shp_coords`
#             if len(s) > 1:
#                 poly_list.append([Polygon(i) for i in s])
#             else:
#                 poly_list.append(Polygon(*s))

#         p_shp = MultiPolygon(poly_list)

#     else:
#         print(f"Unable to handle geometry type: {geo_type}")
#         return NOT_FOUND

#     return p_shp
