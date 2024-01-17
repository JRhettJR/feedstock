import numpy as np
from loguru import logger as log
from pyproj import Geod
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon

from ..data_prep.constants import NOT_FOUND
from .constants import SQUARE_METER_TO_ACRE

# NOT_FOUND = False


def get_polygon_from_shp_features(shp_features):
    geometry = shp_features.get("geometry", NOT_FOUND)
    if geometry == NOT_FOUND:
        log.warning(f'no item "geometry" in {shp_features}.')
        return np.nan
    else:
        shp_coords = geometry.get("coordinates", NOT_FOUND)
        geo_type = geometry.get("type", NOT_FOUND)

        if shp_coords == NOT_FOUND or geo_type == NOT_FOUND:
            log.warning(f"either coordinates or shape type missing in {geometry}")
            return np.nan

    # building Polygons / MultiPolygons from the given coordinates
    if geo_type == "Polygon":
        p_shp = Polygon(shp_coords[0])

    elif geo_type == "MultiPolygon":
        poly_list = []

        for s in shp_coords:
            # catching cases, where there are multiple Polygons within each
            # `shp_coords`
            if len(s) > 1:
                poly_list.append([Polygon(i) for i in s])
            else:
                poly_list.append(Polygon(*s))

        p_shp = MultiPolygon(poly_list)

    else:
        log.warning(f"Unable to handle geometry type: {geo_type}")
        return NOT_FOUND

    return p_shp


def get_acreage_from_polygon(shp_poly):
    # specify a named ellipsoid
    #
    # WGS84 is defined and maintained by the United States National
    # Geospatial-Intelligence Agency (NGA). It is consistent, to about
    # 1cm, with the International Terrestrial Reference Frame (ITRF).
    #
    # see: https://www.linz.govt.nz/guidance/geodetic-system/coordinate-systems-used-new-zealand/geodetic-datums/world-geodetic-system-1984-wgs84#:~:text=The%20World%20Geodetic%20System%201984,Terrestrial%20Reference%20Frame%20(ITRF).

    geod = Geod(ellps="WGS84")

    # wkt.loads reads in string representations of the (Multi)Polygon
    poly = wkt.loads(str(shp_poly))
    # the area will be the first argument in the list and calculated in
    # square meters and may be negative due to its calculation.
    # Converted to acres
    area = abs(geod.geometry_area_perimeter(poly)[0]) * SQUARE_METER_TO_ACRE

    return area


def get_acreage_from_shp_features(shp_features):
    p_shp = get_polygon_from_shp_features(shp_features)

    area = get_acreage_from_polygon(p_shp)

    return area


def get_centroid(shp_features):
    poly = get_polygon_from_shp_features(shp_features)

    centroid = list(poly.centroid.coords)[0]

    return centroid


# def get_shp_box(shp):
#     bbox = shp.__geo_interface__.get("bbox", NOT_FOUND)
#     if bbox == NOT_FOUND:
#         print("missing `bbox` in shape file")
#         return Polygon()

#     shp_box = Polygon(box(*bbox))

#     return shp_box
