import numpy as np
import pandas as pd
from loguru import logger as log

from ...util.conversion import convert_quantity_by_unit
from ..helpers import get_attributes_from_all_sources


def create_app_input_comparison_by_crop(data_sources: dict) -> pd.DataFrame:
    """creates an overview of applications and planting operations.
    Overview is separates inputs by `Crop_type` (where available).
    For unavailable `Crop_type`, the total will be written to the existing records.
    """

    # get complete list of fields from all available sources
    fields = get_attributes_from_all_sources(
        col_name="Field_name", data_sources=data_sources
    )
    # get all products used in all available sources
    products = get_attributes_from_all_sources(
        col_name="Product", data_sources=data_sources
    )
    # get all crop types used in all available sources
    crops = get_attributes_from_all_sources(
        col_name="Crop_type", data_sources=data_sources
    )

    # initialize
    cols = ["Field_name", "Crop_type", "Product", "Applied_unit", "Operation_type"]
    df = pd.DataFrame(columns=cols)
    operation_type = "Application"

    for name, data in data_sources.items():
        log.info(name)
        # if no data available, skip data_source
        if data.empty:
            continue

        # add source column to data frame
        df[name] = np.nan

        for field in fields:
            # create temporary extracts based on field name
            data_t = data[(data.Field_name == field)]

            # if no data available for current field, skip field for this data source
            if data_t.empty:
                continue

            for product in products:
                # if product is nan, skip (irrelevant entries)
                if pd.isna(product):
                    continue

                # prefilter for operations by product
                data_tp = data_t[(data_t.Product == product)]

                # if no data available for current field-product combination, skip product for this field
                if data_tp.empty:
                    continue

                for crop in crops:
                    # prefilter for operations by product
                    if not pd.isna(crop):
                        data_tpc = data_tp[(data_tp.Crop_type == crop)]
                        existing_entry = df[
                            (df.Field_name == field)
                            & (df.Product == product)
                            & (df.Crop_type == crop)
                        ]
                    else:
                        # if no Crop_type available,
                        data_tpc = data_tp
                        existing_entry = df[
                            (df.Field_name == field) & (df.Product == product)
                        ]
                    # if no data available for current field-product combination, skip product for this field
                    if data_tpc.empty:
                        continue

                    # get quantities
                    data_q = data_tpc.Applied_total.sum()
                    # if name == 'Granular' and field == 'Woodrich 104':
                    #     print(name, field, product, crop, data_q)

                    # set unit
                    unit = None

                    if not (data_q == 0 or np.isnan(data_q)):
                        # extract units
                        data_u = data_tpc.Applied_unit.iloc[0]

                        data_q, data_u = convert_quantity_by_unit(data_q, data_u)
                        unit = data_u

                    t = pd.DataFrame(
                        columns=cols + [name],
                        data=[[field, crop, product, unit, operation_type, data_q]],
                    )

                    # CASE 1: no entry for field-product combination in df
                    if existing_entry.empty:
                        # add new row to data frame
                        df = pd.concat([df, t], ignore_index=True)

                    # CASE 2: existing entry for field-product combination in df
                    else:
                        for i in existing_entry.index:
                            df.loc[i, name] = data_q

                        # check for missing unit for this entry
                        if pd.isna(df.loc[existing_entry.index[0], "Applied_unit"]):
                            log.info(f"missing unit for {product} on field {field}")
                            df.loc[existing_entry.index[0], "Applied_unit"] = data_u

    return df
