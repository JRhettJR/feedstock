import numpy as np
import pandas as pd
from loguru import logger as log

from ..helpers import get_attributes_from_all_sources


def create_tillage_input_comparison(data_sources: dict) -> pd.DataFrame:
    """creates a comparison overview based on `Area_applied`"""
    # get complete list of fields from all available sources
    fields = get_attributes_from_all_sources(
        col_name="Field_name", data_sources=data_sources
    )
    # get all crop types used in all available sources
    crops = get_attributes_from_all_sources(
        col_name="Crop_type", data_sources=data_sources
    )

    # inititalise
    cols = ["Field_name", "Crop_type", "Operation_type"]
    df = pd.DataFrame(columns=cols)
    operation_type = "Tillage"

    for name, data in data_sources.items():
        log.info(name)
        # if no data available, skip data_source
        if data.empty:
            continue

        # add source column to data frame
        df[name] = np.nan

        for field in fields:
            # create temporary extracts based on field name
            data_t = data[
                (data.Field_name == field) & (data.Operation_type == "Tillage")
            ]

            # if no data available for current field, skip field for this data source
            if data_t.empty:
                continue

            for crop in crops:
                # prefilter for operations by product
                data_tp = data_t[(data_t.Crop_type == crop)]

                # if no data available for current field-product combination, skip product for this field
                if data_tp.empty:
                    continue

                # get quantities
                data_q = data_tp.Area_applied.sum()
                data_r = data_tp.Applied_rate.max()

                if pd.isna(crop):
                    # continue
                    log.info("missing crop")
                    log.info(f"{data_q}, {data_r}")

                t = pd.DataFrame(
                    columns=cols + [name], data=[[field, crop, operation_type, data_q]]
                )
                existing_entry = df[(df.Field_name == field) & (df.Crop_type == crop)]

                # CASE 1: no entry for for field-product combination in df
                if existing_entry.empty:
                    # add new row to data frame
                    df = pd.concat([df, t], ignore_index=True)

                # CASE 2: existing entry for field-product combination in df
                else:
                    # set value at existing index
                    df.loc[existing_entry.index[0], name] = data_q
                    df.loc[existing_entry.index[0], name] = data_q

    return df
