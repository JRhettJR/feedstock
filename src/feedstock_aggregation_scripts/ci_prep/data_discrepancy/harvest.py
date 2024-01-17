import numpy as np
import pandas as pd
from loguru import logger as log

from ..helpers import get_attributes_from_all_sources


def create_harvest_input_comparison(data_sources: dict) -> pd.DataFrame:
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
    operation_type = "Harvest"

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
                (data.Field_name == field) & (data.Operation_type == "Harvest")
            ]

            # if no data available for current field, skip field for this data source
            if data_t.empty:
                continue

            for crop in crops:
                # if product is nan, skip (irrelevant entries)
                # if pd.isna(crop):
                #     continue

                # prefilter for operations by product
                data_tp = data_t[(data_t.Crop_type == crop)]

                # if no data available for current field-product combination, skip product for this field
                if data_tp.empty:
                    continue

                # get quantities
                data_q = data_tp.Total_dry_yield.sum()

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

                    # # check for missing unit for this entry
                    # if pd.isna(df.loc[existing_entry.index[0], 'Applied_unit']):
                    #     print('missing unit')
                    #     df.loc[existing_entry.index[0], 'Applied_unit'] = data_u

    return df
