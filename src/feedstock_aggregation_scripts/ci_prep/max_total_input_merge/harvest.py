import numpy as np
import pandas as pd

from ..helpers import clean_input_type


def create_comprehensive_harvest_list(
    source_1: pd.DataFrame, source_2: pd.DataFrame
) -> pd.DataFrame:
    # SUGGESTION: rework this to more than 2 source files
    # Additionally, specify expected dataframe type (cleaned data)

    # get complete list of fields
    temp = pd.concat(
        [
            pd.Series(source_1.Field_name.unique()),
            pd.Series(source_2.Field_name.unique()),
        ]
    )
    fields = temp.unique()

    source_1 = source_1.drop_duplicates(ignore_index=True)
    source_2 = source_2.drop_duplicates(ignore_index=True)

    merge_cols = source_1.columns.to_list()
    df = pd.DataFrame(columns=merge_cols)

    for field in fields:
        # create temporary extracts based on field name and operation type tillage
        source_1_t = source_1[
            (source_1["Field_name"] == field)
            & (source_1["Operation_type"] == "Harvest")
        ]
        source_2_t = source_2[
            (source_2["Field_name"] == field)
            & (source_2["Operation_type"] == "Harvest")
        ]

        # get complete list of crop types
        temp = pd.concat(
            [
                pd.Series(source_1_t["Crop_type"].unique()),
                pd.Series(source_2_t["Crop_type"].unique()),
            ]
        )
        crops = temp.unique()

        for crop in crops:
            # create temporary extracts based on field name and operation type tillage
            source_1_tc = source_1_t[(source_1_t["Crop_type"] == crop)]
            source_2_tc = source_2_t[(source_2_t["Crop_type"] == crop)]

            # extract aggregated `Total_dry_yield` for comparison
            # First assess if Total_dry_yield is available
            if source_1_tc.empty:
                source_1_aa = float("NaN")
            else:
                source_1_aa = source_1_tc["Total_dry_yield"].sum()

            if source_2_tc.empty:
                source_2_aa = float("NaN")
            else:
                source_2_aa = source_2_tc["Total_dry_yield"].sum()

            # CASE 1: no data in source_1 - data available in source_2
            if (source_1_aa == 0 or np.isnan(source_1_aa)) and source_2_aa > 0:
                df = pd.concat([df, source_2_tc])

            # CASE 2: data available in source_1 - no data in source_2
            elif (source_2_aa == 0 or np.isnan(source_2_aa)) and source_1_aa > 0:
                df = pd.concat([df, source_1_tc])

            # CASE 3: no data in source_1 - no data in source_2
            elif source_2_aa == source_1_aa == 0:
                pass

            # CASE 4: data available in source_1 - data available in source_2
            else:
                # select the greater total area applied (MAX)
                df = (
                    pd.concat([df, source_1_tc])
                    if source_1_aa > source_2_aa
                    else pd.concat([df, source_2_tc])
                )

    return df


def create_comprehensive_harvest_from_cleaned_files(
    cleaned_files: dict,
) -> pd.DataFrame:
    # initialise dummy df for merging all unique operations per field into
    comp_inputs = pd.DataFrame(
        columns=[
            "Data_source",
            "Field_name",
            "Operation_type",
            "Total_dry_yield",
            "Crop_type",
        ]
    )

    for file in cleaned_files.values():
        comp_inputs = create_comprehensive_harvest_list(comp_inputs, file)
        # return comp_inputs
    comp_inputs = comp_inputs.rename(columns={"Product_type": "Input_type"})
    comp_inputs = clean_input_type(comp_inputs)

    return comp_inputs.reset_index(drop=True)
