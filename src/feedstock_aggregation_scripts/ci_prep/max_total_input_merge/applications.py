import numpy as np
import pandas as pd

from ...util.conversion import convert_quantity_by_unit
from ..helpers import clean_input_type


def create_comprehensive_apps_list(
    source_1: pd.DataFrame, source_2: pd.DataFrame
) -> pd.DataFrame:
    # get complete list of fields
    if not source_1.empty:
        temp = pd.concat(
            [
                pd.Series(source_1.Field_name.unique()),
                pd.Series(source_2.Field_name.unique()),
            ]
        )
    else:
        temp = pd.Series(source_2.Field_name.unique(), name="Field_name")

    fields = temp.unique()

    source_1 = source_1.drop_duplicates(ignore_index=True)
    source_2 = source_2.drop_duplicates(ignore_index=True)

    merge_cols = source_1.columns.to_list()
    df = pd.DataFrame(columns=merge_cols)

    for field in fields:
        # create temporary extracts based on field name
        source_1_t = source_1[(source_1.Field_name == field)]
        source_2_t = source_2[(source_2.Field_name == field)]

        # get all products used from both files
        temp = pd.concat(
            [
                pd.Series(source_1_t.Product.unique()),
                pd.Series(source_2_t.Product.unique()),
            ]
        )
        products = temp.unique()

        for product in products:
            # quantity = 0
            # prefilter for operations by product
            source_1_tp = source_1_t[(source_1_t.Product == product)]
            source_2_tp = source_2_t[(source_2_t.Product == product)]
            # get quantities
            source_1_q = source_1_tp.Applied_total.sum()
            source_2_q = source_2_tp.Applied_total.sum()

            # CASE 1: no data in source_1 - data available in source_2
            if (source_1_q == 0 or np.isnan(source_1_q)) and source_2_q > 0:
                df = pd.concat([df, source_2_tp])

            # CASE 2: data available in source_1 - no data in source_2
            elif (source_2_q == 0 or np.isnan(source_2_q)) and source_1_q > 0:
                df = pd.concat([df, source_1_tp])

            # CASE 3: no data in source_1 - no data in source_2
            elif source_2_q == source_1_q == 0:
                pass

            # CASE 4: data available in source_1 - data available in source_2
            else:
                # extract units
                source_1_u = source_1_tp.Applied_unit.iloc[0]
                source_2_u = source_2_tp.Applied_unit.iloc[0]

                if source_1_u != source_2_u:
                    # make quantities comparable
                    source_1_q, source_1_u = convert_quantity_by_unit(
                        source_1_q, source_1_u
                    )
                    source_2_q, source_2_u = convert_quantity_by_unit(
                        source_2_q, source_2_u
                    )

                # select the greater quantity (MAX)
                df = (
                    pd.concat([df, source_1_tp])
                    if source_1_q > source_2_q
                    else pd.concat([df, source_2_tp])
                )

        # break
    return df


def create_comprehensive_apps_from_cleaned_files(cleaned_files: dict) -> pd.DataFrame:
    # initialise dummy df for merging all unique operations per field into
    comp_inputs = pd.DataFrame(
        columns=[
            "Data_source",
            "Field_name",
            "Product",
            "Applied_total",
            "Applied_unit",
        ]
    )

    # Here we exclude PAP because we need to add these applications, not consider for a max input.
    clean_files_dict = dict(cleaned_files)
    if "PAP" in clean_files_dict:
        del clean_files_dict["PAP"]

    for file in clean_files_dict.values():
        if not file.empty:
            comp_inputs = create_comprehensive_apps_list(comp_inputs, file)

    if "PAP" in cleaned_files:
        pap_app = cleaned_files["PAP"]
        comp_inputs = pd.concat([comp_inputs, pap_app], ignore_index=True)

    # return comp_inputs
    if not comp_inputs.empty:
        comp_inputs = comp_inputs.rename(columns={"Product_type": "Input_type"})
        comp_inputs = clean_input_type(comp_inputs)
        comp_inputs.Growing_cycle = comp_inputs.Growing_cycle.astype(int)

    return comp_inputs.reset_index(drop=True)
