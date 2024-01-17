import pathlib

import numpy as np
import pandas as pd
from loguru import logger as log

from ..config import settings
from ..data_prep.constants import FDCIC_CROPS
from ..data_prep.npk_breakdowns import get_fert_list
from ..util.conversion import convert_quantity_by_unit
from ..util.readers.general import get_breakdown_list
from ..util.readers.generated_reports import (
    read_combined_report,
    read_split_field_reports,
)
from .cc_use import add_cover_crop_use_decision
from .constants import (
    BULK_TEMPLATE_COLS,
    CC_REPORT,
    MANURE_REPORT,
    REFERENCE_ACREAGE_REPORT,
)
from .helpers import (
    adjust_input_type,
    adjust_operation_type,
    adjust_yield_for_secondary_crops,
    determine_major_crop_types,
    get_attributes_from_all_sources,
    mark_verified_fields,
)
from .manure_use import add_manure_use_decision
from .max_total_input_merge.max_total_input_merge import (
    create_comprehensive_inputs_from_cleaned_files,
)
from .n_management import (
    add_eef_decision,
    add_fertilizer_timing_categorization_4r,
    add_fertilizer_timing_decision_4r,
    add_n_management_decision,
    add_nitrogen_use_efficiency,
)
from .reference_acreage import select_reference_acreage
from .shp_files import add_shp_file_name_comparison
from .tillage import add_tillage_params, add_tillage_practice_decision

# This number may change after feedback from Chan - also pending supporting documentation
amount_4r_threshold = 0.15


def prepare_overview_for_bulk_mapping(
    path_to_data: str | pathlib.Path,
    path_to_processed: str | pathlib.Path,
    path_to_dest: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
):
    overview = create_comprehensive_inputs_from_cleaned_files(
        path_to_processed, grower, growing_cycle
    )
    # Reference acreage
    reference_acreage = read_combined_report(
        report_type=REFERENCE_ACREAGE_REPORT,
        path_to_data=path_to_processed,
        grower=grower,
        growing_cycle=growing_cycle,
    )

    # Get reference_acreage for unique combination
    temp = overview[["Field_name", "Crop_type"]].drop_duplicates()
    temp[["Reference_acreage", "Exclusion_reason"]] = temp.apply(
        lambda x: pd.Series(
            select_reference_acreage(x["Field_name"], x["Crop_type"], reference_acreage)
        ),
        axis=1,
    )
    # Update overview with reference acreage and exclusion reason if any
    overview = pd.merge(
        overview,
        temp[["Field_name", "Crop_type", "Reference_acreage", "Exclusion_reason"]],
        on=["Field_name", "Crop_type"],
        how="left",
    )

    # Nitrogen management info
    # 4R Timing
    overview = add_fertilizer_timing_categorization_4r(overview, growing_cycle)
    # soil temperature

    # EEF categorization
    # read chemical product breakdown list
    fert_list = get_fert_list(path_to_data=path_to_data)
    # add EEF marker to products
    overview = pd.merge(
        left=overview,
        right=fert_list[["product_name", "EEF product (y/n) - Fert only"]],
        left_on="Product",
        right_on="product_name",
        how="left",
    )
    # avoid duplicate entries for rows with empty `Product`
    overview.drop_duplicates(inplace=True, ignore_index=True)

    overview = overview.rename(columns={"EEF product (y/n) - Fert only": "EEF_product"})

    return overview


def create_decision_matrix(
    bulk_mapping_overview: pd.DataFrame,
    path_to_data: str | pathlib.Path,
    path_to_processed: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
) -> pd.DataFrame:
    log.info(f"creating decision matrix for grower {grower}...")
    fields = get_attributes_from_all_sources(
        "Field_name", {"Bulk_overview": bulk_mapping_overview}
    )
    decisions = pd.DataFrame(columns=["Field_name"], data=fields)

    # mark verified fields
    decisions = mark_verified_fields(decisions, path_to_data, grower)
    # shp file field name
    decisions = add_shp_file_name_comparison(decisions, path_to_processed, grower)
    # 4R Amount (NUE)
    decisions = add_nitrogen_use_efficiency(
        decisions, bulk_mapping_overview, path_to_data, grower
    )

    if decisions["Crop_type"].isna().all():
        log.info(
            f"adding crop type from bulk_mapping_overview due to missing harvest data for grower {grower}"
        )
        # If no harvest data is present, `Crop_type` will remain empty (None).
        # In this case we will add crop type from bulk.
        # This avoids empty bulk_upload files for missing harvest data
        crops = bulk_mapping_overview[["Field_name", "Crop_type"]].drop_duplicates()
        decisions = pd.merge(
            decisions,
            crops,
            how="left",
            on="Field_name",
        )

    # 4R Amount (NUE)
    decisions["4R_amount"] = decisions["NUE"].apply(
        lambda nue: "4R" if amount_4r_threshold < nue < 1.0 else "NO_4R"
    )

    # 4R - Timing
    decisions["4R_timing"] = decisions.apply(
        lambda x: add_fertilizer_timing_decision_4r(
            x["Field_name"], grower, growing_cycle, bulk_mapping_overview
        ),
        axis=1,
    )
    # EEF (Enhanced Efficiency Fertilizers)
    decisions["EEF"] = decisions.apply(
        lambda x: add_eef_decision(x["Field_name"], bulk_mapping_overview), axis=1
    )

    # N management classification
    decisions["N_MGT_PRACTICE"] = decisions.apply(
        lambda x: add_n_management_decision(
            timing_4r=x["4R_timing"], amount_4r=x["4R_amount"], eef=x["EEF"]
        ),
        axis=1,
    )

    # Manure use
    manure_report = read_combined_report(
        report_type=MANURE_REPORT,
        path_to_data=path_to_processed,
        grower=grower,
        growing_cycle=growing_cycle,
    )
    # manure_report = read_manure_report(path_to_data=path_to_processed, grower=grower)
    if manure_report.empty:
        # If no `manure_report` file is available,
        # set all records to no manure used.
        decisions["MANURE_USE"] = False
    else:
        decisions["MANURE_USE"] = decisions.apply(
            lambda x: add_manure_use_decision(x["Field_name"], manure_report), axis=1
        )

    # Cover crop use
    cc_report = read_combined_report(
        report_type=CC_REPORT,
        path_to_data=path_to_processed,
        grower=grower,
        growing_cycle=growing_cycle,
    )
    if cc_report.empty:
        # If no `cover_crop_report` file is available,
        # set all records to no cover crop used.
        decisions["COVER_CROP_USE"] = False
    else:
        # Currently, this will always be excepted, as the
        # `cover_crop_report` still has no column for
        # `Area_coverage_percent` (see ticket DPREP-201).
        try:
            log.info(f"Adding cover crop use decision for {grower}")
            decisions["COVER_CROP_USE"] = decisions.apply(
                lambda x: add_cover_crop_use_decision(x["Field_name"], cc_report),
                axis=1,
            )
        except Exception as err:
            log.exception(str(err))

    # Tillage practice
    decisions = add_tillage_params(decisions, bulk_mapping_overview)
    decisions["TILL_PRACTICE"] = decisions.apply(
        lambda x: add_tillage_practice_decision(
            x["Field_name"], x["Till_depth"], x["Till_passes"]
        ),
        axis=1,
    )

    # Determine major crop type
    split_field_report = read_split_field_reports(
        path_to_data=path_to_processed,
        grower=grower,
        growing_cycle=growing_cycle,
    )
    if split_field_report.empty:
        # If no `split_field_report` file is available, set by crop count.
        decisions["Major_crop_type"] = decisions.apply(
            lambda x: determine_major_crop_types(x["Field_name"], decisions), axis=1
        )
    else:
        # Set Major_crop_type = Potential_split_field if field in split_field_report
        condition = decisions["Field_name"].isin(split_field_report["Field_name"])
        decisions.loc[condition, "Major_crop_type"] = "Potential_split_field"
        decisions["Major_crop_type"].fillna(decisions["Crop_type"], inplace=True)
    return decisions


def map_to_bulk_upload_template(comprehensive_inputs: pd.DataFrame) -> pd.DataFrame:
    # initialise data frame for bulk_upload_template
    bulk_template = pd.DataFrame(columns=BULK_TEMPLATE_COLS)

    bulk_template["DATA SOURCE"] = comprehensive_inputs.Data_source
    bulk_template["FIELD_NAME"] = comprehensive_inputs.Field_name

    bulk_template["CROP_TYPE"] = comprehensive_inputs.Crop_type
    bulk_template["GROWING_CYCLE"] = comprehensive_inputs.Growing_cycle.astype(int)
    bulk_template["YIELD"] = comprehensive_inputs.Total_dry_yield
    bulk_template["MOISTURE_AT_HARVEST"] = comprehensive_inputs.Moisture

    bulk_template["OPERATION_NAME"] = comprehensive_inputs.Task_name
    bulk_template["OPERATION_TYPE"] = comprehensive_inputs.Operation_type

    bulk_template["OPERATION_START"] = comprehensive_inputs.Operation_start
    bulk_template["OPERATION_END"] = comprehensive_inputs.Operation_end

    # TILLAGE is currently missing in merged file

    bulk_template["INPUT_NAME"] = comprehensive_inputs.Product
    bulk_template["INPUT_TYPE"] = comprehensive_inputs.Input_type
    bulk_template["INPUT_RATE"] = comprehensive_inputs.Applied_total
    bulk_template["INPUT_UNIT"] = comprehensive_inputs.Applied_unit
    bulk_template["INPUT_ACRES"] = comprehensive_inputs.Area_applied
    # bulk_template[''] =

    return bulk_template


def adjust_parameters_after_mapping(bulk_template: pd.DataFrame) -> pd.DataFrame:
    # adjust `OPERATION_TYPE`
    bulk_template["OPERATION_TYPE"] = bulk_template["OPERATION_TYPE"].apply(
        adjust_operation_type
    )

    # adjust `INPUT_TYPE`
    bulk_template["INPUT_TYPE"] = bulk_template.apply(
        lambda x: adjust_input_type(x["INPUT_TYPE"], x["INPUT_UNIT"]), axis=1
    )
    # TODO: adjust YIELD for HARVEST operations that are not
    # related to the main (cash) crop on that field
    bulk_template = adjust_yield_for_secondary_crops(bulk_template)

    # adjust `Total_dry_yield` to yield per acre (need reference acreage!)
    bulk_template["YIELD"] = bulk_template["YIELD"] / bulk_template["REFERENCE_ACREAGE"]

    # set `GREEN_AMMONIA` to False
    bulk_template["GREEN_AMMONIA"] = False

    return bulk_template


def add_field_practice_decision_from_matrix(
    decision_matrix: pd.DataFrame, bulk_template: pd.DataFrame
) -> pd.DataFrame:
    # Adding several field practice decisions from `decision_matrix`
    # at once:
    # - `REFERENCE_ACREAGE`
    # - `N_MANAGEMENT_PRACTICE`
    # - `MANURE_USE`
    # - `COVER_CROP_USE`
    # - `TILLAGE_PRACTICE`
    field_practice_cols = [
        "REFERENCE_ACREAGE",
        "N_MGT_PRACTICE",
        "MANURE_USE",
        "COVER_CROP_USE",
        "TILL_PRACTICE",
    ]
    temp = bulk_template
    for col in field_practice_cols:
        temp = fill_field_practice_gaps(temp, col, decision_matrix)
    # bulk_cols = list(bulk_template.columns)
    # [bulk_cols.remove(col) for col in field_practice_cols]

    # temp = pd.merge(
    #     bulk_template[bulk_cols],
    #     decision_matrix[["Field_name", *field_practice_cols, "Crop_type"]],
    #     left_on=["FIELD_NAME", "CROP_TYPE"],
    #     right_on=["Field_name", "Crop_type"],
    #     how="left",
    # )
    # # Fill gaps in N_MGT_PRACTICE, MANURE_USE and COVER_CROP_USE
    # for col in ["N_MGT_PRACTICE", "MANURE_USE", "COVER_CROP_USE"]:
    #     temp = fill_field_practice_gaps(temp, col, decision_matrix)

    # # Drop possible duplicate rows due to double entries in decision matrix
    # temp.drop_duplicates(
    #     subset=["FIELD_NAME", "INPUT_NAME", "OPERATION_START"],
    #     inplace=True,
    #     ignore_index=True,
    # )

    # Set `CROP_TYPE` to fill out all rows for a given field
    #
    # TODO: check, whether all verified fields in the end are
    # filled out correctly
    # temp["CROP_TYPE"] = temp.apply(
    #     lambda x: adjust_crop_type(x["FIELD_NAME"], decision_matrix), axis=1
    # )

    # replace `None` entries in `TILL_PRACTICE` to `CONVENTIONAL_TILLAGE`
    temp["TILL_PRACTICE"] = temp["TILL_PRACTICE"].apply(
        lambda t: "CONVENTIONAL_TILLAGE" if pd.isna(t) else t
    )

    return temp[BULK_TEMPLATE_COLS].reset_index(drop=True)


def fill_crop_type_in_bulk(
    field_name: str, crop_type: str | None, decision_matrix: pd.DataFrame
) -> str:
    # do not overwrite existing crop types
    if isinstance(crop_type, str):
        return crop_type

    # extract major crop type from decision_matrix
    temp = decision_matrix[decision_matrix["Field_name"].isin([field_name])]
    major_crop_type = temp["Major_crop_type"].unique()

    if len(major_crop_type) == 1:
        return major_crop_type[0]
    else:
        # Currently the decision matrix is displaying more than 1 major crop type
        # for some growers (e.g. Feikema).
        # Check if any GREET Crops are among those. If yes, we will take that for now.
        for crop in major_crop_type:
            if crop in FDCIC_CROPS:
                return crop
        # Else, we will return None for now.
        return None


def fill_ref_acreage_in_bulk(
    field_name: str,
    reference_acreage: float | str | None,
    crop_type: str,
    decision_matrix: pd.DataFrame,
) -> str:
    # print("FILL_REF_AC:", field_name, reference_acreage, crop_type)
    # do not overwrite existing values
    if isinstance(reference_acreage, (float, str)):
        return reference_acreage

    # extract reference acreage from decision_matrix
    temp = decision_matrix[
        (decision_matrix["Field_name"].isin([field_name]))
        & (decision_matrix["Major_crop_type"].isin([crop_type]))
    ]
    if temp.empty:
        return None

    reference_acreage = temp["REFERENCE_ACREAGE"].dropna().unique()

    if len(reference_acreage) == 1:
        return reference_acreage[0]
    else:
        # If reference acreage is ambiguous, return None
        return None


def fill_field_practice_gaps(
    bulk_template: pd.DataFrame,
    col_name: str,
    decision_matrix: pd.DataFrame,
) -> str:
    for field in bulk_template["FIELD_NAME"].unique():
        temp = decision_matrix[decision_matrix["Field_name"].isin([field])]
        # skip if no entries in decision matrix
        if temp.empty:
            continue

        existing_practices = list(temp[col_name].dropna().unique())

        if len(existing_practices) == 1:
            # print("[INFO]", existing_practices[0], len(existing_practices))
            # print("[INFO]", bulk_template.index[bulk_template["FIELD_NAME"] == field])
            bulk_template.loc[
                bulk_template.index[bulk_template["FIELD_NAME"] == field], [col_name]
            ] = existing_practices[0]

        else:
            if col_name in ["MANURE_USE", "COVER_CROP_USE"]:
                # Assumption is that at least one row is marked TRUE
                bulk_template.loc[
                    bulk_template.index[bulk_template["FIELD_NAME"] == field],
                    [col_name],
                ] = True
            else:
                # multiple values for N_MGT_PRACTICE
                bulk_template.loc[
                    bulk_template.index[bulk_template["FIELD_NAME"] == field],
                    [col_name],
                ] = None

    return bulk_template


"""
#####################################################################################
##### GP23 ADDITIONS - FD-CIC2022 (DS-144) ##########################################
#####################################################################################
"""


def add_additional_manure_params_to_bulk(bulk_template: pd.DataFrame) -> pd.DataFrame:
    """Adds additional manure parameters to `bulk_template` to satisfy FD-CIC2022 manure input
    needs. Additional parameters added are:

    - MANURE_TYPE                --> supplied from `verity_chemical_product_breakdown_table - Sheet1.csv`,
                                     in the future from `product_breakdown_table`
    - MANURE_DRY_QUANTITY_EQUIV
    - MANURE_TRANS_DIST      --> set to `None` for now
    - MANURE_TRANS_EN        --> set to `None` for now

    The parameter `MANURE_APPL_EN` will be captured through the regular energy columns within
    the `bulk_template`.

    """
    input_breakdown = get_breakdown_list(settings.data_prep.source_path)
    manure_products = input_breakdown[input_breakdown["product_type"].isin(["manure"])]

    manure_ops = bulk_template[
        bulk_template["INPUT_NAME"].isin(manure_products["product_name"])
    ]

    # Set dummy columns
    bulk_template["MANURE_TYPE"] = None
    # Model input quantity in TN
    bulk_template["MANURE_DRY_QUANTITY_EQUIV"] = None
    # TODO: update according to data availability in source data.
    bulk_template["MANURE_TRANS_DIST"] = None
    bulk_template["MANURE_TRANS_EN"] = None

    # Iterate over existing manure operations and add info
    for idx, row in manure_ops.iterrows():
        prod_stats = manure_products[
            manure_products["product_name"].isin([row["INPUT_NAME"]])
        ]
        # Extract and set manure type
        col_idx = bulk_template.columns.get_loc("MANURE_TYPE")
        bulk_template.iloc[idx, col_idx] = prod_stats.manure_type

        # Extract manure conversion factor
        manure_conv_factor = prod_stats["lbs / gal"]
        # Extract the converted (i.e. to GAL or LBS, respectively) base quantity from data
        base_quantity, _ = convert_quantity_by_unit(
            quantity=row["INPUT_RATE"], unit=row["INPUT_UNIT"]
        )
        # Apply conversion to target unit, i.e. TN
        col_idx = bulk_template.columns.get_loc("MANURE_DRY_QUANTITY_EQUIV")

        bulk_template.iloc[idx, col_idx] = (
            base_quantity
            # Convert to DRY LBS
            * manure_conv_factor
            # Convert LBS to TN (short tons)
            / 2000
        )

    return bulk_template


def create_bulk_upload(
    comprehensive_inputs: pd.DataFrame,
    decision_matrix: pd.DataFrame,
    verified_fields: pd.DataFrame,
) -> pd.DataFrame:
    log.info("Mapping comprehensive inputs to bulk upload shape")
    bulk_template = map_to_bulk_upload_template(comprehensive_inputs)

    log.info(
        "Filtering out rows where fields aren't verified or are potentially a split field"
    )
    exclusions = pd.DataFrame(columns=["Case", "Excluded"])

    # Exclusion case 1: missing data
    fields_before = verified_fields["Field_name"].unique()
    comprehensive_verified = comprehensive_inputs[
        comprehensive_inputs["Field_name"].isin(verified_fields["Field_name"])
    ]
    diff = np.setdiff1d(
        fields_before, comprehensive_verified["Field_name"].unique(), assume_unique=True
    )
    excl_case = pd.DataFrame(data={"Case": ["missing-data"], "Excluded": [None]})

    for i, f in zip(range(len(diff)), diff, strict=True):
        excl_case.at[i, "Case"] = "missing-data"
        excl_case.at[i, "Excluded"] = f

    exclusions = pd.concat([exclusions, excl_case])

    # Remove field if flagged as split field or not in verified list
    split_fields = decision_matrix[
        decision_matrix["Major_crop_type"] == "Potential_split_field"
    ]
    bulk_template = bulk_template[
        bulk_template["FIELD_NAME"].isin(verified_fields["Field_name"])
        & ~bulk_template["FIELD_NAME"].isin(split_fields["Field_name"])
    ]

    # Exclusion case 2: split-field
    fields_before = verified_fields[~verified_fields["Field_name"].isin(diff)][
        "Field_name"
    ].unique()
    diff = np.setdiff1d(
        fields_before, bulk_template["FIELD_NAME"].unique(), assume_unique=True
    )
    excl_case = pd.DataFrame(data={"Case": ["split-field"], "Excluded": [None]})

    for i, f in zip(range(len(diff)), diff, strict=True):
        excl_case.at[i, "Case"] = "split-field"
        excl_case.at[i, "Excluded"] = f

    exclusions = pd.concat([exclusions, excl_case])

    # Some application products don't have a crop type, if the field only has 1 crop, let's use that value
    # If a given field has more than 1 crop, we cannot trust the values, therefore we won't set the crop.
    # As a result, records w missing crop types will be removed and the grower app will use defaults instead
    unique_crops = (
        bulk_template.dropna(subset=["CROP_TYPE"])
        .groupby("FIELD_NAME")["CROP_TYPE"]
        .unique()
    )
    for field, crops in unique_crops.items():
        # Check for multiple values or missing values
        if len(crops) > 1:
            log.warning(f"Multiple crop types found for field '{field}'")
        elif pd.isna(crops):
            log.warning(f"Missing crop type for field '{field}'")
        else:
            condition = (bulk_template["FIELD_NAME"] == field) & bulk_template[
                "CROP_TYPE"
            ].isna()
            bulk_template.loc[condition, "CROP_TYPE"] = crops[0]

    log.info("Adding field practice decisions from decision matrix")
    bulk_template = add_field_practice_decision_from_matrix(
        decision_matrix, bulk_template
    )

    """
    #####################################################################################
    ##### GP23 ADDITIONS - FD-CIC2022 ###################################################
    #####################################################################################
    """
    log.debug("Adding/extracting FD-CIC22 manure params")
    bulk_template = add_additional_manure_params_to_bulk(bulk_template)

    log.info("Adjusting parameters")
    bulk_template = adjust_parameters_after_mapping(bulk_template)

    # Filtering out unnecessary acreage/are which are really not applied product records
    fields_before = bulk_template["FIELD_NAME"].unique()
    bulk_template = bulk_template[~bulk_template["INPUT_UNIT"].isin(["AC", "are"])]
    diff = np.setdiff1d(
        fields_before, bulk_template["FIELD_NAME"].unique(), assume_unique=True
    )

    log.info(
        "excluded fields:\n",
    )
    # Set unmarked rows to major_crop_type and add reference acreage.
    # TODO: integrate this into the createion of bulk upload overview
    bulk_template["CROP_TYPE"] = bulk_template.apply(
        lambda x: fill_crop_type_in_bulk(
            x["FIELD_NAME"], x["CROP_TYPE"], decision_matrix
        ),
        axis=1,
    )
    bulk_template["REFERENCE_ACREAGE"] = bulk_template.apply(
        lambda x: fill_ref_acreage_in_bulk(
            x["FIELD_NAME"], x["REFERENCE_ACREAGE"], x["CROP_TYPE"], decision_matrix
        ),
        axis=1,
    )

    # Final filter - remove any rows missing reference acreage or crop is not corn
    fields_before = bulk_template["FIELD_NAME"].unique()
    bulk_template = bulk_template[
        bulk_template["REFERENCE_ACREAGE"].notnull()
        & bulk_template["CROP_TYPE"].isin(["Corn"])
    ]

    # Exclusion case 3: missing reference acreage or non-corn fields
    diff = np.setdiff1d(
        fields_before, bulk_template["FIELD_NAME"].unique(), assume_unique=True
    )

    ref_ac_reasons = comprehensive_inputs[
        ~pd.notna(comprehensive_inputs["Reference_acreage"])
        & pd.notna(comprehensive_inputs["Exclusion_reason"])
    ][["Field_name", "Crop_type", "Exclusion_reason"]].drop_duplicates()

    excl_case = pd.DataFrame(
        data={
            "Case": ["ref-ac/non-corn"],
            "Excluded": [None],
            "Crop_type": [None],
            "Reason": [None],
        }
    )

    for i, f in zip(range(len(diff)), diff, strict=True):
        excl_case.at[i, "Case"] = "ref-ac/non-corn"
        excl_case.at[i, "Excluded"] = f
        matching_reason = ref_ac_reasons[ref_ac_reasons["Field_name"] == f]
        if not matching_reason.empty:
            excl_case.at[i, "Crop_type"] = matching_reason["Crop_type"].iloc[0]
            excl_case.at[i, "Reason"] = matching_reason["Exclusion_reason"].iloc[0]
    exclusions = pd.concat([exclusions, excl_case])

    # DPREP-349 Temporary stop gap - replacing all fuel values and removing any pesticide/insecticide
    columns_to_set_nan = [
        "DIESEL",
        "GASOLINE",
        "NATURAL GAS",
        "LIQUEFIED PETROLEUM GAS",
        "ELECTRICITY",
    ]
    bulk_template[columns_to_set_nan] = float("nan")
    columns_to_set_none = [
        "DIESEL_UNIT",
        "GAS_UNIT",
        "NG_UNIT",
        "LPG_UNIT",
        "ELECTRICITY_UNIT",
    ]
    bulk_template[columns_to_set_none] = None
    rows_to_remove = ["HERBICIDE", "PESTICIDE", "FUNGICIDE", "INSECTICIDE"]
    bulk_template = bulk_template[~bulk_template["INPUT_TYPE"].isin(rows_to_remove)]

    # after all is done: set ID
    bulk_template["ID"] = np.arange(1, len(bulk_template) + 1)

    return bulk_template, exclusions
