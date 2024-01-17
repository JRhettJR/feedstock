import numpy as np
import pandas as pd
from loguru import logger as log

from ..ci_prep.helpers import get_datetime
from ..config import settings
from ..data_prep.constants import NOT_FOUND
from ..util.cleaners.helpers import add_missing_columns
from ..util.conversion import convert_quantity_by_unit

# from ..data_prep.npk_breakdowns import get_fert_list
from ..util.readers.general import get_breakdown_list, get_cover_crop_list
from ..util.readers.generated_reports import read_attestation_overwrite
from .constants import (
    ACRE_PER_HECTARE,
    CC_TYPES,
    DEFAULT_CC_APPL_EN,
    DEFAULT_CC_HERB_AI,
    DEFAULT_MANURE_APPL_EN,
    DEFAULT_MANURE_TRANS_DIST,
    DEFAULT_MANURE_TRANS_EN,
    DIESEL_BTU_PER_GAL,
    G_PER_LBS,
    KG_PER_LBS,
)


def separate_unit_per_acre(unit: str) -> tuple[str, bool]:
    split = []
    per_acre = False

    if "ac" in unit.lower() or "acre" in unit.lower() or "acres" in unit.lower():
        if "/" in unit:
            split = unit.split("/")

        elif "per" in unit:
            split = unit.split("per")

        else:
            log.error(f"unable to separate unit per acre for {unit}")
            return "", per_acre

        if len(split) != 2:
            log.error(f"unexpected length for unit split: {split}")
            return "", per_acre

        unit = split[0].strip()
        per_acre = True

    return unit, per_acre


def do_lime(bulk: pd.DataFrame, field: str, vals: dict):
    bulk_temp = bulk[bulk["FIELD_NAME"].isin([field])]
    # Extract information from attestation row
    growing_cycle = vals.get("Growing_cycle", NOT_FOUND)
    if growing_cycle == NOT_FOUND or pd.isna(growing_cycle):
        growing_cycle = bulk_temp["GROWING_CYCLE"].unique().iloc[0]

    op_start = vals.get("Operation_start", NOT_FOUND)

    input_val = vals.get("Input_value", NOT_FOUND)
    input_unit = vals.get("Input_unit", NOT_FOUND)
    if input_unit not in ["LBS", "T", "TN", "KG", "G"]:
        log.error(
            "input unit for lime needs to be one of dry units: ['LBS', 'T', 'TN', 'KG', 'G']"
        )
        return bulk

    area_applied = vals.get("Area_applied", NOT_FOUND)

    # 1. existing lime ops
    bulk = bulk.drop(
        bulk[
            bulk["FIELD_NAME"].isin([field]) & (bulk["INPUT_NAME"].isin(["Lime"]))
        ].index
    )

    # 2. Add artificial op
    op = pd.DataFrame(columns=bulk.columns, index=[None])
    # Set values of artificial operation
    op["DATA SOURCE"] = "Verity"
    op["FIELD_NAME"] = field
    op["CROP_TYPE"] = "Corn"
    op["GROWING_CYCLE"] = int(growing_cycle)

    op["OPERATION_NAME"] = "Lime application"
    op["OPERATION_TYPE"] = "APPLYING_PRODUCTS"
    # Setting operation time to arbitrary time when not given
    op["OPERATION_START"] = (
        op_start if not pd.isna(op_start) else get_datetime(int(growing_cycle), 6, 1)
    )

    # Copy tillage practice from other field ops
    op["TILL_PRACTICE"] = bulk_temp["TILL_PRACTICE"].iloc[0]

    op["INPUT_NAME"] = "Lime"
    op["INPUT_TYPE"] = "FERTILIZER"
    op["INPUT_RATE"] = float(input_val)
    op["INPUT_UNIT"] = str(input_unit)
    op["INPUT_ACRES"] = float(area_applied)

    op["GREEN_AMMONIA"] = bulk_temp["GREEN_AMMONIA"].iloc[0]
    op["N_MGT_PRACTICE"] = bulk_temp["N_MGT_PRACTICE"].iloc[0]
    op["REFERENCE_ACREAGE"] = bulk_temp["REFERENCE_ACREAGE"].iloc[0]
    op["MANURE_USE"] = bulk_temp["MANURE_USE"].iloc[0]
    op["COVER_CROP_USE"] = bulk_temp["COVER_CROP_USE"].iloc[0]

    # Add to bulk
    return pd.DataFrame(np.vstack((bulk, op)), columns=bulk.columns)


def do_potash(bulk: pd.DataFrame, field: str, vals: dict):
    bulk_temp = bulk[bulk["FIELD_NAME"].isin([field])]
    # Extract information from attestation row
    growing_cycle = vals.get("Growing_cycle", NOT_FOUND)
    if growing_cycle == NOT_FOUND or pd.isna(growing_cycle):
        growing_cycle = bulk_temp["GROWING_CYCLE"].unique().iloc[0]

    op_start = vals.get("Operation_start", NOT_FOUND)

    input_val = vals.get("Input_value", NOT_FOUND)
    input_unit = vals.get("Input_unit", NOT_FOUND)
    if input_unit not in ["LBS", "T", "TN", "KG", "G"]:
        log.error(
            "input unit for potash needs to be one of dry units: ['LBS', 'T', 'TN', 'KG', 'G']"
        )
        return bulk

    area_applied = vals.get("Area_applied", NOT_FOUND)

    # 1. existing lime ops
    bulk = bulk.drop(
        bulk[
            bulk["FIELD_NAME"].isin([field]) & (bulk["INPUT_NAME"].isin(["Potash 60"]))
        ].index
    )

    # 2. Add artificial op
    op = pd.DataFrame(columns=bulk.columns, index=[None])
    # Set values of artificial operation
    op["DATA SOURCE"] = "Verity"
    op["FIELD_NAME"] = field
    op["CROP_TYPE"] = "Corn"
    op["GROWING_CYCLE"] = int(growing_cycle)

    op["OPERATION_NAME"] = "Potash application"
    op["OPERATION_TYPE"] = "APPLYING_PRODUCTS"
    # Setting operation time to arbitrary time when not given
    op["OPERATION_START"] = (
        op_start if not pd.isna(op_start) else get_datetime(int(growing_cycle), 6, 1)
    )

    # Copy tillage practice from other field ops
    op["TILL_PRACTICE"] = bulk_temp["TILL_PRACTICE"].iloc[0]

    op["INPUT_NAME"] = "Potash 60"
    op["INPUT_TYPE"] = "FERTILIZER"
    op["INPUT_RATE"] = float(input_val)
    op["INPUT_UNIT"] = str(input_unit)
    op["INPUT_ACRES"] = float(area_applied)

    op["GREEN_AMMONIA"] = bulk_temp["GREEN_AMMONIA"].iloc[0]
    op["N_MGT_PRACTICE"] = bulk_temp["N_MGT_PRACTICE"].iloc[0]
    op["REFERENCE_ACREAGE"] = bulk_temp["REFERENCE_ACREAGE"].iloc[0]
    op["MANURE_USE"] = bulk_temp["MANURE_USE"].iloc[0]
    op["COVER_CROP_USE"] = bulk_temp["COVER_CROP_USE"].iloc[0]
    # Add to bulk
    return pd.DataFrame(np.vstack((bulk, op)), columns=bulk.columns)


def do_pure_p2o5(bulk: pd.DataFrame, field: str, vals: dict):
    bulk_temp = bulk[bulk["FIELD_NAME"].isin([field])]
    # Extract information from attestation row
    growing_cycle = vals.get("Growing_cycle", NOT_FOUND)
    if growing_cycle == NOT_FOUND or pd.isna(growing_cycle):
        growing_cycle = bulk_temp["GROWING_CYCLE"].unique().iloc[0]

    op_start = vals.get("Operation_start", NOT_FOUND)

    input_val = vals.get("Input_value", NOT_FOUND)
    input_unit = vals.get("Input_unit", NOT_FOUND)
    if input_unit not in ["LBS", "T", "TN", "KG", "G"]:
        log.error(
            "input unit for potash needs to be one of dry units: ['LBS', 'T', 'TN', 'KG', 'G']"
        )
        return bulk

    area_applied = vals.get("Area_applied", NOT_FOUND)

    # 1. existing lime ops
    bulk = bulk.drop(
        bulk[
            bulk["FIELD_NAME"].isin([field]) & (bulk["INPUT_NAME"].isin(["Pure P2O5"]))
        ].index
    )

    # 2. Add artificial op
    op = pd.DataFrame(columns=bulk.columns, index=[None])
    # Set values of artificial operation
    op["DATA SOURCE"] = "Verity"
    op["FIELD_NAME"] = field
    op["CROP_TYPE"] = "Corn"
    op["GROWING_CYCLE"] = int(growing_cycle)

    op["OPERATION_NAME"] = "P2O5 application"
    op["OPERATION_TYPE"] = "APPLYING_PRODUCTS"
    # Setting operation time to arbitrary time when not given
    op["OPERATION_START"] = (
        op_start if not pd.isna(op_start) else get_datetime(int(growing_cycle), 6, 1)
    )

    # Copy tillage practice from other field ops
    op["TILL_PRACTICE"] = bulk_temp["TILL_PRACTICE"].iloc[0]

    op["INPUT_NAME"] = "Pure P2O5"
    op["INPUT_TYPE"] = "FERTILIZER"
    op["INPUT_RATE"] = float(input_val)
    op["INPUT_UNIT"] = str(input_unit)
    op["INPUT_ACRES"] = float(area_applied)

    op["GREEN_AMMONIA"] = bulk_temp["GREEN_AMMONIA"].iloc[0]
    op["N_MGT_PRACTICE"] = bulk_temp["N_MGT_PRACTICE"].iloc[0]
    op["REFERENCE_ACREAGE"] = bulk_temp["REFERENCE_ACREAGE"].iloc[0]
    op["MANURE_USE"] = bulk_temp["MANURE_USE"].iloc[0]
    op["COVER_CROP_USE"] = bulk_temp["COVER_CROP_USE"].iloc[0]
    # Add to bulk
    return pd.DataFrame(np.vstack((bulk, op)), columns=bulk.columns)


def do_product(bulk: pd.DataFrame, field: str, vals: dict):
    """Generalized function to add a field operation with a specific product
    to the bulk. The product needs to be part of the Verity Chemical Input Product
    Breakdown table to be a valid product.

    A product can be added on top of existing field operations (`Drop_existing` == False)
    or replace all operations of the respective product (`Drop_existing` == True). If the
    column `Drop_existing` is not present in the attestation overwrite file (`vals`), the
    function assumes that existing field operations should be dropped. This way, the
    previous attestation overwrite files do not need to be changed when using this
    functionality.
    """
    product = vals.get("Input_type", NOT_FOUND)

    # Prepare fertility info
    fert_list = get_breakdown_list(settings.data_prep.source_path)
    fert_product = fert_list[fert_list["product_name"].isin([product])]

    bulk_temp = bulk[bulk["FIELD_NAME"].isin([field])]
    # Extract information from attestation row
    growing_cycle = vals.get("Growing_cycle", NOT_FOUND)
    if growing_cycle == NOT_FOUND or pd.isna(growing_cycle):
        growing_cycle = bulk_temp["GROWING_CYCLE"].unique().iloc[0]

    op_start = vals.get("Operation_start", NOT_FOUND)

    input_val = vals.get("Input_value", NOT_FOUND)

    input_unit = vals.get("Input_unit", NOT_FOUND)
    per_acre = False
    # Separate per acre units
    if (
        "ac" in input_unit.lower()
        or "acre" in input_unit.lower()
        or "acres" in input_unit.lower()
    ):
        input_unit, per_acre = separate_unit_per_acre(input_unit)

    if input_unit not in ["LBS", "T", "TN", "KG", "G", "GAL"]:
        log.error(
            "input unit for potash needs to be one of dry units: ['LBS', 'T', 'TN', 'KG', 'G', 'GAL']"
        )
        return bulk

    area_applied = vals.get("Area_applied", NOT_FOUND)

    drop = vals.get("Drop_existing", NOT_FOUND)
    # Assume to drop existing records, if column "Drop" not in attestation overwrite file.
    # This avoids to change all existing files to make them work with this logic.
    if drop == NOT_FOUND:
        drop = True

    # 1. existing lime ops
    if drop:
        bulk = bulk.drop(
            bulk[
                bulk["FIELD_NAME"].isin([field]) & (bulk["INPUT_NAME"].isin([product]))
            ].index
        )

    # 2. Add artificial op
    op = pd.DataFrame(columns=bulk.columns, index=[None])
    # Set values of artificial operation
    op["DATA SOURCE"] = "Verity"
    op["FIELD_NAME"] = field
    op["CROP_TYPE"] = "Corn"
    op["GROWING_CYCLE"] = int(growing_cycle)

    op["OPERATION_NAME"] = product + " application"
    op["OPERATION_TYPE"] = "APPLYING_PRODUCTS"
    # Setting operation time to arbitrary time when not given
    op["OPERATION_START"] = (
        op_start if not pd.isna(op_start) else get_datetime(int(growing_cycle), 6, 1)
    )

    # Copy tillage practice from other field ops
    op["TILL_PRACTICE"] = bulk_temp["TILL_PRACTICE"].iloc[0]

    op["INPUT_NAME"] = product
    op["INPUT_TYPE"] = fert_product["product_type"].iloc[0].upper()
    # for per acre inputs, multiply by reference acreage
    op["INPUT_RATE"] = (
        float(input_val)
        if not per_acre
        else float(input_val) * bulk_temp["REFERENCE_ACREAGE"].iloc[0]
    )
    op["INPUT_UNIT"] = str(input_unit)
    op["INPUT_ACRES"] = float(area_applied)

    op["GREEN_AMMONIA"] = bulk_temp["GREEN_AMMONIA"].iloc[0]
    op["N_MGT_PRACTICE"] = bulk_temp["N_MGT_PRACTICE"].iloc[0]
    op["REFERENCE_ACREAGE"] = bulk_temp["REFERENCE_ACREAGE"].iloc[0]
    op["MANURE_USE"] = bulk_temp["MANURE_USE"].iloc[0]
    op["COVER_CROP_USE"] = bulk_temp["COVER_CROP_USE"].iloc[0]
    # Add to bulk
    return pd.DataFrame(np.vstack((bulk, op)), columns=bulk.columns)


def do_n_management(bulk: pd.DataFrame, field: str, vals: dict):
    bulk_temp = bulk[bulk["FIELD_NAME"].isin([field])]
    # Extract information from attestation row
    input_val = vals.get("Input_value", NOT_FOUND)

    if input_val not in ["EEF", "4R"]:
        log.error("input unit for n_mgt needs to be one of dry units: ['EEF', '4R']")
        return bulk

    current_n_mgt = bulk_temp["N_MGT_PRACTICE"].iloc[0]
    # do not overwrite existing 4R
    if current_n_mgt == "4R":
        return bulk

    bulk.loc[(bulk["FIELD_NAME"].isin([field])), "N_MGT_PRACTICE"] = input_val

    return bulk


"""
#####################################################################################
##### GP23 ADDITIONS - FD-CIC2022 (DS-272) ##########################################
#####################################################################################
"""


def estimate_cc_yield(cc_type: str, reference_acreage: float) -> float:
    """Estimates the total plant matter of a given `cc_type`.

    Returns total amount in dry tons."""
    cc_list = get_cover_crop_list(settings.data_prep.source_path)

    cc_product = cc_list[cc_list["Cover_crop_type"].isin([cc_type])]

    mt_per_ton = KG_PER_LBS / 1000 * 2000

    # Convert average yield (METRIC TONNE per HECTARE) to target measure (TN)
    est_total_yield = (
        cc_product["Yield_mt_per_hectare"].iloc[0]
        / ACRE_PER_HECTARE
        / mt_per_ton
        * reference_acreage
    )
    return est_total_yield


def prepare_field_op(bulk: pd.DataFrame, field: str, vals: dict, field_op_type: str):
    """Adds FD-CIC22 relevant inputs for manure from attestation overwrite file.

    A product can be added on top of existing field operations (`Drop_existing` == False)
    or replace all operations of the respective product (`Drop_existing` == True). If the
    column `Drop_existing` is not present in the attestation overwrite file (`vals`), the
    function assumes that existing field operations should be dropped. This way, the
    previous attestation overwrite files do not need to be changed when using this
    functionality.
    """
    if field_op_type not in ["manure", "cc"]:
        log.error(f"invalid field operation type: {field_op_type}")
        return bulk

    # Pre-filter to relevant field to extract existing field practices
    bulk_temp = bulk[bulk["FIELD_NAME"].isin([field])]
    # Extract reference acreage
    reference_acreage = bulk_temp["REFERENCE_ACREAGE"].iloc[0]

    product = vals.get("Input_product", NOT_FOUND)

    # Prepare fertility info
    fert_list = get_breakdown_list(settings.data_prep.source_path)
    fert_product = fert_list[fert_list["product_name"].isin([product])]

    # Extract information from attestation row
    growing_cycle = vals.get("Growing_cycle", NOT_FOUND)
    if growing_cycle == NOT_FOUND or pd.isna(growing_cycle):
        growing_cycle = bulk_temp["GROWING_CYCLE"].unique()[0]

    op_start = vals.get("Operation_start", NOT_FOUND)

    input_val = vals.get("Input_value", NOT_FOUND)
    if pd.isna(input_val) or input_val == NOT_FOUND and field_op_type != "cc":
        log.error(f"missing input value for field {field}, field op {field_op_type}")
        return bulk

    input_unit = vals.get("Input_unit", NOT_FOUND)
    if pd.isna(input_unit) or input_unit == NOT_FOUND:
        log.error(f"missing input unit for field {field}, field op {field_op_type}")
        return bulk

    # Separate per acre units
    input_unit, per_acre = separate_unit_per_acre(input_unit)

    if field_op_type != "cc" and input_unit not in ["LBS", "T", "TN", "KG", "G", "GAL"]:
        log.error(
            "input unit for potash needs to be one of dry units: ['LBS', 'T', 'TN', 'KG', 'G', 'GAL']"
        )
        return bulk

    area_applied = vals.get("Area_applied", NOT_FOUND)

    drop = vals.get("Drop_existing", NOT_FOUND)
    # Assume to drop existing records, if column "Drop" not in attestation overwrite file.
    # This avoids to change all existing files to make them work with this logic.
    if drop == NOT_FOUND:
        drop = True

    # 1. existing operations using the manure input product
    if drop:
        bulk = bulk.drop(
            bulk[
                bulk["FIELD_NAME"].isin([field]) & (bulk["INPUT_NAME"].isin([product]))
            ].index
        )

    # 2. Create artificial field operation
    op = pd.DataFrame(columns=bulk.columns, index=[None])
    # Set values of artificial operation
    op["DATA SOURCE"] = "Verity"
    op["FIELD_NAME"] = field
    op["CROP_TYPE"] = "Corn"
    op["GROWING_CYCLE"] = int(growing_cycle)

    op["OPERATION_NAME"] = f"{field_op_type.capitalize()} application"
    op["OPERATION_TYPE"] = "APPLYING_PRODUCTS"
    # Setting operation time to arbitrary time when not given
    op["OPERATION_START"] = (
        op_start if not pd.isna(op_start) else get_datetime(int(growing_cycle), 6, 1)
    )

    op["INPUT_NAME"] = product
    op["INPUT_TYPE"] = None
    # for per acre inputs, multiply by reference acreage
    op["INPUT_RATE"] = (
        float(input_val)
        if not per_acre
        else float(input_val) * bulk_temp["REFERENCE_ACREAGE"].iloc[0]
    )
    op["INPUT_UNIT"] = str(input_unit)
    op["INPUT_ACRES"] = float(area_applied)

    # Copy existing field practice values
    op["TILL_PRACTICE"] = bulk_temp["TILL_PRACTICE"].iloc[0]
    op["GREEN_AMMONIA"] = bulk_temp["GREEN_AMMONIA"].iloc[0]
    op["N_MGT_PRACTICE"] = bulk_temp["N_MGT_PRACTICE"].iloc[0]
    op["REFERENCE_ACREAGE"] = bulk_temp["REFERENCE_ACREAGE"].iloc[0]
    op["MANURE_USE"] = bulk_temp["MANURE_USE"].iloc[0]
    op["COVER_CROP_USE"] = bulk_temp["COVER_CROP_USE"].iloc[0]

    if field_op_type == "manure":
        #### Add FD-CIC22 specific parameters
        #
        # Cover crops are usually terminated by a herbicide product. Sometimes
        # the cover crop gets (partially) harvested.
        #
        # Params:
        # - `Input_product`: manure product applied.
        # - `fert_product`: corresponding product breakdown entry for `Input_product`.
        # - `manure_type`: manure type (should be one of ['Beef Cattle', 'Dairy Cow', 'Swine', 'Chicken']).
        # - `manure_trans_dist`: distance between manure source and farm.
        # - `Manure_trans_en`: energy used for transportation (assumed DIESEL)
        # - `Manure_trans_en_unit`: unit for `Manure_trans_en`
        # - `Manure_appl_en`: energy used for application (assumed DIESEL)

        # 2.1 - manure type
        manure_type = fert_product["manure_type"].iloc[0]

        if manure_type in ["Beef Cattle", "Dairy Cow", "Swine", "Chicken"]:
            op["MANURE_TYPE"] = manure_type
        else:
            op["MANURE_TYPE"] = "Other"
            log.warning(
                f"Manure type {manure_type} not within FD-CIC 2022 expected input range: ['Beef Cattle', 'Dairy Cow', 'Swine', 'Chicken']"
            )

        # 2.2 - dry matter equivalent in short tons
        # Convert input to GAL or LBS, respectively
        input_conv, _ = convert_quantity_by_unit(float(input_val), input_unit)
        dens_conv_factor = fert_product["lbs / gal"].iloc[0]
        # Calculate short tons of manure
        dry_quantity_equiv = input_conv * dens_conv_factor / 2000

        op["MANURE_DRY_QUANTITY_EQUIV"] = dry_quantity_equiv

        # 2.3 - manure transportation distance in miles
        manure_trans_dist = vals.get("Manure_trans_dist", NOT_FOUND)
        if manure_trans_dist == NOT_FOUND or pd.isna(manure_trans_dist):
            # assume default distance from FD-CIC 2022, if value unavailable
            manure_trans_dist = DEFAULT_MANURE_TRANS_DIST
        op["MANURE_TRANS_DIST"] = manure_trans_dist

        # 2.4 - manure transportation energy in Btu / ton of manure / mile

        # NOTE: Assumption = manure transportation energy of type DIESEL
        manure_trans_en = vals.get("Manure_trans_en", NOT_FOUND)
        manure_trans_en_unit = vals.get("Manure_trans_en_unit", NOT_FOUND)
        if (
            manure_trans_en != NOT_FOUND
            and not pd.isna(manure_trans_en)
            and manure_trans_en_unit != NOT_FOUND
            and not pd.isna(manure_trans_en_unit)
        ):
            trans_en_conv, _ = convert_quantity_by_unit(
                float(manure_trans_en), manure_trans_en_unit
            )
        else:
            # Create artificial default input energy value
            trans_en_conv = (
                DEFAULT_MANURE_TRANS_EN
                / DIESEL_BTU_PER_GAL
                * dry_quantity_equiv
                * manure_trans_dist
            )

        # Units = Btu / ton of manure / mile
        trans_en_conv = (
            trans_en_conv * DIESEL_BTU_PER_GAL / dry_quantity_equiv / manure_trans_dist
        )
        op["MANURE_TRANS_EN"] = trans_en_conv

        # 2.5 - manure application energy
        # NOTE: Assumption = manure application energy of type DIESEL in GAL
        manure_application_en = vals.get("Manure_appl_en", NOT_FOUND)

        if manure_application_en == NOT_FOUND or pd.isna(manure_application_en):
            # In case no energy value is given, create artificial total energy applied value
            # using GREET default for manure application energy.

            manure_application_en = DEFAULT_MANURE_APPL_EN * reference_acreage
        else:
            manure_application_en = manure_application_en * DIESEL_BTU_PER_GAL

        op["MANURE_APPL_EN"] = manure_application_en

    elif field_op_type == "cc":
        ### Add FD-CIC22 specific parameters for COVER CROPS
        #
        # Cover crops are usually terminated by a herbicide product. Sometimes
        # the cover crop gets (partially) harvested.
        #
        # Params:
        # - `cc_herb_product`: herbicide product applied for cover crop termination.
        # - `herb_product`: corresponding product breakdown entry for `cc_herb_product`.
        # - `cc_type`: cover crop type (must be part of `CC_TYPES`).
        # - `cc_herb_amount`: total amount of `CC_herb_product` applied.
        # - `cc_herb_unit`: unit for `cc_herb_amount`.
        # - `cc_herb_ai`: total amount of active ingredients applied, in G (gramm).
        # - `cc_yield`: total estimated yield for `cc_type` minus `CC_yield_harvested`.

        # 2.1 - cc type
        cc_type = vals.get("CC_type", None)

        if pd.isna(cc_type) or cc_type not in CC_TYPES:
            log.error(
                f"Missing or invalid cover crop type: {cc_type}; for field {field}"
            )
            return bulk

        op["CC_TYPE"] = cc_type

        # 2.2 - cc herbicide product
        cc_herb_product = vals.get("CC_herb_product", None)
        herb_product = fert_list[fert_list["product_name"].isin([cc_herb_product])]

        if herb_product.empty and not pd.isna(cc_herb_product):
            log.error(f"missing product breakdown for product {cc_herb_product}")
            return bulk

        op["CC_HERBICIDE_PRODUCT"] = cc_herb_product

        # 2.3 - cc herbicide amount applied
        if not pd.isna(cc_herb_product):
            # CASE: herbicide product was applied to terminate cover
            cc_herb_amount = vals.get("CC_herb_amount", None)
            cc_herb_unit = vals.get("CC_herb_unit", None)

            if not pd.isna(cc_herb_amount) and not pd.isna(cc_herb_unit):
                # Convert herbicide input into GAL
                herb_input_conv, _ = convert_quantity_by_unit(
                    float(cc_herb_amount), cc_herb_unit
                )

                ai_conv_factor = herb_product["lbs AI / gal"].iloc[0]

                cc_herb_ai = herb_input_conv * ai_conv_factor * G_PER_LBS
            else:
                log.warning(
                    "missing or invalid herbicide amount; setting to FD-CIC22 default"
                )
                # Set model default in case no input values were given
                cc_herb_ai = DEFAULT_CC_HERB_AI * reference_acreage
        else:
            cc_herb_ai = None

        op["CC_HERBICIDE_AI"] = cc_herb_ai

        # 2.4 - cc yield
        cc_yield_harvested = vals.get("CC_yield_harvested", None)
        est_total_yield = estimate_cc_yield(cc_type, reference_acreage)

        if not pd.isna(cc_yield_harvested):
            # NOTE: assuming that yield is given in TN / ACRE
            cc_yield_harvested = cc_yield_harvested * reference_acreage
        else:
            cc_yield_harvested = 0.0

        # Total matter minus harvested matter
        op["CC_YIELD"] = est_total_yield - cc_yield_harvested

        # 2.5 - cc application energy
        # NOTE: Assumption = cc application energy of type DIESEL in GAL
        cc_application_en = vals.get("CC_appl_en", None)

        if pd.isna(cc_application_en):
            # In case no energy value is given, create artificial total energy applied value
            # using GREET default for cover crop application energy.

            cc_application_en = DEFAULT_CC_APPL_EN * reference_acreage
        else:
            cc_application_en = cc_application_en * DIESEL_BTU_PER_GAL

        op["CC_APPL_EN"] = cc_application_en

        # 2.6 - cc N-factor
        cc_list = get_cover_crop_list(settings.data_prep.source_path)
        cc_entry = cc_list[cc_list.Cover_crop_type.isin([cc_type])]
        op["CC_N_FACTOR"] = cc_entry.N_content_total.iloc[0]

    # Add to bulk
    bulk = add_missing_columns(bulk, op.columns)

    return pd.DataFrame(np.vstack((bulk, op)), columns=bulk.columns)


""" ### END GP23 ADDITIONS ###################################################### """


def apply_attestations(
    bulk: pd.DataFrame, attest: pd.DataFrame, exclusions: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if attest.empty:
        log.info("no attestations available --> cancel attestation overwrite")
        return bulk, exclusions

    for field in bulk["FIELD_NAME"].unique():
        temp = attest[attest["Field_name"].isin([field])]
        # If no attestation entries are present, skip to next field
        if temp.empty:
            log.info(f"no attestation entries for field {field}")
            continue

        for _, row in temp.iterrows():
            # Extract information from attestation row
            vals = row.to_dict()

            attestation = vals.get("Input_type", NOT_FOUND)
            # print(field, attestation, vals)
            if attestation == NOT_FOUND or pd.isna(attestation):
                log.error(f"missing attestation type for field {field}")
                continue

            # to check for specific product overwrites
            fert_list = get_breakdown_list(settings.data_prep.source_path)

            # Check the type of attestation made:

            # IF `lime`:
            # 1. Eliminate existing lime operations.
            # 2. Add artificial lime operation with provided values
            if attestation.lower() == "lime":
                bulk = do_lime(bulk, field, vals)

            elif attestation.lower() == "potash":
                bulk = do_potash(bulk, field, vals)

            elif attestation.lower() == "pure_p2o5":
                bulk = do_pure_p2o5(bulk, field, vals)

            # IF `manure`:
            # Change manure to `True`
            elif attestation.lower() == "manure":
                bulk.loc[(bulk["FIELD_NAME"].isin([field])), "MANURE_USE"] = True

                """
                #####################################################################################
                ##### GP23 ADDITIONS - FD-CIC2022 (DS-272) ##########################################
                #####################################################################################
                """
                # bulk = do_manure(bulk, field, vals)
                bulk = prepare_field_op(
                    bulk, field, vals, field_op_type=attestation.lower()
                )
                """ ### END GP23 ADDITIONS ###################################################### """

            # IF `cc`:
            # Change cover crop use to `True`
            elif attestation.lower() == "cc":
                bulk.loc[(bulk["FIELD_NAME"].isin([field])), "COVER_CROP_USE"] = True

                """
                #####################################################################################
                ##### GP23 ADDITIONS - FD-CIC2022 (DS-272) ##########################################
                #####################################################################################
                """
                # bulk = do_manure(bulk, field, vals)
                bulk = prepare_field_op(
                    bulk, field, vals, field_op_type=attestation.lower()
                )
                """ ### END GP23 ADDITIONS ###################################################### """

            # IF `till`:
            # Set tillage practice to given input value. If no value is given,
            # no adjustments are made.
            elif attestation.lower() == "till":
                input_val = vals.get("Input_value", NOT_FOUND)
                if input_val == NOT_FOUND:
                    log.error(f"missing input value for attestation {attestation}")
                    continue

                # check for input value validity
                if input_val not in [
                    "CONVENTIONAL_TILLAGE",
                    "REDUCED_TILLAGE",
                    "NO_TILLAGE",
                ]:
                    log.error(
                        f"input value for attestation {attestation} must be one of: ['CONVENTIONAL_TILLAGE', 'REDUCED_TILLAGE', 'NO_TILLAGE']"
                    )
                    continue

                bulk.loc[
                    (bulk["FIELD_NAME"].isin([field])), "TILL_PRACTICE"
                ] = input_val

            # IF `fert`:
            # Exclude all operations with OPERATION_TYPE == FERTILIZER
            elif attestation.lower() == "fert":
                bulk.drop(
                    bulk[
                        bulk["FIELD_NAME"].isin([field])
                        & (bulk["INPUT_TYPE"].isin(["FERTILIZER"]))
                        & (~bulk["INPUT_NAME"].isin(["Lime"]))
                    ].index,
                    inplace=True,
                )

            # IF `n_mgt`:
            # Set input value if existing value is not a better benefit.
            # E.g. EEF < 4R --> conserve 4R
            elif attestation.lower() == "n_mgt":
                bulk = do_n_management(bulk, field, vals)

            # IF `product name`:
            # For any products contained in the chemical input product breakdown table,
            # artificial operations can be added. Spelling needs to agree with entry in
            # the respective table.
            elif attestation in list(fert_list["product_name"]):
                bulk = do_product(bulk, field, vals)

            elif "exclude" in attestation.lower():
                bulk.drop(bulk[bulk["FIELD_NAME"].isin([field])].index, inplace=True)
                # extract the exclusion case
                case = attestation.split(":")[-1]
                if case == NOT_FOUND:
                    log.warning("missing case for exclusion; set to 'manual'")
                    case = "manual"

                reason = vals.get("Input_value", NOT_FOUND)
                # add to exclusions report
                exclusions.loc[len(exclusions.index)] = (case, field, None, reason)

            else:
                log.error(f"unknown attestation type: {attestation}; field: {field}")

    return (
        bulk.sort_values(by=["FIELD_NAME", "OPERATION_START"], ascending=True),
        exclusions,
    )


def attestation_overwrite(
    bulk: pd.DataFrame, grower: str, growing_cycle: int, exclusions: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Handles attestation overwrites from file `{grower}_attestation_overwrite_{growing_cycle}.csv`.

    Overwrites are handles based on `Input_type` and `Field_name`. Depending on the case, different
    parameters are required to carry out the overwrite. "Manual" exclusions are added to the `exclusions`
    data frame passed in.

    For detailled examples, compare: https://veritytracking.atlassian.net/wiki/spaces/GEVO/pages/180879396/DT+-+Attestation+Overwrite
    """
    log.info(f"initiating attestation overwrite for grower {grower}...")
    if bulk.empty:
        log.error("bulk data unavailable")
        return pd.DataFrame(), exclusions
    path_to_data = settings.data_prep.source_path

    attest = read_attestation_overwrite(
        path_to_data=path_to_data, grower=grower, growing_cycle=growing_cycle
    )

    bulk, exclusions = apply_attestations(bulk, attest, exclusions)
    # After all is done: (re-)set ID
    bulk["ID"] = np.arange(1, len(bulk) + 1)

    return bulk, exclusions
