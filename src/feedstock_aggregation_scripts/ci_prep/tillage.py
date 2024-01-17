import pandas as pd
from loguru import logger as log


def determine_till_depth(
    area_applied: float | None, till_depth: float | None
) -> float | None:
    """Returns value for tillage depth, based on parameters supplied.
    If both `area_applied` and `till_depth` is NaN, the assumption
    is, that no tillage records are found in the data. The function,
    hence, return None as tillage depth.

    In all other cases, the (maximum) tillage depth in inches is
    returned.
    """
    if pd.isna(area_applied) and pd.isna(till_depth):
        return None
    else:
        return till_depth


def determine_till_passes(
    area_applied: float | None, reference_acreage: float | None
) -> float | None:
    """Returns number of tillage passes based on `area_applied`. In cases where
    NaN is supplied for `area_applied`, the assumption is that no tillage
    records are available and the function returns None as tillage passes,
    accordingly.

    Where no reference_acreage is available, no value is returned.
    """

    if pd.isna(area_applied) or pd.isna(reference_acreage) or reference_acreage == 0.0:
        return None

    else:
        return area_applied / reference_acreage


def add_tillage_params(
    decisions: pd.DataFrame, bulk_upload_overview: pd.DataFrame
) -> pd.DataFrame:
    """Adds columns `Till_depth` and `Till_passes` to data frame
    `decisions`. `bulk_upload_overview` contains a set of field
    operations that was merged from all available data sources.

    `Till_depth` is based on the maximum tillage depth across
    all tillage passes recorded in `bulk_upload_overview`.

    `Till_passes` is the total number of acres tilled (`Area_applied`)
    devided by the reference acreage for a given field.
    """
    temp = bulk_upload_overview[
        bulk_upload_overview["Operation_type"].isin(["Tillage"])
    ]

    area_applied = (
        temp[["Field_name", "Area_applied"]]
        .groupby(by="Field_name", as_index=False, dropna=False)
        .sum()
    )

    applied_rate = (
        temp[["Field_name", "Applied_rate"]]
        .groupby(by="Field_name", as_index=False, dropna=False)
        .max()
    )

    reference_acreage = bulk_upload_overview[
        ["Field_name", "Reference_acreage"]
    ].drop_duplicates(ignore_index=True)

    # Combine all extracted information with the decision matrix
    for df in [area_applied, applied_rate, reference_acreage]:
        decisions = pd.merge(decisions, df, on="Field_name", how="left")

    # Add tillage metrics to decision matrix
    decisions = decisions.rename(columns={"Applied_rate": "Till_depth"})

    decisions["Till_depth"] = decisions.apply(
        lambda x: determine_till_depth(x["Area_applied"], x["Till_depth"]), axis=1
    )

    decisions["Till_passes"] = decisions.apply(
        lambda x: determine_till_passes(x["Area_applied"], x["Reference_acreage"]),
        axis=1,
    )

    # Rename `Reference_acreage` to match the column name
    # in the bulk upload template
    decisions = decisions.rename(columns={"Reference_acreage": "REFERENCE_ACREAGE"})

    return decisions


def add_tillage_practice_decision(field, till_depth, till_passes):
    # This function implements the Verity STIR Lite methodology,
    # which classifies tillage practices for GREET's FD-CIC
    # model based on the tillage depth and the number of
    # tillage passes per field.
    #
    # CONVENTIONAL_TILLAGE: > 3 tillage passes or a tillage
    # depth of > 3 inches.
    #
    # REDUCED_TILLAGE: up to 3 tillage passes and a tillage depth
    # that is <= 3 inches.
    #
    # NO_TILLAGE: < 1 tillage passes and no tillage depth.
    if pd.isna(till_depth) and pd.isna(till_passes):
        log.warning(
            f"Unable to determine tillage practice for field {field} with depth: {till_depth}, and passes: {till_passes}"
        )
        return None
    elif (
        (till_depth > 3.0 or till_passes > 3.0)
        or (pd.isna(till_passes) and till_depth > 3.0)
        or (pd.isna(till_depth) and till_passes > 3.0)
    ):
        return "CONVENTIONAL_TILLAGE"

    elif (
        (3.0 >= till_depth >= 0.0 and 3.0 >= till_passes >= 1.0)
        or (pd.isna(till_depth) and 3.0 >= till_passes >= 1.0)
        or (3.0 >= till_depth >= 0.0 and pd.isna(till_passes))
    ):
        return "REDUCED_TILLAGE"

    elif till_depth == 0 and till_passes < 1.0:
        return "NO_TILLAGE"
