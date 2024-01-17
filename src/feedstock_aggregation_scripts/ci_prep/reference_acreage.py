import pandas as pd
from loguru import logger as log


def select_reference_acreage(
    field_name: str, crop_type: str, reference_acreage: pd.DataFrame
) -> tuple[float | None, str | None]:
    """Implements the business rule decision on `Reference_acreage`:

    IF planted acreage available --> use planted acres

    ELSE IF harvested acreage available and close to the shp-file
            acreage of that field --> use harvested acres

    ELSE --> no reference acreage available

    In addition, to the general business rule, this function will
    favor the highest entry available for the decision category
    (planted vs. harvested acreage). A lower value for
    `Reference_acreage` leads to higher input values per input
    category for the model, and, thus, to higher CI scores as a
    result (conservative approach).
    Using the greater entries, we are giving credit to available
    data records across data sources.

    This additional rule is required as we are dealing with a
    combined data set of potential planted / harvested acres from
    various data aggregators. The data points in this data set may
    vary across data aggregator systems for the same field.
    """
    temp = reference_acreage[
        (reference_acreage["Field_name"].isin([field_name]))
        & (reference_acreage["Crop_type"].isin([crop_type]))
        & (~reference_acreage["Reference_acreage"].isna())
    ]
    if temp.empty:
        log.error(
            f"Missing reference acreage entries for field {field_name} with crop type {crop_type}"
        )
        return None, "Missing reference acreage completely"

    PLA_available = False
    if temp["PLA_available"].any():
        PLA_available = True

    reference = temp.loc[
        (temp["PLA_available"] == PLA_available) & (temp["Crop_type"] == "Corn")
    ]

    if not reference.empty:
        if len(reference) == 1:
            return (
                reference["Reference_acreage"].values[0],
                reference["Exclusion_reason"].values[0],
            )
        max_reference = reference.loc[
            reference["Reference_acreage"].idxmax(),
            ["Reference_acreage", "Exclusion_reason"],
        ]
        return max_reference["Reference_acreage"], max_reference["Exclusion_reason"]
    else:
        return None, temp["Exclusion_reason"].values[0]
