import pathlib

import pandas as pd
from fuzzywuzzy import fuzz, process

from ..util.readers.generated_reports import read_shp_file_overview

partial_match_ratio = 60


def fuzzy_match_field_names(
    source_df: pd.DataFrame, list1: pd.Series, list2: pd.Series, ratio: float
):
    # Helper to fuzzy match function below
    matches = []
    probabilities = []

    for i in list1:
        ratios = process.extractOne(i, list2, scorer=fuzz.token_sort_ratio)

        if int(ratios[1]) > ratio:  # can play with this number
            matches.append(ratios[0])
            probabilities.append(ratios[1])
        else:
            matches.append("no match")
            probabilities.append(None)

    df = source_df
    df["Potential_shp_match"] = matches
    df["Match_score"] = probabilities

    return df


def add_shp_file_name_comparison(
    decisions: pd.DataFrame,
    path_to_processed: str | pathlib.Path,
    grower: str,
):
    shp_overview = read_shp_file_overview(path_to_data=path_to_processed, grower=grower)

    temp = fuzzy_match_field_names(
        source_df=decisions,
        list1=decisions["Field_name"],
        list2=shp_overview["Field_name"],
        ratio=partial_match_ratio,
    )

    return temp
