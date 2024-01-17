import pandas as pd


def add_cover_crop_use_decision(field: str, cc_report: pd.DataFrame) -> bool:
    temp = cc_report[cc_report["Field_name"].isin([field])]

    if temp.empty:
        return False
    else:
        if (temp["Area_coverage_percent"] > 0.50).any():
            return True
        return False
