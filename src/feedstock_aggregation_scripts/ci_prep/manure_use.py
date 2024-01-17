import pandas as pd


def add_manure_use_decision(field: str, manure_report: pd.DataFrame) -> bool:
    temp = manure_report[manure_report["Field_name"].isin([field])]

    if temp.empty:
        return False
    else:
        if (temp["Area_coverage_percent"] > 0.50).any():
            return True
        return False
