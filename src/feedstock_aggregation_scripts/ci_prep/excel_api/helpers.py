import pandas as pd

# MOVED TO `util/cleaners/helpers`
# def add_missing_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
#     temp = df.copy()
#     for col in columns:
#         if col not in temp.columns:
#             temp[col] = np.nan
#     return temp


def set_soc_inputs_to_default(inputs: pd.DataFrame) -> pd.DataFrame:
    inputs["Manure"] = "No manure"
    inputs["Cover_crop"] = "No cover crop"
    inputs["Tillage"] = "CONVENTIONAL"
    return inputs
