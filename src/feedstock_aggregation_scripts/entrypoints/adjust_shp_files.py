import pathlib

from src.feedstock_aggregation_scripts.config import settings

from ..shp_files.change_dbf import adjust_dbfs


def get_dbf_file_names(grower: str) -> list(str):
    path_to_data = settings.data_prep.source_path

    field_names = []
    folders = pathlib.Path(path_to_data).joinpath(grower, "shp-files").glob("[!.]*")
    for f in folders:
        # print(f)
        file = f.glob("*.dbf")
        # print(file)
        file = str(next(file))
        if file:
            field_names.append(file)

    return field_names


def get_change_dict_and_ids(grower: str) -> tuple[dict, dict]:
    key_dict = {}
    change_dict = {}

    if grower == "Field":
        change_dict = {"Fjelde Home South": "Mom and Dads", "Fjelde South": "Kochs"}

        key_dict = {
            "CLIENT": {
                "Field": "52af0d83-ecdb-432e-b681-034e12d68e0c",
            },
            "FARMS": {
                "RYLAND": "f23557ef-b7e2-4063-9724-dd147bca9a5a",
                "Fjelde Farms": "68156f11-0bf4-4970-b6af-564aa4d7c231",
                "Jensons": "511ef2e1-49a6-49be-aef8-692755197263",
                "Johnson": "84819104-b30a-43bc-ab02-18c270b83187",
                "Olivers": "f67d64f0-6aac-44ae-8d91-7b99748f8c63",
                "Ryland Farm": "159aa578-23de-4b16-80f0-28d0c9645b0f",
                "STYF": "0d1d2984-fefb-4547-aa5a-79399f87669e",
            },
        }
    return change_dict, key_dict


def run():
    """!!!THIS FUNCTION AS IS IS GROWER SPECIFIC!!!"""
    grower = "Field"

    field_names = get_dbf_file_names(grower)
    change_dict, key_dict = get_change_dict_and_ids(grower)
    # !!!ALL KEYS AND MAPPINGS ARE SPECIFIC TO FIELD!!!
    adjust_dbfs(field_names, grower, change_dict, key_dict)

    return True
