import pandas as pd
from loguru import logger as log


def init_product_breakdown():
    pb = {
        "Product": [],
        "Product_type": [],
        "Conversion_factor": [],
        "A": [],
        "U": [],
        "AN": [],
        "AS": [],
        "UAN": [],
        "MAP_N": [],
        "DAP_N": [],
        "MAP_P": [],
        "DAP_P": [],
        "K2O": [],
        "AI_H": [],  # HERBICIDES + FUNGICIDES
        "AI_I": [],  # INSECTICIDES
        "CACO3": [],  # LIME
    }
    return pb


cpb = pd.read_csv("01_data/verity_chemical_product_breakdown_table - Sheet1.csv")


def extract_product_breakdown_info(product_names):
    pb = init_product_breakdown()

    for product_name in product_names:
        temp = cpb[cpb.product_name == product_name]
        # skip not registered products
        # or temp.product_type.iloc[0].upper() != 'FERTILISER':
        if temp.empty or not isinstance(temp.product_type.iloc[0], str):
            continue

        pb["Product"].append(product_name)
        product_type = temp.product_type.iloc[0]

        if temp.empty:
            log.warning(f"no breakdown in DB for {product_name}")

            pb["Product_type"].append(None)
            pb["Conversion_factor"].append(None)
            pb["A"].append(None)
            pb["U"].append(None)
            pb["AN"].append(None)
            pb["AS"].append(None)
            pb["UAN"].append(None)
            pb["MAP_N"].append(None)
            pb["DAP_N"].append(None)
            pb["MAP_P"].append(None)
            pb["DAP_P"].append(None)
            pb["K2O"].append(None)
            pb["AI_H"].append(None)
            pb["AI_I"].append(None)
            pb["CACO3"].append(None)

            continue

        pb["Product_type"].append(product_type)
        pb["Conversion_factor"].append(temp["lbs / gal"].iloc[0])
        pb["A"].append(temp["% Ammonia"].iloc[0])
        pb["U"].append(temp["% Urea"].iloc[0])
        pb["AN"].append(temp["% AN"].iloc[0])
        pb["AS"].append(temp["% AS"].iloc[0])
        pb["UAN"].append(temp["% UAN"].iloc[0])
        pb["MAP_N"].append(temp["% MAP N"].iloc[0])
        pb["DAP_N"].append(temp["% DAP N"].iloc[0])
        pb["MAP_P"].append(temp["% MAP P2O5"].iloc[0])
        pb["DAP_P"].append(temp["% DAP P2O5"].iloc[0])
        pb["K2O"].append(temp["% K2O"].iloc[0])

        if product_type.upper() == "HERBICIDE" or product_type.upper() == "FUNGICIDE":
            pb["AI_H"].append(temp["lbs AI / gal"].iloc[0])
            pb["AI_I"].append(None)
            pb["CACO3"].append(None)

        elif product_type.upper() == "INSECTICIDE":
            pb["AI_H"].append(None)
            pb["AI_I"].append(temp["lbs AI / gal"].iloc[0])
            pb["CACO3"].append(None)
        elif product_type.upper() == "LIME":
            pb["AI_H"].append(None)
            pb["AI_I"].append(None)
            pb["CACO3"].append(temp["% CaCO3"].iloc[0])
        else:
            pb["AI_H"].append(None)
            pb["AI_I"].append(None)
            pb["CACO3"].append(None)

    return pd.DataFrame(pb)


def calculate_inputs_by_row(pb):
    """Adds columns with total input values to the passed data frame"""
    pb["Total_A"] = pb.Applied_total * pb.Conversion_factor * pb.A / pb.Area_applied
    pb["Total_U"] = pb.Applied_total * pb.Conversion_factor * pb.U / pb.Area_applied
    pb["Total_AN"] = pb.Applied_total * pb.Conversion_factor * pb.AN / pb.Area_applied
    pb["Total_AS"] = pb.Applied_total * pb.Conversion_factor * pb.AS / pb.Area_applied
    pb["Total_UAN"] = pb.Applied_total * pb.Conversion_factor * pb.UAN / pb.Area_applied
    pb["Total_MAP_N"] = (
        pb.Applied_total * pb.Conversion_factor * pb.MAP_N / pb.Area_applied
    )
    pb["Total_DAP_N"] = (
        pb.Applied_total * pb.Conversion_factor * pb.DAP_N / pb.Area_applied
    )
    pb["Total_MAP_P"] = (
        pb.Applied_total * pb.Conversion_factor * pb.MAP_P / pb.Area_applied
    )
    pb["Total_DAP_P"] = (
        pb.Applied_total * pb.Conversion_factor * pb.DAP_P / pb.Area_applied
    )
    pb["Total_K2O"] = pb.Applied_total * pb.Conversion_factor * pb.K2O / pb.Area_applied
    pb["Total_AI_H"] = pb.Applied_total * pb.AI_H / pb.Area_applied
    pb["Total_AI_I"] = pb.Applied_total * pb.AI_I / pb.Area_applied
    pb["Total_CACO3"] = pb.Applied_total * pb.CACO3 / pb.Area_applied

    return pb
