# CONSTANTS

CORN = "Corn"
SOYBEAN = "Soybean"

INPUT_TYPE_FERTILIZER = "FERTILIZER"

FDCIC_DIESEL = "DIESEL"
FDCIC_GAS = "GAS"
FDCIC_LPG = "LPG"
FDCIC_NG = "NG"
FDCIC_ELECTRICITY = "ELECTRICITY"

FDCIC_A = "A"
FDCIC_U = "U"
FDCIC_AN = "AN"
FDCIC_AS = "AS"
FDCIC_UAN = "UAN"
FDCIC_MAP_N = "MAP N"
FDCIC_DAP_N = "DAP N"
FDCIC_N_FERTILISER = "N FERTILISER"
FDCIC_N_FERTILISERS = [
    FDCIC_A,
    FDCIC_U,
    FDCIC_AN,
    FDCIC_AS,
    FDCIC_UAN,
    FDCIC_MAP_N,
    FDCIC_DAP_N,
]

FDCIC_MAP_P = "MAP P"
FDCIC_DAP_P = "DAP P"
FDCIC_P2O5_FERTILISER = "P2O5 FERTILISER"
FDCIC_P2O5_FERTILISERS = [FDCIC_MAP_P, FDCIC_DAP_P]

FDCIC_K2O_FERTILISER = "K2O"

FDCIC_LIME = "LIME"

FDCIC_HERBICIDE = "AI H"
FDCIC_INSECTICIDE = "AI I"

FDCIC_INPUTS = [
    FDCIC_DIESEL,
    FDCIC_GAS,
    FDCIC_NG,
    FDCIC_LPG,
    FDCIC_ELECTRICITY,
    *FDCIC_N_FERTILISERS,
    *FDCIC_P2O5_FERTILISERS,
    FDCIC_K2O_FERTILISER,
    FDCIC_LIME,
    FDCIC_HERBICIDE,
    FDCIC_INSECTICIDE,
]

COL_ORDER = [
    "Client",
    "Field_name",
    "Area_applied",
    "Crop_type",
    "Total_yield",
    "Total_diesel",
    "Total_gas",
    "Total_ng",
    "Total_lpg",
    "Total_electricity",
    "Total_A",
    "Total_U",
    "Total_AN",
    "Total_AS",
    "Total_UAN",
    "Total_MAP_N",
    "Total_DAP_N",
    "Total_MAP_P",
    "Total_DAP_P",
    "Total_K2O",
    "Total_lime",
    "Total_AI_H",
    "Total_AI_I",
    "Manure",
    "Cover_crop",
    "Tillage",
]


BULK_TEMPLATE_COLS = [
    # 1, INT; running number 1 - n
    "ID",
    # 2, STR; name of the data aggregatort / source -> `Data_source` in cleaned data
    "DATA SOURCE",
    # 3, STR; not used in GP22
    "FIELD_ID",
    # 4, STR; `Field_name` in cleaned data
    "FIELD_NAME",
    # 5, ENUM (CORN SOYBEAN SORGHUM SUGAR_CANE COTTON WHEAT RICE); `Crop_type` in cleaned data
    "CROP_TYPE",
    # 6, INT;
    "GROWING_CYCLE",
    # 7, FLOAT; in bushel / acre -> `Total_dry_yield` in cleaned data (part of `Applied_total` where `Operation_type == Harvest`)
    "YIELD",
    # 8, FLOAT; 0.15 = 15% -> not used in DP22
    "MOISTURE_AT_HARVEST",
    # All fuel related intputs will be set to defaults for GP22
    # 9, FLOAT; -> ususally `Total_fuel` in cleaned data
    "DIESEL",
    # 10, ENUM (GAL LITER); -> usually `Fuel_unit` in cleaned data
    "DIESEL_UNIT",
    "GASOLINE",  # 11, FLOAT;
    "GAS_UNIT",  # 12, ENUM (GAL LITER);
    "NATURAL GAS",  # 13, FLOAT;
    "NG_UNIT",  # 14, ENUM (FT3);
    "LIQUEFIED PETROLEUM GAS",  # 15, FLOAT;
    "LPG_UNIT",  # 16, ENUM (GAL LITER);
    "ELECTRICITY",  # 17, FLOAT;
    "ELECTRICITY_UNIT",  # 18, ENUM (KWH);
    # 19, ENUM (SENSOR CONTROLLED (someone with expertise controlled the stated value(s)) CLAIMED (only stated, not controlled));
    "DATA_QUALITY_ENERGY",
    # -> not used in GP22
    "OPERATION_NAME",  # 20, STR;
    # 21, ENUM (TILLAGE APPLYING_PRODUCTS IRRIGATION DRY_DOWN HARVEST); `Operation_type` in cleaned data (with some adjustments)
    "OPERATION_TYPE",
    "CUSTOM_APP",  # 22, BOOL; not used in GP22
    # 23, ENUM (SENSOR CONTROLLED (someone with expertise controlled the stated value(s)) CLAIMED (only stated, not controlled));
    "DATA_QUALITY_OPERATION",
    # -> not used in GP22
    "OPERATION_START",  # 24, DATETIME; `Operation_start` in cleaned data
    "OPERATION_END",  # 25, DATETIME; `Operation_end` in cleaned data
    "IMPLEMENT_ID",  # 26, STR; not used in GP22
    "NUM_TILL_PASSES",  # 27, FLOAT;
    "TILL_DEPTH",  # 28, FLOAT; in inches
    # 29, ENUM (CONVENTIONAL_TILLAGE DISK_TILLAGE REDUCED_TILLAGE STRIP_TILLAGE NO_TILLAGE);
    "TILL_PRACTICE",
    # -> needs to be derived from `Operation_type == Tillage` ops
    "INPUT_FORMULA",  # 30, STR; not used
    "INPUT_NAME",  # 31, STR; `Product` in cleaned data
    # 32, ENUM (FERTILIZER SEED HERBICIDE FUNGICIDE INSECTICIDE empty); derived by `Product` and the chemical
    "INPUT_TYPE",
    # input product breakdown table
    "INPUT_RATE",  # 33, FLOAT; `Applied_total` in cleaned data
    # 34, ENUM (GAL LBS PINT FL_OZ QT G T (metric ton) TN (short ton));
    "INPUT_UNIT",
    # 35, FLOAT; the surface on which `INPUT_RATE` was actually applied
    "INPUT_ACRES",
    "DILUTION_FACTOR",  # 36, FLOAT; not used
    "CUSTOM_BLEND",  # 37, BOOL; not used
    # 38, BOOL; False (CONVENTIONAL Ammonia) by default (compare business rules methodology v1)
    "GREEN_AMMONIA",
    # 39, ENUM (4R EEF BAU); -> need to include business rule to determine
    "N_MGT_PRACTICE",
    # 40, FLOAT; according to the business rule, filled for every row
    "REFERENCE_ACREAGE",
    # 41, BOOL;
    "MANURE_USE",
    # 42, BOOL;
    "COVER_CROP_USE",
    """### GP23 Additions (DS-284) ######################################"""
    # 43, ENUM; (Beef Cattle, Dairy Cow, Chicken, Swine, Other)
    "MANURE_TYPE"
    # 44, FLOAT;
    "MANURE_DRY_QUANTITY_EQUIV"
    # 45, FLOAT; in miles
    "MANURE_TRANS_DIST"
    # 46, FLOAT; energy needed for transportation in Btu / ton manure / mile
    "MANURE_TRANS_EN"
    # 47, FLOAT; energy needed for manure application in Btu
    "MANURE_APPL_EN"
    """### END GP23 Additions ##########################################""",
]

SPLIT_FIELD = "split_field_report"
MANURE_REPORT = "manure_report"
LIME_REPORT = "lime_report"
REFERENCE_ACREAGE_REPORT = "reference_acreage_report"
CC_REPORT = "cover_crop_report"

"""
#####################################################################################
##### GP23 ADDITIONS - FD-CIC2022 (DS-272) ##########################################
#####################################################################################
"""

CC_TYPES = [
    "Winter Wheat",
    "Spring Wheat",
    "Barley",
    "Oats",
    "Maize",
    "Rye",
    "Rice",
    "Millet",
    "Sorghum",
    "Beans",
    "Pulses",
    "Soybeans",
    "Potatoes",
    "Tubers",
    "Peanuts",
    "Alfalfa",
    "Non-legume hay",
    "N-fixing forages",
    "Non-N-fixing forages",
    "Perennial Grasses",
    "Grass-Clover Mixtures",
    "Brassica hybrids",
    "Buckwheat",
    "Cabbage",
    "Camelina",
    "Canola",
    "Clover",
    "Collards or Kale",
    "Corn",
    "Cowpeas",
    "Fava beans",
    "Flax",
    "Lentils",
    "Mustard",
    "Peas",
    "Phacelia",
    "Radishes",
    "Rapeseed",
    "Ryegrass",
    "Safflowers",
    "Sudangrass",
    "Sugar beets",
    "Sunflowers",
    "Sunn hemp",
    "Teff grass",
    "Triticale",
    "Turnips",
    "Vetch",
]

# Conversion factors taken from FD-CIC22 model; data sheet "Parameters"
ACRE_PER_HECTARE = 2.47105
KG_PER_LBS = 0.4535925
G_PER_LBS = 453.592

# Conversion taken from FD-CIC 2022 model, sheet "Parameters"
DIESEL_BTU_PER_GAL = 128450

# Default value (in Btu / ton of manure / mile) from FD-CIC22 model
DEFAULT_MANURE_TRANS_EN = 10416.49299
# Default manure transportation distance (miles) from FD-CIC22 model
DEFAULT_MANURE_TRANS_DIST = 0.367
# Default manure application energy (in Btu / ac) from FD-CIC22 model
DEFAULT_MANURE_APPL_EN = 221365.589648777

# Default amount of active ingredients in herbicide application (g / ac) from FD-CIC22 model
DEFAULT_CC_HERB_AI = 612.3496995
# Default cover crop application energy (in Btu / ac) from FD-CIC22 model
DEFAULT_CC_APPL_EN = 62060

""" ### END GP23 ADDITIONS ###################################################### """
