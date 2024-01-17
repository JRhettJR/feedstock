#
# DATA AGGREGATORS
#
DA_CFV = "CFV"  # Climate FieldView
DA_LDB = "LDB"  # Land.db
DA_JDOPS = "JDOps"  # MyJohnDeere Operations
DA_GRANULAR = "Granular"
DA_PAP = "PAP"  # PrairieAg Partners
DA_FM = "FM"  # FarMobile
DA_SMS = "SMS"  # SMS Ag Leader

DATA_AGGREGATORS = [DA_CFV, DA_GRANULAR, DA_JDOPS, DA_LDB, DA_PAP, DA_FM, DA_SMS]

#
# PRODUCT TYPES
#
FERTILIZER = "FERTILIZER"
HERBICIDE = "HERBICIDE"
FUNGICIDE = "FUNGICIDE"
INSECTICIDE = "INSECTICIDE"
OTHER = "OTHER"

NOT_FOUND = False

col_renamer = {
    # PAP
    "Name": "Client",
    "Field": "Field_name",
    "Date": "Operation_start",
    "Acres": "Area_applied",
    "Crop": "Crop_type",
    "Rate/Acre": "Applied_rate",
    "Rate_acre": "Applied_rate",
    "Totals": "Applied_total",
    "Total": "Applied_total",
    "Unit total": "Applied_unit",
    "Unit": "Applied_unit",
    "Net bushels": "Yield",
    # Verified Acres
    "Farmer / Customer": "Client",
    # LDB
    "Farm": "Farm_name",
    "Field Name": "Field_name",
    "Start date": "Operation_start",
    "End date": "Operation_end",
    "Application name": "Task_name",
    "Rate value": "Applied_rate",
    "Total product qty": "Applied_total",
    "Total product unit": "Applied_unit",
    "Crop zone area": "Area_applied",
    "Weighted final dry quantity": "Total_dry_yield",
    "Final unit": "Applied_unit",
    "Target moisture percent": "Moisture",
    # JDOps
    "Farms": "Farm_name",
    "Fields": "Field_name",
    "Products": "Product",
    "Total applied": "Applied_total",
    "Rate": "Applied_rate",
    "Last applied": "Operation_start",
    "Clients": "Client",
    "Work": "Task_name",
    "Area harvested": "Area_applied",
    "Last harvested": "Operation_start",
    "Dry yield": "Yield",
    "Last seeded": "Operation_start",
    "Varieties": "Product",
    "Last tilled": "Operation_start",
    "Depth": "Applied_rate",
    # 'Total dry yield': 'Total_yield',
    "Area seeded": "Area_applied",
    "Area tilled": "Area_applied",
    # Granular
    "Input_name": "Product",
    "Input_unit": "Applied_unit",
    "Input_rate": "Applied_total",
    "Organization": "Client",
    "Entity": "Farm_name",
    # 'Task name': 'Product',
    "Boundary": "Field_name",
    # "Field": "Sub_field_name",
    "Task completed date": "Operation_start",
    "Task type": "Operation_type",
    "Crop product": "Crop_type",
    "Plan name": "Crop_plan",
    "Task": "Product",
    "Actual quantity - sum": "Total_dry_yield",
    "Actual quantity - sum - unit": "Applied_unit",
    "Boundary area - sum": "Area_boundary",
    # 'Boundary Area - SUM - Unit': '',
    "Planted area - sum": "Area_seeded",
    # 'Planted area - sum - Unit': '',
    "Harvested area - sum": "Area_applied",
    # 'Harvested area - sum - Unit': '',
    "Harvested area yield - avg": "Yield",
    "Moisture - wt avg": "Moisture",
    # 'Sub Field Name': '',
    "Task start and end dates": "Ops_dates",
    "Ops boundary name": "Field_name",
    # 'Task Name': '',
    "Task subtype": "Operation_type",
    "Crop subspecies": "Crop_type",
    # 'Operator': '',
    "Equipment": "Implement",
    "Input name": "Product",
    # 'EPA Number': '',
    "Rate applied": "Applied_rate",
    # CFV
    "Bushels": "Total_dry_yield",
    "Date harvested": "Operation_start",
    "Average population": "Applied_rate",
    "Acres planted": "Area_applied",
    "Date planted": "Operation_start",
    "Date applied": "Operation_start",
    "Acres applied": "Area_applied",
    "Avg rate": "Applied_rate",
    "Units": "Applied_unit",
    # FM
    "Farm_nm": "Farm_name",
    "Fld_nm": "Field_name",
    "Grwr_nm": "Client",
    "Crop_cycle": "Growing_cycle",
    "Crop_nm": "Crop_type",
    "Hrvst_acre": "Area_applied",
    "Hrvst_strt": "Operation_start",
    "Hrvst_end": "Operation_end",
    "Tot_dprd": "Total_dry_yield",
    "Sp_strt": "Operation_start",
    "Sp_end": "Operation_end",
    "Avg_rate": "Applied_rate",
    "Spray_acre": "Area_applied",
    "Till_strt": "Operation_start",
    "Till_end": "Operation_end",
    "Till_acre": "Area_applied",
    "Tillage_depth_1": "Applied_rate",
    # SMS
    "Acreage": "Area_applied",
    "Yield_bu_ac": "Yield",
    "Total_yield": "Total_dry_yield",
}

"""FILE TYPES"""

# Climate Field View
CFV_APPLICATION = "ApplicationReport"
CFV_HARVEST = "HarvestReport"
CFV_PLANTING = "PlantingReport"

CFV_FILE_TYPES = [CFV_APPLICATION, CFV_HARVEST, CFV_PLANTING]

# FarmMobile
FM_APPLICATION = "data_SP"
FM_HARVEST = "data_HR"
FM_PLANTING = "data_PL"
FM_TILLAGE = "data_TL"

FM_FILE_TYPES = [FM_APPLICATION, FM_HARVEST, FM_PLANTING, FM_TILLAGE]

# Granular
GRAN_APPLICATION = "application_record"
GRAN_HARVEST = "YieldCustom"
# Extracted from GRAN_APPLICATION
GRAN_PLANTING = "planting"
GRAN_TILLAGE = "tillage"

GRAN_FILE_TYPES = [GRAN_APPLICATION, GRAN_HARVEST]
GRAN_GENERATED = [GRAN_TILLAGE, GRAN_PLANTING]


# JDOps file types
JD_APPLICATION = "Application"
JD_HARVEST = "Harvest"
JD_PLANTING = "Seeding"
JD_TILLAGE = "Tillage"
# Generated internally (from PDFs)
JD_FUEL = "Fuel_report"

JD_FILE_TYPES = [JD_APPLICATION, JD_HARVEST, JD_PLANTING, JD_TILLAGE, JD_FUEL]

# Land.db
LDB_APPLICATION = "Applied Products"  # sheet name of LDB excel
LDB_HARVEST = "Yield Field To Sale"
# internally generated
LDB_PLANTING = "planting"
LDB_FUEL = "fuel"
LDB_TILLAGE = "tillage"

LDB_FILE_TYPES = [LDB_APPLICATION, LDB_HARVEST]
LDB_GENERATED = [LDB_PLANTING, LDB_FUEL, LDB_TILLAGE]

# Prairie Ag Partners
PAP_APPLICATION = "converted"
PAP_HARVEST = "ScaleTickets"

PAP_FILE_TYPES = [PAP_APPLICATION, PAP_HARVEST]

# SMS Ag Leader
# raw file names
SMS_HARVEST_RAW = "SMS_harvest"
SMS_PLANTING_RAW = "SMS_planting"
SMS_FERTILISER_RAW = "SMS_fertiliser"
# actual file names
SMS_APPLICATION = "application"
SMS_PLANTING = "planting"
SMS_HARVEST = "harvest"

SMS_FILE_TYPES = [SMS_APPLICATION, SMS_PLANTING, SMS_HARVEST]

#
# General
#
APPLICATION = "file_type_application"
PLANTING = "file_type_planting"
HARVEST = "file_type_harvest"
# TILLAGE = 'file_type_tillage'

APPLICATION_FILE_TYPES = [
    CFV_APPLICATION,
    JD_APPLICATION,
    GRAN_APPLICATION,
    PAP_APPLICATION,
    FM_APPLICATION,
    SMS_APPLICATION,
    LDB_APPLICATION,
]

HARVEST_FILE_TYPES = [
    CFV_HARVEST,
    JD_HARVEST,
    GRAN_HARVEST,
    PAP_HARVEST,
    FM_HARVEST,
    SMS_HARVEST,
    LDB_HARVEST,
]

PLANTING_FILE_TYPES = [
    CFV_PLANTING,
    JD_PLANTING,
    GRAN_PLANTING,
    FM_PLANTING,
    SMS_PLANTING,
    LDB_PLANTING,
]

TILLAGE_FILE_TYPES = [JD_TILLAGE, GRAN_TILLAGE, FM_TILLAGE, LDB_TILLAGE]

FILE_CATEGORIES = [APPLICATION, PLANTING, HARVEST]

MISSING_IMPLEMENTATION = "missing_implementation"

HARVEST_DATES = "harvest_dates"
SPLIT_FIELD = "split_field_report"
MANURE_REPORT = "manure_report"
LIME_REPORT = "lime_report"
REFERENCE_ACREAGE_REPORT = "reference_acreage_report"
CC_REPORT = "cover_crop_report"

GENERIC_REPORTS = [
    HARVEST_DATES,
    SPLIT_FIELD,
    MANURE_REPORT,
    LIME_REPORT,
    LDB_PLANTING,
    LDB_FUEL,
    GRAN_PLANTING,
    GRAN_TILLAGE,
]

# Crops
CORN = "Corn"
SOYBEAN = "Soybean"
SOYBEANS = "Soybeans"
BEANS = "Beans"

FDCIC_CROPS = [CORN, SOYBEAN, SOYBEANS, BEANS]

CROP_TYPES = [*FDCIC_CROPS]

# Columns
BASE_COLUMNS = ["Client", "Farm_name", "Field_name", "Operation_start", "Operation_end"]
HARVEST_COLUMNS = [
    *BASE_COLUMNS,
    "Task_name",
    "Crop_type",
    "Sub_crop_type",
    "Area_applied",
    "Total_dry_yield",
    "Total_dry_yield_check",
    "Applied_unit",
    "Moisture",
    "Operation_type",
]
PLANTING_COLUMNS = [
    *BASE_COLUMNS,
    "Task_name",
    "Product",
    "Crop_type",
    "Sub_crop_type",
    "Manufacturer",
    "Area_applied",
    "Applied_rate",
    "Applied_total",
    "Applied_unit",
    "Operation_type",
]
APPLICATION_COLUMNS = [
    *BASE_COLUMNS,
    "Task_name",
    "Crop_type",
    "Product",
    "Manufacturer",
    "Reg_number",
    "Area_applied",
    "Applied_rate",
    "Applied_total",
    "Applied_unit",
    "Operation_type",
]
TILLAGE_COLUMNS = [
    *BASE_COLUMNS,
    "Task_name",
    "Area_applied",
    "Applied_rate",
    "Applied_total",
    "Applied_unit",
    "Operation_type",
]
FUEL_COLUMNS = [
    *BASE_COLUMNS,
    "Task_name",
    "Product",
    "Area_applied",
    "Area_unit",
    "Applied_total",
    "Applied_unit",
    "Total_fuel",
    "Fuel_unit",
    "Operation_type",
]

# internally generated reports
HARVEST_DATES_COLUMNS = [
    "Client",
    "Farm_name",
    "Field_name",
    "Crop_type",
    "Sub_crop_type",
    "Harvest_date",
    "Year",
]

LIME_REPORT_COLUMNS = [
    "Client",
    "Farm_name",
    "Field_name",
    "Task_name",
    "Product",
    "Operation_start",
    "Operation_end",
    "Area_applied",
    "Applied_rate",
    "Applied_total",
    "Applied_unit",
    "Operation_type",
    "Crop_type",
    "Harvest_prev",
    "Harvest_curr",
    "Op_relevance",
    "Growing_cycle",
]

MANURE_REPORT_COLUMNS = [
    "Client",
    "Farm_name",
    "Field_name",
    "Task_name",
    "Product",
    "Operation_start",
    "Operation_end",
    "Area_applied",
    "Applied_rate",
    "Applied_total",
    "Applied_unit",
    "Operation_type",
    "Crop_type",
    "Manure_op",
    "Harvest_prev",
    "Harvest_curr",
    "Op_relevance",
    "Growing_cycle",
]
