import re

COLUMN_TRANSFORMATIONS = {
    "Application": {
        "Product": "Product",
        "Field": "Field_name",
        "Date Applied": "Operation_start",
        "Acres Applied": "Area_applied",
        "Avg Rate": "Applied_rate",
        "Units": "Applied_unit",
    },
    "Harvest": {
        "Unnamed: 0": "Farm_name",
        "Unnamed: 1": "Client",
        "Field": "Field_name",
        "DateHarvested": "Operation_start",
        "Acres": "Area_applied",
        "Yield": "Total_dry_yield_check",
        "Moisture": "Moisture",
        "Bushels": "Total_dry_yield",
        "Lbs": "Applied_unit",
    },
    "Planting": {
        "Unnamed: 0": "Farm_name",
        "Unnamed: 1": "Client",
        "Field": "Field_name",
        "Date Planted": "Operation_start",
        "Avg Population": "Applied_rate",
        "Acres Planted": "Area_applied",
    },
}
CFV_NUMERIC_COLUMNS = [
    "Acres_Applied",
    "Avg_Rate",
    "Acres_Planted",
    "Average_Population",
    "Avg_Speed",
    "Sing_Percent",
]
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

REPORT_COLUMN_MAP = {
    "Harvest": HARVEST_COLUMNS,
    "Planting": PLANTING_COLUMNS,
    "Application": APPLICATION_COLUMNS,
    "Tillage": TILLAGE_COLUMNS,
    "Fuel": FUEL_COLUMNS,
}

import pandas as pd


class Base:
    def __init__(self, filepath):
        self.report_type = self.identify_report_type(filepath)
        self.crop_type = self.identify_crop_type(filepath)

    def standardize_header(self, df):
        if self.report_type not in REPORT_COLUMN_MAP:
            raise ValueError(f"Unknown report type: {self.report_type}")

        transformation_map = COLUMN_TRANSFORMATIONS.get(self.report_type, {})
        df = df.rename(columns=transformation_map)

        df["Crop_type"] = self.crop_type
        df["Operation_type"] = self.report_type

        for col in REPORT_COLUMN_MAP[self.report_type]:
            if col not in df.columns:
                df[col] = pd.NA

        df = df.reindex(columns=REPORT_COLUMN_MAP[self.report_type], fill_value=pd.NA)

        return df

    def identify_report_type(self, file_path: str) -> str:
        pattern = r"(Harvest|Planting|Application|Seeding|Fuel|Yield|Tillage)"
        match = re.search(pattern, file_path, re.IGNORECASE)
        return match.group(1) if match else "Unknown"

    def identify_crop_type(self, file_path: str) -> str:
        match = re.search(r"(corn|soybean|wheat)", file_path.lower())
        return match.group(0) if match else "unknown"
