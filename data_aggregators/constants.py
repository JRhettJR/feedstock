# Dictionary to rename column headers from raw data files to generalized
# column header(s).
COLUMN_RENAMER = {
    # Climate FieldView
    "CFV": {
        # Application file
        "Application": {
            # Common column
            "Product": "Product",
            "Field": "Field_name",
            "Acres Applied": "Area_applied",
            "Date Applied": "Operation_start",
            "Avg Rate": "Applied_rate",
            "Units": "Applied_unit",
        },
        # Harvesting file
        "Harvest": {
            # Common column
            "Product": "Product",
            "Field": "Field_name",
            "Bushels": "Total_dry_yield",
            "Date Harvested": "Operation_start",
            "Moisture": "Moisture",
            "Acres": "Area_applied",
        },
        # Planting file
        "Seeding": {
            # Common column
            "Product": "Product",
            "Field": "Field_name",
            "Average Population": "Applied_rate",
            "Acres Planted": "Area_applied",
            "Date Planted": "Operation_start",
        },
    },
    # John Deere Ops
    "JDOps": {
        # Application file
        "Application": {
            # Common columns
            "Clients": "Client",
            "Farms": "Farm_name",
            "Fields": "Field_name",
            # Specific columns
            "Work": "Task_name",
            "Products": "Product",
            "Total Applied": "Applied_total",
            "Rate": "Applied_rate",
            "Last Applied": "Operation_start",
            # Specific unit mapping
            "Unit_0": "Area_unit",
            "Unit.0": "Area_unit",
            "Unit_1": "Rate_unit",
            "Unit.1": "Rate_unit",
            "Unit_2": "Applied_unit",
            "Unit.2": "Applied_unit",
        },
        # Harvest file
        "Harvest": {
            # Common columns
            "Clients": "Client",
            "Farms": "Farm_name",
            "Fields": "Field_name",
            # Specific columns
            "Work": "Task_name",
            "Varieties": "Product",
            "Crop Type": "Crop_type",
            "Dry yield": "Yield",
            "Moisture": "Moisture",
            "Total Dry Yield": "Total_dry_yield",
            "Area harvested": "Area_applied",
            "Last harvested": "Operation_start",
            # Specific unit mapping
            "Unit_0": "Area_unit",
            "Unit.0": "Area_unit",
            "Unit_3": "Applied_unit",
            "Unit.3": "Applied_unit",
        },
        # Tillage file
        "Tillage": {
            # Common columns
            "Clients": "Client",
            "Farms": "Farm_name",
            "Fields": "Field_name",
            # Specific columns
            "Area Tilled": "Area_applied",
            "Depth": "Applied_rate",
            "Target Depth": "Target_rate",
            "Last Tilled": "Operation_start",
            # Specific unit mapping
            "Unit_0": "Area_unit",
            "Unit.0": "Area_unit",
            "Unit_1": "Applied_unit",
            "Unit.1": "Applied_unit",
        },
        # Seeding file
        "Seeding": {
            # Common columns
            "Clients": "Client",
            "Farms": "Farm_name",
            "Fields": "Field_name",
            # Specific columns
            "Work": "Task_name",
            "Varieties": "Product",
            "Crop Type": "Crop_type",
            "Area Seeded": "Area_applied",
            "Rate": "Applied_rate",
            "Total Applied": "Applied_total",
            "Last Seeded": "Operation_start",
            # Specific unit mapping
            "Unit_0": "Area_unit",
            "Unit.0": "Area_unit",
            "Unit_1": "Rate_unit",
            "Unit.1": "Rate_unit",
            "Unit_2": "Applied_unit",
            "Unit.2": "Applied_unit",
        },
    },
    "Granular": {
        "Application": {
            "Field Name": "Field_name",
            "Task Start and End Dates": "Ops_dates",
            "Ops Boundary Name": "Field_name",
            "Task Name": "Task_name",
            "Task Subtype": "Operation_type",
            "Crop Subspecies": "Crop_type",
            "Equipment": "Implement",
            "Input Name": "Product",
            "Area Applied": "Area_applied",
            "Rate Applied": "Applied_rate",
            "Total Applied": "Applied_total",
            "Task": "Product",
        },
        "Harvest": {
            "Organization": "Client",
            "Entity": "Farm_name",
            "Boundary": "Field_name",
            "Field": "Sub_field_name",
            "Crop Product": "Crop_type",
            "Task Name": "Task_name",
            "Task Type": "Operation_type",
            "Task Completed Date": "Operation_start",
            "Actual Quantity - SUM": "Total_dry_yield",
            "Actual Quantity - SUM - Unit": "Applied_unit",
            "Boundary Area - SUM": "Area_boundary",
            "Planted Area - SUM": "Area_seeded",
            "Harvested Area - SUM": "Area_applied",
            "Harvested Area Yield - avg": "Yield",
            "Moisture - WT AVG": "Moisture",
        },
    },
    "FarMobile": {
        # Spraying data file
        "Application": {
            "FARM_NM": "Farm_name",
            "FLD_NM": "Field_name",
            "GRWR_NM": "Client",
            "CROP_CYCLE": "Growing_cycle",
            "CROP_NM": "Crop_type",
            "SP_STRT": "Operation_start",
            "SP_END": "Operation_end",
            "AVG_RATE": "Applied_rate",
            "SPRAY_ACRE": "Area_applied",
        },
        "Harvest": {
            "FARM_NM": "Farm_name",
            "FLD_NM": "Field_name",
            "GRWR_NM": "Client",
            "CROP_CYCLE": "Growing_cycle",
            "CROP_NM": "Crop_type",
            "HRVST_ACRE": "Area_applied",
            "DRY_YIELD": "Yield",
            "MOISTURE": "Moisture",
            "HRVST_STRT": "Operation_start",
            "HRVST_END": "Operation_end",
            "TOT_DPRD": "Total_dry_yield",
        },
        "Tillage": {
            "FARM_NM": "Farm_name",
            "FLD_NM": "Field_name",
            "GRWR_NM": "Client",
            "CROP_CYCLE": "Growing_cycle",
            "CROP_NM": "Crop_type",
            "TILL_STRT": "Operation_start",
            "TILL_END": "Operation_end",
            "TILL_ACRE": "Area_applied",
            "TILLAGE_DEPTH_1": "Applied_rate",
        },
    },
}
