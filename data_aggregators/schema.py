from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Measurement(BaseModel):
    value: Optional[float] = None
    unit: Optional[str] = None

    @field_validator("value", mode="before")
    def validate_value(cls, v):
        if isinstance(v, str):
            if v.strip() == "---":
                return None
            try:
                return float(v)
            except ValueError:
                raise ValueError(
                    f"Invalid string input for measurement value: {v}"
                ) from None
        if isinstance(v, (float, int)):
            return v
        elif isinstance(v, dict):
            return cls(**v)
        return v


class MeasurementField(BaseModel):
    value: Optional[float] = None
    unit: Optional[str] = None

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if isinstance(v, MeasurementField):
            return v
        elif isinstance(v, (float, int)):
            return cls(value=float(v), unit="default_unit")
        elif isinstance(v, str):
            if v.strip() == "---":
                return cls(value=0.0, unit="default_unit")
            try:
                return cls(value=float(v), unit="default_unit")
            except ValueError:
                raise ValueError(f"Invalid string input for measurement: {v}") from None
        elif isinstance(v, dict):
            return cls(**v)
        else:
            raise ValueError(f"Unexpected type for measurement: {type(v)}")


class DAMeta(BaseModel):
    """Class for data aggregator related additional data"""

    model_config = ConfigDict(populate_by_name=True)

    File_type: Optional[str] = Field(default=None)


"""CSV DATA"""


class CFVData(DAMeta):
    Data_source: str = Field(
        default="CFV",
        description="Source of the data, default is CFV",
        validate_default=True,
    )
    Crop_type: str | None


class CFVApplicationReport(CFVData):
    Product: str | None = Field(
        default=None,
        description="Name or identifier of the product",
        validate_default=True,
    )
    Field_name: str = Field(
        default=None,
        description="Identifier or name of the field",
        validate_default=True,
        alias="Field",
    )
    Date_Applied: Optional[str] = Field(
        default=None, validate_default=True, alias="Date Applied"
    )
    Acres_Applied: Optional[float] = Field(default=None, alias="Acres Applied")
    Avg_Rate: Optional[float] = Field(default=None, alias="Avg Rate")
    Units: Optional[str] = Field(
        default=None,
        validate_default=True,
    )

    @field_validator("Product", mode="before")
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value

    @field_validator(
        "Field_name", "Date_Applied", "Units", mode="before"
    )  # , always=True)
    def ensure_str(cls, v):
        return str(v) if v is not None else None

    @field_validator("Acres_Applied", "Avg_Rate", mode="before")
    def ensure_float(cls, v):
        if isinstance(v, (float, int)):
            return float(v)
        return v


class CFVPlantingReport(CFVData):
    Client: str
    Farm_name: str
    Field_name: str = Field(
        default=None,
        description="Identifier or name of the field",
        validate_default=True,
        alias="Field",
    )
    Date_Planted: Optional[str] = Field(
        default=None, validate_default=True, alias="Date Planted"
    )
    Acres_Planted: Optional[float] = Field(default=None, alias="Acres Planted")
    Average_Population: Optional[float] = Field(
        default=None, alias="Average Population"
    )
    Avg_Speed: Optional[float] = Field(default=None, alias="Avg Speed/n(mph)")
    Sing_Percent: Optional[float] = Field(default=None, alias="Sing %")
    Good_Spacing: Optional[str] = Field(
        default=None, validate_default=True, alias="Good Spacing"
    )
    Loss_of_GC: Optional[str] = Field(
        default=None, validate_default=True, alias="Loss of GC"
    )
    Good_Down_Force_Percent: Optional[str] = Field(
        default=None, validate_default=True, alias="Good\nDown Force Percent"
    )
    Excess_Down_Force: Optional[str] = Field(
        default=None, validate_default=True, alias="% Excess Down Force"
    )

    @field_validator(
        "Field_name",
        "Date_Planted",
        "Good_Spacing",
        "Loss_of_GC",
        "Good_Down_Force_Percent",
        "Excess_Down_Force",
        mode="before",
        # always=True,
    )
    def ensure_str(cls, v):
        return str(v) if v is not None else None

    @field_validator(
        "Acres_Planted",
        "Average_Population",
        "Avg_Speed",
        "Sing_Percent",
        mode="before",
    )
    def ensure_float(cls, v):
        if isinstance(v, str):
            for char in ["%", ","]:
                if char in v:
                    v = v.replace(char, "").strip()
            return float(v)
        if isinstance(v, (float, int)):
            return float(v)
        return v


class CFVHarvestReport(CFVData):
    Field_name: str = Field(
        default=None,
        description="Identifier or name of the field",
        validate_default=True,
        alias="Field",
    )
    Date_Harvested: Optional[str] = Field(
        default=None, validate_default=True, alias="Date Harvested"
    )
    Acres: Optional[float] = None
    Yield: Optional[float] = None
    Moisture: Optional[str] = Field(
        default=None,
        validate_default=True,
    )
    Bushels: Optional[str] = Field(
        default=None,
        validate_default=True,
    )
    Lbs: Optional[str] = Field(
        default=None,
        validate_default=True,
    )

    @field_validator(
        "Field_name",
        "Date_Harvested",
        "Moisture",
        "Bushels",
        "Lbs",
        mode="before",
        # always=True
    )
    def ensure_str(cls, v):
        return str(v) if v is not None else None

    @field_validator("Acres", "Yield", mode="before")
    def ensure_float(cls, v):
        if isinstance(v, (float, int)):
            return float(v)
        return v


class MyJohnDeereData(BaseModel):
    Product: str = Field(default=None, description="Name or identifier of the product")
    Data_source: str = Field(
        default="JDOps", description="Source of the data, default is JDOps"
    )
    operation_id: Optional[str] = Field(
        default=None, description="Identifier for the operation", validate_default=True
    )
    operation_type: Optional[str] = Field(
        default=None, description="Type of the operation", validate_default=True
    )
    date: Optional[datetime] = Field(
        default=None, description="Date and time of the operation"
    )
    crop_type: Optional[str] = Field(
        default=None,
        description="Type of crop involved in the operation",
        validate_default=True,
    )
    area: Optional[float] = Field(
        default=None, description="Area covered or affected by the operation"
    )
    additional_info: Dict = Field(
        default_factory=dict, description="Additional information in dictionary format"
    )

    @field_validator(
        "operation_id", "operation_type", "crop_type", mode="before"  # , always=True
    )
    def ensure_str(cls, v):
        return str(v) if v is not None else None

    @field_validator("area", mode="before")
    def ensure_float(cls, v):
        if isinstance(v, (float, int)):
            return float(v)
        return v

    @field_validator("additional_info", mode="before")
    def ensure_dict(cls, v):
        if not isinstance(v, dict):
            raise ValueError("additional_info must be a dictionary")
        return v


class MyJohnDeereApplication(DAMeta):
    Clients: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the clients associated with the application",
    )
    Farms: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the farms where the application occurs",
    )
    Fields: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the specific fields receiving the application",
    )
    Products: Optional[str] = Field(
        default=None, description="Names or identifiers of the products applied"
    )
    Work: Optional[str] = Field(
        default=None, description="Description of the work or task performed"
    )
    Area_Applied: Optional[str] = Field(
        default=None,
        description="Total area that has received application",
        alias="Area Applied",
    )
    Unit: Optional[str | None] = Field(default=None)

    Rate: Optional[str] = None
    Unit_1: Optional[str | None] = Field(default=None, alias="Unit.1")

    Total_Applied: Optional[str] = Field(
        default=None,
        description="Total quantity of product applied",
        alias="Total Applied",
    )
    Unit_2: Optional[str | None] = Field(default=None, alias="Unit.2")

    Target_Rate: Optional[str] = Field(
        default=None, description="Target rate for the application", alias="Target Rate"
    )
    Unit_3: Optional[str | None] = Field(default=None, alias="Unit.3")

    Target_Total: Optional[str] = Field(
        default=None,
        description="Target total quantity of product to apply",
        alias="Target Total",
    )
    Unit_4: Optional[str | None] = Field(default=None, alias="Unit.4")

    Speed: Optional[str] = None
    Unit_5: Optional[str | None] = Field(default=None, alias="Unit.5")

    Last_Applied: Optional[str] = Field(
        default=None,
        description="Date when the product was last applied",
        alias="Last Applied",
    )

    @field_validator(
        "Unit",
        "Unit_1",
        "Unit_2",
        "Unit_3",
        "Unit_4",
        "Unit_5",
        mode="before",
    )
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value

    @field_validator("Area_Applied", mode="before")
    def float_to_str(cls, value):
        if isinstance(value, float):
            return str(value)
        return value


class MyJohnDeereHarvest(DAMeta):
    Clients: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the clients associated with the harvest",
    )
    Farms: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the farms where harvest occurs",
    )
    Fields: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the specific fields being harvested",
    )
    Varieties: Optional[str] = Field(
        default=None, description="Types or varieties of crops being harvested"
    )
    Crop_Type: Optional[str] = Field(
        default=None, description="Type of crop being harvested", alias="Crop Type"
    )
    Area_Harvested: Optional[str] = Field(
        default=None,
        description="Total area that has been harvested",
        alias="Area Harvested",
    )
    Unit: Optional[str | None] = Field(default=None)

    Moisture: Optional[str] = Field(
        default=None, description="Moisture content of the harvested crop"
    )
    Unit_1: Optional[str | None] = Field(default=None, alias="Unit.1")

    Dry_Yield: Optional[str] = Field(
        default=None, description="Dry yield of the harvested crop", alias="Dry Yield"
    )
    Unit_2: Optional[str | None] = Field(default=None, alias="Unit.2")

    Total_Dry_Yield: Optional[str] = Field(
        default=None,
        description="Total dry yield of the harvested crop",
        alias="Total Dry Yield",
    )
    Unit_3: Optional[str | None] = Field(default=None, alias="Unit.3")

    Wet_Weight: Optional[str] = Field(
        default=None, description="Wet weight of the harvested crop", alias="Wet Weight"
    )
    Unit_4: Optional[str | None] = Field(default=None, alias="Unit.4")

    Total_Wet_Weight: Optional[str] = Field(
        default=None,
        description="Total wet weight of the harvested crop",
        alias="Total Wet Weight",
    )
    Unit_5: Optional[str | None] = Field(default=None, alias="Unit.5")

    Speed: Optional[str] = Field(
        default=None, description="Speed at which the harvest equipment operates"
    )
    Unit_6: Optional[str | None] = Field(default=None, alias="Unit.6")

    Last_Harvested: Optional[str] = Field(
        default=None,
        description="Date when the field was last harvested",
        alias="Last Harvested",
    )

    @field_validator(
        "Unit",
        "Unit_1",
        "Unit_2",
        "Unit_3",
        "Unit_4",
        "Unit_5",
        "Unit_6",
        mode="before",
    )
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value

    @field_validator("Area_Harvested", mode="before")
    def float_to_str(cls, value):
        if isinstance(value, float):
            return str(value)
        return value

    # @field_validator("Moisture", mode="before")
    # def convert_moisture(cls, v):
    #     if isinstance(v, (float, int)):
    #         return Measurement(value=v)
    #     elif isinstance(v, str):
    #         if v.strip() == "---":
    #             return Measurement(value=0.0)
    #         try:
    #             return Measurement(value=float(v))
    #         except ValueError:
    #             raise ValueError(f"Invalid string input for moisture: {v}") from None
    #     elif isinstance(v, dict):
    #         return Measurement(**v)
    #     return v


class MyJohnDeerePlanting(DAMeta):
    Clients: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the clients associated with the planting",
    )
    Farms: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the farms where planting occurs",
    )
    Fields: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the specific fields being planted",
    )
    Varieties: Optional[str] = Field(
        default=None, description="Types or varieties of crops being planted"
    )
    Crop_Type: Optional[str] = Field(
        default=None, description="Type of crop being planted", alias="Crop Type"
    )
    Area_Seeded: Optional[str] = Field(
        default=None, description="Total area that has been seeded", alias="Area Seeded"
    )
    Unit: Optional[str | None] = Field(default=None)

    Rate: Optional[str] = None
    Unit_1: Optional[str | None] = Field(default=None, alias="Unit.1")

    Total_Applied: Optional[str] = Field(
        default=None,
        description="Total quantity of seeds or other materials applied",
        alias="Total Applied",
    )
    Unit_2: Optional[str | None] = Field(default=None, alias="Unit.2")

    Target_Rate: Optional[str] = Field(
        default=None, description="Desired rate for seeding", alias="Target Rate"
    )
    Unit_3: Optional[str | None] = Field(default=None, alias="Unit.3")

    Target_Total: Optional[str] = Field(
        default=None,
        description="Desired total quantity of seeds or materials to apply",
        alias="Target Total",
    )
    Unit_4: Optional[str | None] = Field(default=None, alias="Unit.4")

    Speed: Optional[str] = None
    Unit_5: Optional[str | None] = Field(default=None, alias="Unit.5")

    Last_Seeded: Optional[str] = Field(
        default=None,
        description="Date when the field was last seeded",
        alias="Last Seeded",
    )

    @field_validator(
        "Unit",
        "Unit_1",
        "Unit_2",
        "Unit_3",
        "Unit_4",
        "Unit_5",
        mode="before",
    )
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value

    @field_validator("Area_Seeded", mode="before")
    def float_to_str(cls, value):
        if isinstance(value, float):
            return str(value)
        return value


class MyJohnDeereTillage(DAMeta):
    Clients: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the clients associated with the tillage",
    )
    Farms: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the farms where tillage occurs",
    )
    Fields: Optional[str] = Field(
        default=None,
        description="Names or identifiers of the specific fields being tilled",
    )
    Area_Tilled: Optional[str] = Field(
        default=None, description="Total area that has been tilled", alias="Area Tilled"
    )
    Unit: Optional[str | None] = Field(default=None)

    Depth: Optional[str] = None
    Unit_1: Optional[str | None] = Field(default=None, alias="Unit.1")

    Target_Depth: Optional[str] = Field(
        default=None, description="Desired depth for the tillage", alias="Target Depth"
    )
    Unit_2: Optional[str | None] = Field(default=None, alias="Unit.2")

    Target_Pressure: Optional[str] = Field(
        default=None,
        description="Desired pressure to be applied during tillage",
        alias="Target Pressure",
    )
    Unit_3: Optional[str | None] = Field(default=None, alias="Unit.3")

    Speed: Optional[str] = None
    Unit_4: Optional[str | None] = Field(default=None, alias="Unit.4")

    Last_Tilled: Optional[str] = Field(
        default=None,
        description="Date when the field was last tilled",
        alias="Last Tilled",
    )

    @field_validator(
        "Unit",
        "Unit_1",
        "Unit_2",
        "Unit_3",
        "Unit_4",
        mode="before",
    )
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value

    @field_validator("Area_Tilled", mode="before")
    def float_to_str(cls, value):
        if isinstance(value, float):
            return str(value)
        return value


class GranularData(DAMeta):
    pass
    # field_name: Optional[str] = Field(
    #     default=None, description="Name of the agricultural field or plot"
    # )
    # task_dates: Optional[str] = Field(
    #     default=None,
    #     description="Dates associated with the agricultural task, likely in a specific format",
    # )
    # boundary_name: Optional[str] = Field(
    #     default=None,
    #     description="Name or identifier of the geographical boundary for the task",
    # )
    # task_name: Optional[str] = Field(
    #     default=None, description="Name or identifier of the specific agricultural task"
    # )
    # task_subtype: Optional[str] = Field(
    #     default=None, description="Subtype or category of the agricultural task"
    # )
    # crop_subspecies: Optional[str] = Field(
    #     default=None,
    #     description="Subspecies or variety of the crop involved in the task",
    # )
    # operator: Optional[str] = Field(
    #     default=None,
    #     description="Name of the person or entity operating the equipment or overseeing the task",
    # )
    # operator_license_number: Optional[str] = Field(
    #     default=None, description="License number of the operator, if applicable"
    # )
    # boundary_lat_lng: Optional[str] = Field(
    #     default=None, description="Latitude and longitude coordinates of the boundary"
    # )
    # equipment: Optional[str] = Field(
    #     default=None,
    #     description="Details of the equipment used for the agricultural task",
    # )
    # notes: Optional[str] = Field(
    #     default=None,
    #     description="Additional notes or comments related to the task or field",
    # )


class GranularHarvestData(GranularData):
    Organization: Optional[str] = Field(
        default=None, description="Name of the organization managing the harvest"
    )
    Entity: Optional[str] = Field(
        default=None, description="Entity involved in the harvesting process"
    )
    Boundary: Optional[str] = Field(
        default=None, description="Boundary identifier for the harvesting area"
    )
    Field_name: Optional[str] = Field(
        default=None,
        description="Specific field identifier where the harvest takes place",
        alias="Field",
    )
    Crop_product: Optional[str] = Field(
        default=None,
        description="Type or name of the crop product being harvested",
        alias="Crop Product",
    )
    Task_name: Optional[str] = Field(
        default=None, description="Name of the harvesting task", alias="Task Name"
    )
    Task_type: Optional[str] = Field(
        default=None, description="Type of the harvesting task", alias="Task Type"
    )
    Task_completed_date: Optional[str] = Field(
        default=None,
        description="Date when the harvesting task was completed",
        alias="Task Completed Date",
    )
    Actual_quantity_sum: Optional[float] = Field(
        default=None,
        description="Total actual quantity of harvested product",
        alias="Actual Quantity - SUM",
    )
    Actual_quantity_sum_unit: Optional[str] = Field(
        default=None,
        description="Unit of measurement for the actual quantity sum",
        alias="Actual Quantity - SUM - Unit",
    )
    Boundary_area_sum: Optional[float] = Field(
        default=None,
        description="Total area of the boundary where harvesting occurred",
        alias="Boundary Area - SUM",
    )
    Boundary_area_sum_unit: Optional[str] = Field(
        default=None,
        description="Unit of measurement for the boundary area sum",
        alias="Boundary Area - SUM - Unit",
    )
    Boundary_area_yield_avg: Optional[float] = Field(
        default=None,
        description="Average yield per unit area within the boundary",
        alias="Boundary Area Yield - AVG",
    )
    Boundary_area_yield_avg_unit: Optional[str] = Field(
        default=None,
        description="Unit of measurement for the average yield per boundary area",
        alias="Boundary Area Yield - AVG - Unit",
    )
    Planted_area_sum: Optional[float] = Field(
        default=None,
        description="Total area of planting relevant to the harvest",
        alias="Planted Area - SUM",
    )
    Planted_area_sum_unit: Optional[str] = Field(
        default=None,
        description="Unit of measurement for the planted area sum",
        alias="Planted Area - SUM - Unit",
    )
    Planted_area_yield_avg: Optional[float] = Field(
        default=None,
        description="Average yield per unit area of planted crop",
        alias="Planted Area Yield - AVG",
    )
    Planted_area_yield_avg_unit: Optional[str] = Field(
        default=None,
        description="Unit of measurement for the average yield per planted area",
        alias="Planted Area Yield - AVG - Unit",
    )
    Harvested_area_sum: Optional[float] = Field(
        default=None,
        description="Total area that has been harvested",
        alias="Harvested Area - SUM",
    )
    Harvested_area_sum_unit: Optional[str] = Field(
        default=None,
        description="Unit of measurement for the harvested area sum",
        alias="Harvested Area - SUM - Unit",
    )
    Harvested_area_yield_avg: Optional[float] = Field(
        default=None,
        description="Average yield per unit area of harvested crop",
        alias="Harvested Area Yield - AVG",
    )
    Harvested_area_yield_avg_unit: Optional[str] = Field(
        default=None,
        description="Unit of measurement for the average yield per harvested area",
        alias="Harvested Area Yield - AVG - Unit",
    )
    Moisture_wt_avg: Optional[float] = Field(
        default=None,
        description="Average weight of moisture in the harvested product",
        alias="Moisture - WT AVG",
    )

    @field_validator(
        "Entity",
        "Boundary",
        "Field_name",
        "Crop_product",
        "Task_name",
        "Task_type",
        "Task_completed_date",
        "Actual_quantity_sum_unit",
        "Boundary_area_yield_avg_unit",
        "Planted_area_yield_avg_unit",
        "Harvested_area_yield_avg_unit",
        mode="before",
    )
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value


class GranularApplicationData(GranularData):
    Field_name: str = Field(
        default=None, description="Name of the field", alias="Field Name"
    )
    Task_start_and_end_dates: str = Field(
        default=None,
        description="Start and end dates of the task",
        alias="Task Start and End Dates",
    )
    Ops_boundary_name: str = Field(
        default=None,
        description="Name of the operational boundary",
        alias="Ops Boundary Name",
    )
    Task_name: str = Field(
        default=None, description="Name of the task", alias="Task Name"
    )
    Task_subtype: str = Field(
        default=None, description="Subtype of the task", alias="Task Subtype"
    )
    Crop_subspecies: str = Field(
        default=None,
        description="Subspecies of the crop being handled",
        alias="Crop Subspecies",
    )
    Operator: str = Field(default=None, description="Name of the operator")
    Operator_license_number: str = Field(
        default=None,
        description="License number of the operator",
        alias="Operator Licence Number",
    )
    Boundary_latitude_longitude: str = Field(
        default=None, description="Latitude and longitude of the boundary"
    )
    Equipment: str = Field(default=None, description="Details of the equipment used")
    Windspeed: float = Field(
        default=None, description="Windspeed at the time of application"
    )
    Wind_direction: str = Field(
        default=None, description="Wind direction at the time of application"
    )
    Temperature: float = Field(
        default=None, description="Temperature at the time of application"
    )
    Humidity: float = Field(
        default=None, description="Humidity at the time of application"
    )
    Previous_24hr_precipitation: float = Field(
        default=None, description="Precipitation in the previous 24 hours"
    )
    Next_24hr_precipitation: float = Field(
        default=None, description="Expected precipitation in the next 24 hours"
    )
    Notes: str = Field(default=None, description="Additional notes or remarks")
    Input_name: str = Field(
        default=None, description="Name of the input material", alias="Input Name"
    )
    Epa_number: str = Field(
        default=None,
        description="EPA registration number of the product",
        alias="EPA Number",
    )
    Reentry_interval: str = Field(
        default=None, description="Reentry interval after application"
    )
    Reentry_expiration: str = Field(
        default=None, description="Expiration of the reentry interval"
    )
    Area_applied: str = Field(
        default=None,
        description="Area covered in the application",
        alias="Area Applied",
    )
    Rate_applied: str = Field(
        default=None, description="Rate of application", alias="Rate Applied"
    )
    Total_applied: str = Field(
        default=None, description="Total quantity applied", alias="Total Applied"
    )
    Notes: Optional[str] = Field(default=None, description="...")

    # _ensure_str = field_validator('field_name', 'task_dates', 'boundary_name',
    #                         'task_name', 'task_subtype', 'crop_subspecies',
    #                         'operator', 'operator_license_number', 'boundary_lat_lng',
    #                         'equipment', 'notes', mode="before", allow_reuse=True)(str)


class GranularPlantingData(GranularData):
    Planted_area: Optional[float] = Field(
        default=None,
        description="Total area that has been planted, measured in appropriate units (e.g., acres, hectares)",
    )
    Rate_applied: Optional[str] = Field(
        default=None,
        description="Application rate of planting, could include details like seeds per unit area or fertilizer application rate",
        validate_default=True,
    )
    Total_applied: Optional[str] = Field(
        default=None,
        description="Total quantity of seeds, fertilizers, or other materials applied during planting",
        validate_default=True,
    )

    @field_validator("Planted_area", mode="before")
    def ensure_float(cls, v):
        if isinstance(v, (float, int)):
            return float(v)
        return v

    @field_validator("Rate_applied", "Total_applied", mode="before")  # , always=True)
    def ensure_str(cls, v):
        return str(v) if v is not None else None


"""JSON DATA"""


class ProductInfo(BaseModel):
    product_name: Optional[str] = Field(default=None, description="Name of the product")
    product_category: Optional[str] = Field(
        default=None, description="Category of the product"
    )
    product_details: Optional[str] = Field(
        default=None, description="Details of the product"
    )


class SprayData(BaseModel):
    tank_name: str = Field(default=None, description="Name of the tank")
    avg_rate: float = Field(default=None, description="Average rate of spraying")
    total_applied: float = Field(default=None, description="Total quantity applied")
    product_info: ProductInfo = Field(
        default_factory=ProductInfo, description="Product information"
    )


class FuelConsumption(BaseModel):
    total_gal: float = Field(default=None, description="Total gallons of fuel consumed")


class SeedVarietyInfo(BaseModel):
    pass


class SeedData(BaseModel):
    seeds_per_acre: float = Field(default=None, description="Seeds planted per acre")
    total_seeds: float = Field(
        default=None, description="Total number of seeds planted"
    )
    seed_variety_info: Optional[SeedVarietyInfo] = Field(
        default=None, description="Information about the seed variety"
    )


class CropInfo(BaseModel):
    crop_category: str = Field(default=None, description="Category of the crop")
    crop_type: str = Field(default=None, description="Type of the crop")


class FieldEvent(BaseModel):
    activity: str = Field(default=None, description="Type of field activity")
    date: str = Field(default=None, description="Date of the activity")
    pulse_count: int = Field(default=0, description="Pulse count during the activity")
    fuel_consumption: FuelConsumption = Field(
        default_factory=FuelConsumption, description="Fuel consumption data"
    )
    crop_info: Optional[CropInfo] = Field(default=None, description="Crop information")
    seed_data: Optional[SeedData] = Field(default=None, description="Seed data")
    spray_data: Optional[List[SprayData]] = Field(
        default=None, description="List of Spray data"
    )


class Metadata(BaseModel):
    partner_name: str = Field(default=None, description="Name of the partner")
    grower_name: str = Field(default=None, description="Name of the grower")
    farm_name: str = Field(default=None, description="Name of the farm")
    field_name: str = Field(default=None, description="Name of the field")
    field_acres: float = Field(default=None, description="Total acres of the field")


class FieldEventsSummary(BaseModel):
    Data_source: str = Field(default="FM", description="Source of the data")
    total_fuel_consumption: float = Field(
        default=None, description="Total fuel consumption"
    )
    activities: List[str] = Field(
        default_factory=list, description="List of activities"
    )
    crop_info: CropInfo = Field(
        default_factory=CropInfo, description="Crop information"
    )
    metadata: Metadata = Field(default_factory=Metadata, description="Metadata")


class FarMobileData(DAMeta):
    # id: str = Field(default=None, description="Unique identifier")
    # start_date: str = Field(default=None, description="Start date of the data range")
    # end_date: str = Field(default=None, description="End date of the data range")
    # enrollment_id: str = Field(default=None, description="Enrollment identifier")
    # field_events: List[FieldEvent] = Field(
    #     default_factory=list, description="List of field events"
    # )
    # field_events_summary: FieldEventsSummary = Field(
    #     default_factory=FieldEventsSummary, description="Summary of field events"
    # )
    # created_at: str = Field(default=None, description="Creation date of the record")
    # updated_at: Optional[str] = Field(
    #     default=None, description="Date of the last update"
    # )
    EFR_FMID: str  # f66f26e82
    GRWR_FMID: str  # fd6646fca
    GRWR_SRCID: Optional[str]  #       NaN
    GRWR_NM: str  # Casper Farms
    FARM_FMID: str  #   fa59d8ca8
    FARM_SRCID: Optional[str]  #         NaN
    FARM_NM: str  # Casper Farms
    FLD_FMID: str  #    fa215a14d
    FLD_SRCID: Optional[str]  #          NaN
    FLD_NM: str  #     Johnsons
    BOUND_TYPE: str  # user_entered
    BOUND_ACRE: float  #     187.201422
    SECTION: Optional[int]
    TOWNSHIP: Optional[str]  #        110N
    RANGE: Optional[str]  #         55W
    CITY: Optional[str]  #         NaN
    COUNTY: Optional[str]  # Kingsbury County
    STATE: Optional[str]  #   South Dakota
    COUNTRY: Optional[str]  #  United States
    CROP_CYCLE: int  #          2022
    VARIETY: Optional[str]  #        NaN
    CLU_ID: Optional[str]  #       NaN

    @model_validator(mode="before")
    def convert_na_to_none(cls, values):
        for value in values:
            print(value, values[value])
            if pd.isna(values[value]):
                values[value] = None
        return values


class FarMobileApplication(FarMobileData):
    CROP_NM: Optional[str]  #         NaN
    CMDTY_CODE: Optional[int]  #         NaN
    # Application (spraying) specific columns
    SPRAY_ACRE: float  # 12.571539
    AVG_RATE: float  # 18.293982
    AVG_PRESS: float  # 65.385106
    PRODUCT: str  # Soybean Post
    SP_STRT: datetime  # 2022-06-27 20:06:00
    SP_END: datetime  # 2022-06-27 20:22:52
    C_AVG_RATE: Optional[float]  #          NaN
    C_AVG_PRES: Optional[float]  #         NaN
    C_PRODUCT: Optional[str]  #        NaN
    C_SP_STRT: Optional[datetime]  #       NaN
    C_SP_END: Optional[datetime]  #      NaN


class FarMobileHarvest(FarMobileData):
    CROP_NM: str
    CMDTY_CODE: int
    # Harvest specific columns
    HRVST_ACRE: float
    TOT_DPRD: float
    TOT_WWGT: float
    DRY_YIELD: float
    WET_YIELD: float
    AVG_RATE: float
    MOISTURE: float
    HRVST_STRT: datetime
    HRVST_END: datetime
    C_CROP: Optional[str]
    C_VARIETY: Optional[str]
    C_TOT_WWGT: Optional[float]
    C_TOT_DPRD: Optional[float]
    C_WET_YLD: Optional[float]
    C_DRY_YLD: Optional[float]
    C_AVG_RATE: Optional[float]
    C_MOISTURE: Optional[float]
    C_HR_STRT: Optional[datetime]
    C_HR_END: Optional[datetime]


class FarMobileTillage(FarMobileData):
    CROP_NM: Optional[str]  #         NaN
    CMDTY_CODE: Optional[int]  #         NaN
    # Tillage specific columns
    TILL_ACRE: float
    COV_CROP: Optional[str]
    TILL_TYPE: Optional[str]
    TILL_STRT: datetime
    TILL_END: datetime
    C_COV_CROP: Optional[str]
    C_TIL_TYPE: Optional[str]
    C_TIL_STRT: Optional[datetime]
    C_TIL_END: Optional[datetime]
    IMPLEMENT_TYPE: str
    TILLAGE_DEPTH_1: float
    TILLAGE_DEPTH_2: Optional[float]
    TILLAGE_DEPTH_3: Optional[float]
    C_IMPLEMENT_TYPE: Optional[str]
    C_TILLAGE_DEPTH_1: Optional[float]
    C_TILLAGE_DEPTH_2: Optional[float]
    C_TILLAGE_DEPTH_3: Optional[float]


""" ### DATA TEMPLATE FILES ########################################### """


class DataTemplate(DAMeta):
    Data_source: str = Field(default="UNKNWON")
    Client: Optional[str]
    Farm_name: str
    Field_name: str
    Boundary_name: Optional[str] = Field(default=None)
    Operation_start: Optional[datetime] = Field(default=None)
    Operation_end: Optional[datetime] = Field(default=None)
    Total_fuel: Optional[float] = Field(default=None, ge=0)
    Fuel_unit: Optional[str] = Field(default=None)
    Task_name: str
    Crop_type: Optional[str]
    Area_applied: Optional[float]
    Applied_unit: Optional[str]

    @field_validator("Farm_name", "Field_name", "Task_name", mode="before")
    @classmethod
    def value_not_none(cls, value):
        if pd.isna(value) or value == "":
            raise ValueError("Missing value, cannot be `None`")
        return value

    @field_validator("Boundary_name", mode="before")
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value


class PlantingDataTemplate(DataTemplate):
    Product: str
    Manufacturer: Optional[str]
    Applied_rate: Optional[float]
    Applied_total: Optional[float]
    Operation_type: str = Field(default="Planting")
    Product_type: Optional[str] = Field(default=None)

    @field_validator("Product", mode="before")
    @classmethod
    def product_not_none(cls, value):
        if pd.isna(value) or value == "":
            raise ValueError("Missing value for 'Product'")
        return value

    @model_validator(mode="before")
    def check_area_and_amount(cls, values):
        """We need at least 2 out of the 3 following parameters to derive required
        information on area applied and amount applied of a given input product:

        - Area_applied
        - Applied_total
        - Applied_rate
        """
        area_applied = values.get("Area_applied")
        applied_total = values.get("Applied_total")
        applied_rate = values.get("Applied_rate")
        if (
            pd.isna(area_applied)
            and pd.isna(applied_total)
            or pd.isna(area_applied)
            and pd.isna(applied_rate)
            or pd.isna(applied_total)
            and pd.isna(applied_rate)
        ):
            raise ValueError(
                f"For product `{values.get('Product')}`: At least 2 of the following need to be filled: Area_applied={area_applied}, Applied_total={applied_total}, Applied_rate={applied_rate}"
            )
        return values

    @field_validator("Manufacturer", mode="before")
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value


class ApplicationDataTemplate(DataTemplate):
    Product: str
    Manure_type: Optional[str]
    Manufacturer: Optional[str]
    Reg_number: Optional[str]
    Applied_rate: Optional[float]
    Applied_total: Optional[float]
    Operation_type: str = "Application"

    @field_validator("Product", mode="before")
    @classmethod
    def product_not_none(cls, value):
        if pd.isna(value) or value == "":
            raise ValueError("Missing value for 'Product'")
        return value

    @model_validator(mode="before")
    def check_area_and_amount(cls, values):
        """We need at least 2 out of the 3 following parameters to derive required
        information on area applied and amount applied of a given input product:

        - Area_applied
        - Applied_total
        - Applied_rate
        """
        area_applied = values.get("Area_applied")
        applied_total = values.get("Applied_total")
        applied_rate = values.get("Applied_rate")
        if (
            pd.isna(area_applied)
            and pd.isna(applied_total)
            or pd.isna(area_applied)
            and pd.isna(applied_rate)
            or pd.isna(applied_total)
            and pd.isna(applied_rate)
        ):
            raise ValueError(
                f"For product `{values.get('Product')}`: At least 2 of the following need to be filled: Area_applied={area_applied}, Applied_total={applied_total}, Applied_rate={applied_rate}"
            )
        return values


class HarvestDataTemplate(DataTemplate):
    Field_name: str
    Crop_type: str
    Applied_rate: Optional[float] = Field(ge=0.0)
    Total_dry_yield: Optional[float] = Field(..., ge=0.0)
    Moisture: float = Field(..., ge=0.0)
    Operation_type: str = "Harvest"

    @field_validator("Crop_type", mode="before")
    @classmethod
    def crop_type_not_none(cls, value):
        """For harvest operations, the crop type is obligatory."""
        if pd.isna(value) or value == "":
            raise ValueError("Missing crop type for harvest operation")
        return value

    @model_validator(mode="before")
    def check_area_and_amount(cls, values):
        """We need at least 2 out of the 3 following parameters to derive required
        information on area applied and amount applied of a given input product:

        - Area_applied
        - Applied_total
        - Applied_rate
        """
        area_applied = values.get("Area_applied")
        applied_total = values.get("Total_dry_yield")
        applied_rate = values.get("Applied_rate")
        if (
            pd.isna(area_applied)
            and pd.isna(applied_total)
            or pd.isna(area_applied)
            and pd.isna(applied_rate)
            or pd.isna(applied_total)
            and pd.isna(applied_rate)
        ):
            raise ValueError(
                f"For product `{values.get('Product')}`: At least 2 of the following need to be filled: Area_applied={area_applied}, Total_dry_yield={applied_total}, Applied_rate={applied_rate}"
            )
        return values


class TillageDataTemplate(DataTemplate):
    Product: Optional[str]
    Manufacturer: Optional[str]
    Reg_number: Optional[str]
    Applied_rate: float
    Applied_total: float
    Operation_type: str = Field(default="Harvest")
    Product_type: Optional[str]

    @field_validator(
        "Applied_unit", "Product", "Manufacturer", "Reg_number", mode="before"
    )
    def convert_na_to_none(cls, value):
        if pd.isna(value):
            return None
        return value


""" MAPPING FILES ###################################################"""


class MappingData(BaseModel):
    model_config = ConfigDict(frozen=True)


class InputStatusError(Exception):
    def __init__(self, value, message):
        self.value = value
        self.message = message
        super().__init__(message)


class ProductStateError(Exception):
    def __init__(self, value, message):
        self.value = value
        self.message = message
        super().__init__(message)


class ManureTypeError(Exception):
    def __init__(self, value, message):
        self.value = value
        self.message = message
        super().__init__(message)


class InputBreakdownData(MappingData):
    status: Optional[str]
    product_name: str
    manufacturer: Optional[str]
    product_type: str
    product_state: Optional[str]
    EEF: Optional[bool]
    crop_type: Optional[str]
    manure_type: Optional[str]
    # Density conversion factors
    dens_lbs_per_gal: float = Field(ge=0.0)
    dens_g_per_ml: float = Field(ge=0.0)
    # Chemical composition of fertilizers in percent
    perc_N: float = Field(ge=0, le=1)
    perc_P2O5: float = Field(ge=0.0, le=1.0)
    perc_K2O: float = Field(ge=0.0, le=1.0)
    perc_CaCO3: float = Field(ge=0.0, le=1.0)
    perc_Ammonia: float = Field(ge=0.0, le=1.0)
    perc_Urea: float = Field(ge=0.0, le=1.0)
    perc_AN: float = Field(ge=0.0, le=1.0)
    perc_AS: float = Field(ge=0.0, le=1.0)
    perc_UAN: float = Field(ge=0.0, le=1.0)
    perc_MAP_N: float = Field(ge=0.0, le=1.0)
    perc_DAP_N: float = Field(ge=0.0, le=1.0)
    perc_potassium_nitrate_N: float = Field(ge=0.0, le=1.0)
    perc_calcium_nitrate_N: float = Field(ge=0.0, le=1.0)
    perc_calcium_ammonium_nitrate_N: float = Field(ge=0.0, le=1.0)
    perc_AN_phosphate_N: float = Field(ge=0.0, le=1.0)
    perc_MAP_P2O5: float = Field(ge=0.0, le=1.0)
    perc_DAP_P2O5: float = Field(ge=0.0, le=1.0)
    perc_AN_phosphate_P2O5: float = Field(ge=0.0, le=1.0)
    perc_phosphate_rock_P2O5: float = Field(ge=0.0, le=1.0)
    perc_phosphoric_acid: float = Field(ge=0.0, le=1.0)
    perc_single_superphosphate_P2O5: float = Field(ge=0.0, le=1.0)
    perc_thomas_meal_P2O5: float = Field(ge=0.0, le=1.0)
    perc_triple_superphosphate_P2O5: float = Field(ge=0.0, le=1.0)
    perc_potassium_chloride_K2O: float = Field(ge=0.0, le=1.0)
    perc_potassium_nitrate_K2O: float = Field(ge=0.0, le=1.0)
    perc_potassium_sulphate_K2O: float = Field(ge=0.0, le=1.0)
    # Pesticide parameters (HERBICIDE, INSECTICIDE, FUNGICIDE)
    perc_total_AI_incl_surfact: float = Field(ge=0.0, le=1.0)
    perc_total_AI_excl_surfact: float = Field(ge=0.0, le=1.0)
    lbs_AI_per_gal: Optional[float] = Field(ge=0.0)

    @field_validator("status")
    @classmethod
    def status_acceptable(cls, value):
        if value in ["verified", "new", None, "PRIO"]:
            return value
        raise InputStatusError(
            value=value, message=f"Invalid value for status: {value}"
        )

    @model_validator(mode="before")
    def validate_product_state_and_dens_conversion(cls, values):
        """Validates parameters `product_state` and `dens_lbs_per_gal` for products with `product_type`:

        - herbicide
        - insecticide
        - fungicide
        - fertilizer
        - manure
        """
        product_type = values.get("product_type")
        product_name = values.get("product_name")
        # Check(s) specific to FERTILIZERS, PESTICIDES, and MANURE
        if product_type in [
            "herbicide",
            "insecticide",
            "fungicide",
            "fertilizer",
            "manure",
        ]:
            product_state = values.get("product_state")
            # Check if product state available for relevant product types
            if pd.isna(product_state):
                raise ValueError(f"Missing product state for product {product_name}")
            # Check product state validity
            if product_state not in ["liquid", "dry"]:
                raise ProductStateError(
                    value=product_state,
                    message=f"Product state must be `liquid` or `dry` for product {product_name}",
                )

        # Check(s) specific to FERTILIZERS and MANURE
        if product_type in ["manure", "fertilizer"]:
            # Check density conversion factor validity
            dens_factor = values.get("dens_lbs_per_gal")
            if dens_factor == 0.0 or pd.isna(dens_factor):
                raise ValueError(
                    f"Missing or wrong density conversion factor (lbs per gal) for product {product_name}"
                )

        # Check(s) specific to PESTICIDES
        if product_type in [
            "herbicide",
            "insecticide",
            "fungicide",
        ]:
            lbs_ai_per_gal = values.get("lbs_AI_per_gal")
            # Check for pesticide product availability of active ingredients conversion factor
            if lbs_ai_per_gal == 0.0 or pd.isna(lbs_ai_per_gal):
                raise ValueError(
                    f"Missing active ingredients conversion factor for product {product_name}"
                )

        # Check(s) specific to MANURE
        if product_type == "manure":
            manure_type = values.get("manure_type")
            if pd.isna(manure_type) or manure_type == "":
                raise ValueError(
                    f"Manure products need the manure type specified; missing for product {product_name}"
                )
            if manure_type.lower() not in [
                "beef cattle",
                "dairy cow",
                "swine",
                "chicken",
                "other",
            ]:
                raise ManureTypeError(
                    value=manure_type,
                    message="Invalid manure type, must be ['beef cattle', 'dairy cow', 'swine', 'chicken', 'other']",
                )

        # All available product_types (for reference)
        #
        # herbicide
        # fertilizer
        # seed
        # manure
        # insecticide
        # surfactant
        # fungicide
        # carbon soil ammendment
        # microbial
        # lime
        # other
        return values

    @field_validator("EEF", mode="before")
    @classmethod
    def eef_acceptable(cls, value):
        if pd.isna(value):
            return False
        return value

    @model_validator(mode="before")
    def validate_percentages_and_densities(cls, field_values):
        """Validator for all float fields (percentages and density values). Fills NaN values with 0.0,
        ensures non-negative values and percentages being in the range 0.0 <= val <= 1.0
        """
        for value in field_values:
            if (
                value.startswith("perc_")
                or value.startswith("dens_")
                or value == "lbs_AI_per_gal"
            ):
                if pd.isna(field_values.get(value)):
                    field_values[value] = 0.0

        return field_values


class FieldNameMappingData(MappingData):
    system: str
    farm_name: str | None
    name: str
    system_acres: float | None = Field(ge=0.0)
    clear_name: str | None
    clear_acres: float | None = Field(ge=0.0)


class ProductNameMappingData(MappingData):
    name: str | None
    clear_name: str | None


class CoverCropTableData(MappingData):
    Cover_crop_type: str
    N_content_above: float = Field(ge=0.0)
    N_content_below: float = Field(ge=0.0)
    N_content_total: float = Field(ge=0.0)
    Yield_mt_per_hectare: float | None = Field(ge=0.0)


class StandardUnits(str, Enum):
    ACRE = "AC"
    BAG_OF_SEEDS = "BAG"  # 80,000 kernels per BAG
    BUSHEL = "BU"
    CORN_BALE = "CBL"
    DRY_OUNCE = "OZ"
    FLUID_OUNCE = "FL_OZ"
    GALLON = "GAL"
    GRAMM = "G"
    INCH = "IN"
    METRIC_TON = "MT"
    POUND = "LBS"
    PINT = "PINT"
    QUARTER = "QT"
    SHORT_TON = "TN"


class UnitNameMappingData(MappingData, use_enum_values=True):
    unit: str
    clear_unit: StandardUnits
    system: str | None
    comment: str | None


class UnitConversionTableData(MappingData, use_enum_values=True):
    unit: str
    target_unit: StandardUnits
    conversion_factor: float = Field(ge=0.0)
    comment: str | None
