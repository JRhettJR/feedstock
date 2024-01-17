import logging
import pathlib
import re
from abc import ABC, abstractmethod

# from dataclasses import field
from typing import Any, Generic, List, Optional, TypeVar

import gcsfs
import pandas as pd
from data_aggregators.clean import Base
from data_aggregators.schema import (
    ApplicationDataTemplate,
    CFVApplicationReport,
    CFVData,
    CFVHarvestReport,
    CFVPlantingReport,
    CoverCropTableData,
    DataTemplate,
    FarMobileApplication,
    FarMobileData,
    FarMobileHarvest,
    FarMobileTillage,
    FieldNameMappingData,
    GranularApplicationData,
    GranularData,
    GranularHarvestData,
    HarvestDataTemplate,
    InputBreakdownData,
    MyJohnDeereApplication,
    MyJohnDeereData,
    MyJohnDeereHarvest,
    MyJohnDeerePlanting,
    MyJohnDeereTillage,
    PlantingDataTemplate,
    ProductNameMappingData,
    TillageDataTemplate,
    UnitConversionTableData,
    UnitNameMappingData,
)
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass

from src.feedstock_aggregation_scripts.data_prep.constants import CROP_TYPES

GOOGLE_CLOUD_FILE_SYSTEM = gcsfs.GCSFileSystem()

T = TypeVar("T")


class DataFile(BaseModel, Generic[T]):
    file_name: str
    operations: List[T] = []

    def add_operation(self, operation: T):
        self.operations.append(operation)

    @classmethod
    @abstractmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        with GOOGLE_CLOUD_FILE_SYSTEM.open(file_path, "rb") as f:
            if file_path.endswith(".csv"):
                return pd.read_csv(f)
            elif file_path.endswith(".xlsx"):
                return pd.read_excel(f, engine="openpyxl")
            elif file_path.endswith(".xls"):
                return pd.read_excel(f, engine="xlrd")
            else:
                raise ValueError("Unsupported file format")

    @classmethod
    @abstractmethod
    def from_file(cls, file_path: str) -> "DataFile":
        pass


class ClimateFieldViewFile(DataFile[CFVData]):
    def add_operation(self, operation: "CFVData"):
        self.operations.append(operation)

    @classmethod
    def read_file(cls, file_path):
        return super(ClimateFieldViewFile, cls).read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str) -> "ClimateFieldViewFile":
        df = cls.read_file(file_path)
        file = cls(file_name=file_path)
        cleaner = Base(filepath=file_path)

        crop_type = pathlib.Path(file_path).stem.split("_")[-1]

        df["Crop_type"] = crop_type if crop_type in CROP_TYPES else None
        df = df.rename(
            columns={
                "Unnamed: 0": "Client",
                "Unnamed: 1": "Farm_name",
            }
        )
        # print(df)

        # df = cleaner.standardize_header(df=df)

        # file_instance = cls(file_name=file_path)
        # cls.add_operations(file_instance, df, cleaner.report_type)
        # return file_instance

        for _, row in df.iterrows():
            row_data = row.to_dict()

            if cleaner.report_type == "Application":
                operation = CFVApplicationReport(**row_data)
            elif cleaner.report_type == "Harvest":
                operation = CFVHarvestReport(**row_data)
            elif cleaner.report_type == "Seeding" or cleaner.report_type == "Planting":
                operation = CFVPlantingReport(**row_data)
            else:
                continue

            operation.File_type = cleaner.report_type
            file.add_operation(operation)

        return file

    @classmethod
    def add_operations(
        cls,
        file_instance: "ClimateFieldViewFile",
        df: pd.DataFrame,
        report_type: str,
    ):
        for _, row in df.iterrows():
            operation = cls.create_operation(report_type, row)
            if operation:
                file_instance.add_operation(operation)

    @staticmethod
    def create_operation(report_type: str, row: pd.Series) -> Optional[CFVData]:
        operation = None
        if report_type == "Application":
            operation = CFVApplicationReport(**row.to_dict())
        elif report_type == "Harvest":
            operation = CFVHarvestReport(**row.to_dict())
        elif report_type == "Planting":
            operation = CFVPlantingReport(**row.to_dict())
        return operation

    @staticmethod
    def process_files_in_gcs_folder(
        bucket_name: str, folder_path: str
    ) -> List[CFVData]:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []
        grower_name = folder_path.strip("/").split("/")[-1]
        report_types = ["Harvest", "Planting", "Application"]

        for file in files:
            file_lower = file.lower()
            if (
                "@" in file_lower
                and grower_name.lower() in file_lower
                and any(rt.lower() in file_lower for rt in report_types)
            ):
                try:
                    cfv_file = ClimateFieldViewFile.from_file(file_path=file)
                    combined_data.extend(cfv_file.operations)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        return combined_data

    @staticmethod
    def process_file_by_type_in_gcs(
        bucket_name: str, folder_path: str, file_type: str
    ) -> List[CFVData]:
        """Reads only file by file_type from GCS bucket.

        `file_type` is using constants defined at `src/feedstock_aggregation_scripts/data_prep/constants.py`.
        Those file types are defined to contain parts of the name of the target file."""
        files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")

        combined_data = []

        for file in files:
            if "@" in file and file_type in file:
                try:
                    cfv_file = ClimateFieldViewFile.from_file(file_path=file)
                    combined_data.extend(cfv_file.operations)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        return combined_data


# @dataclass
class MyJohnDeereFile(DataFile[MyJohnDeereData]):
    # file_name: str
    # operations: List["MyJohnDeereData"] = Field(default_factory=list)

    def add_operation(self, operation: "MyJohnDeereData"):
        self.operations.append(operation)

    @classmethod
    def read_file(cls, file_path):
        return super(MyJohnDeereFile, cls).read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str):
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)
        cleaner = Base(filepath=file_path)

        # measurement_fields = {
        #     "Application": [
        #         "Area_Applied",
        #         "Rate",
        #         "Total_Applied",
        #         "Target_Rate",
        #         "Target_Total",
        #         "Speed",
        #     ],
        #     "Harvest": [
        #         "Area_Harvested",
        #         "Moisture",
        #         "Dry_Yield",
        #         "Total_Dry_Yield",
        #         "Wet_Weight",
        #         "Total_Wet_Weight",
        #         "Speed",
        #     ],
        #     "Seeding": [
        #         "Area_Seeded",
        #         "Rate",
        #         "Total_Applied",
        #         "Target_Rate",
        #         "Target_Total",
        #         "Speed",
        #     ],
        #     "Tillage": [
        #         "Area_Tilled",
        #         "Depth",
        #         "Target_Depth",
        #         "Target_Pressure",
        #         "Speed",
        #     ],
        # }

        # columns = list(df.columns)
        for _, row in df.iterrows():
            row_data = row.to_dict()

            #     if cleaner.report_type in measurement_fields:
            #         for m_field in measurement_fields[cleaner.report_type]:
            #             if m_field in row_data:
            #                 field_index = columns.index(m_field)
            #                 unit_field = (
            #                     columns[field_index + 1]
            #                     if field_index + 1 < len(columns)
            #                     else None
            #                 )
            #                 value = row_data[m_field]
            #                 unit = row_data[unit_field] if unit_field else None
            #                 row_data[m_field] = Measurement(value=value, unit=unit)

            if cleaner.report_type == "Application":
                operation = MyJohnDeereApplication(**row_data)
            elif cleaner.report_type == "Harvest":
                operation = MyJohnDeereHarvest(**row_data)
            elif cleaner.report_type == "Seeding":
                operation = MyJohnDeerePlanting(**row_data)
            elif cleaner.report_type == "Tillage":
                operation = MyJohnDeereTillage(**row_data)
            else:
                continue

            operation.File_type = cleaner.report_type
            file.add_operation(operation)

        return file

    @staticmethod
    def process_files_in_gcs_folder(bucket_name: str, folder_path: str) -> List:
        files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        combined_operations = []

        for file in files:
            if re.search(
                r"(Harvest|Seeding|Application|Tillage)_[0-9]{4}_[a-zA-Z]*\.xlsx$", file
            ):
                john_deere_file = MyJohnDeereFile.from_file(file)
                combined_operations.extend(john_deere_file.operations)

        return combined_operations

    @staticmethod
    def process_file_by_type_in_gcs(
        bucket_name: str, folder_path: str, file_type: str
    ) -> List[MyJohnDeereData]:
        """Reads only file by file_type from GCS bucket."""
        files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        combined_operations = []

        for file in files:
            if re.search(rf"{file_type}_[0-9]{{4}}_[a-zA-Z]*\.xlsx", file):
                john_deere_file = MyJohnDeereFile.from_file(file)
                combined_operations.extend(john_deere_file.operations)

        return combined_operations


# @dataclass
class GranularFile(DataFile[GranularData]):
    # file_name: str
    # operations: List["GranularData"] = Field(default_factory=list)

    def add_operation(self, operation: "GranularData"):
        self.operations.append(operation)

    @classmethod
    def read_file(cls, file_path):
        return super(GranularFile, cls).read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str):
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)

        for _, row in df.iterrows():
            if "yield" in file.file_name.lower():
                operation = GranularHarvestData(**row.to_dict())
                operation.File_type = "Harvest"
            elif "application" in file.file_name.lower():
                operation = GranularApplicationData(**row.to_dict())
                operation.File_type = "Application"

            # Planting and tillage data is extracted from the origin file used for application data.
            # Planting and tillage files are stored in target column header format. Hence, the use of
            # DataTemplate files to read them in.
            elif "planting" in file.file_name.lower():
                operation = PlantingDataTemplate(**row.to_dict())
                operation.File_type = "Planting"
                operation.Data_source = "Granular"
            elif "tillage" in file.file_name.lower():
                operation = TillageDataTemplate(**row.to_dict())
                operation.File_type = "Tillage"
                operation.Data_source = "Granular"
            else:
                continue

            file.add_operation(operation)

        return file

    @staticmethod
    def process_files_in_gcs_folder(bucket_name: str, folder_path: str):
        files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        combined_operations = []

        for file in files:
            if re.match(r"(.*\d{6,}.*)_(\d{4})\.csv$", file, re.IGNORECASE):
                granular_file = GranularFile.from_file(file)
                combined_operations.extend(granular_file.operations)
        return combined_operations

    @staticmethod
    def process_file_by_type_in_gcs(
        bucket_name: str, folder_path: str, file_type: str
    ) -> List[GranularData]:
        """Reads only file by file_type from GCS bucket."""
        files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        combined_operations = []

        for file in files:
            if file_type in file or f"Granular_{file_type}" in file:
                granular_file = GranularFile.from_file(file)
                combined_operations.extend(granular_file.operations)

        return combined_operations


# @dataclass
class FarmMobileFile(DataFile):
    # file_name: str
    # operations: List[FarMobileData] = Field(default_factory=list)

    def add_operation(self, operation: FarMobileData):
        if not isinstance(operation, FarMobileData):
            raise TypeError("operation must be an instance of FarMobileData")
        self.operations.append(operation)

    @classmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        return super(FarmMobileFile, cls).read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str) -> "FarmMobileFile":
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)
        # cleaner = Base(filepath=file_path)

        for _, row in df.iterrows():
            if "_SP_" in file_path:
                operation = FarMobileApplication(**row.to_dict())
                operation.File_type = "Application"
            elif "_HR_" in file_path:
                operation = FarMobileHarvest(**row.to_dict())
                operation.File_type = "Harvest"
            elif "_TL_" in file_path:
                operation = FarMobileTillage(**row.to_dict())
                operation.File_type = "Tillage"
            else:
                continue
            file.add_operation(operation)

        return file

    @staticmethod
    def process_file_by_type_in_gcs(
        bucket_name: str, folder_path: str, file_type: str
    ) -> List[FarMobileData]:
        """Reads only file by file_type from GCS bucket."""
        files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        combined_operations = []

        for file in files:
            if file_type in file and "efr_data_" in file:
                fm_file = FarmMobileFile.from_file(file)
                combined_operations.extend(fm_file.operations)

        return combined_operations


# @dataclass
class DataTemplateFile(DataFile[DataTemplate]):
    # file_name: str
    # operations: List["DataTemplate"] = Field(default_factory=list)

    def add_operation(self, operation: "DataTemplate"):
        self.operations.append(operation)

    @classmethod
    def read_file(cls, file_path):
        return super(DataTemplateFile, cls).read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str):
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)

        cleaner = Base(filepath=file_path)
        file_name = file_path.strip("/").split("/")[-1]

        for _, row in df.iterrows():
            # Replace np.nan values in row with None --> enables Pydantic standard validation without
            # the need to create extra checks for `np.nan` cases
            row = row.where(pd.notnull(row), None)
            # print(row)

            if cleaner.report_type == "application":
                operation = ApplicationDataTemplate(**row.to_dict())
            elif cleaner.report_type == "harvest":
                operation = HarvestDataTemplate(**row.to_dict())
            elif cleaner.report_type == "planting":
                operation = PlantingDataTemplate(**row.to_dict())
            else:
                continue

            operation.Client = file_name.split(
                f"_{cleaner.report_type}_data_template_"
            )[0]
            operation.Data_source = file_name.split(
                f"_{cleaner.report_type}_data_template_"
            )[-1].split(".csv")[0]

            file.add_operation(operation)

    @staticmethod
    def process_files_in_gcs_folder(bucket_name: str, folder_path: str) -> List:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []
        grower_name = folder_path.strip("/").split("/")[-1]
        report_types = ["Harvest", "Planting", "Application"]
        print(folder_path)
        # data_source =
        # crop_types = ["Corn", "Soybean", "Soybeans"]

        for file in files:
            print(file)
            file_lower = file.lower()

            if (
                grower_name.lower() in file_lower
                and any(rt.lower() in file_lower for rt in report_types)
                and "_data_template_" in file_lower
            ):
                try:
                    template_file = DataTemplateFile.from_file(file_path=file)
                    combined_data.extend(template_file.operations)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        return combined_data


""" MAPPING FILES """


class MappingFile(ABC):
    file_name: str
    mappings: List = Field(default_factory=list)

    def __init__(self, file_name: str):
        self.file_name = file_name

    def add_mapping(self, mapping):
        self.mappings.append(mapping)

    @classmethod
    @abstractmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        with GOOGLE_CLOUD_FILE_SYSTEM.open(file_path, "rb") as f:
            if file_path.endswith(".csv"):
                return pd.read_csv(f)
            elif file_path.endswith(".xlsx"):
                return pd.read_excel(f, engine="openpyxl")
            elif file_path.endswith(".xls"):
                return pd.read_excel(f, engine="xlrd")
            else:
                raise ValueError("Unsupported file format")

    @classmethod
    @abstractmethod
    def from_file(cls, file_path: str) -> "DataFile":
        pass


@dataclass
class InputBreakdownTable(MappingFile):
    file_name: str
    mappings: List[InputBreakdownData] = Field(default_factory=list)

    def add_mapping(self, mapping: InputBreakdownData):
        if not isinstance(mapping, InputBreakdownData):
            raise TypeError("mapping must be an instance of InputBreakdownData")
        self.mappings.append(mapping)

    @classmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        return super(InputBreakdownTable, cls).read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str) -> "InputBreakdownTable":
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)

        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            print(row)
            mapping = InputBreakdownData(**row.to_dict())
            file.add_mapping(mapping)

        return file

    def query_entry(self, **kwargs) -> Any:
        return super().query_entry(**kwargs)

    @staticmethod
    def process_file_in_gcs_folder(
        bucket_name: str, folder_path: str
    ) -> List[InputBreakdownData]:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []

        for file in files:
            if "input_breakdown_table" in file.lower():
                try:
                    combined_data = InputBreakdownTable.from_file(file_path=file)

                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        # Converts Pydantic model to data frame with propper column names
        # pd.DataFrame([model.dict() for model in ib_table.mappings])
        return combined_data


@dataclass
class FieldNameMapping(MappingFile):
    """Grower specific mapping file to convert field_name - farm_name combinations
    into respective clear names."""

    grower: str
    file_name: str
    entries: List[FieldNameMappingData] = Field(default_factory=list)

    def add_entry(self, entry: FieldNameMappingData):
        if not isinstance(entry, FieldNameMappingData):
            raise TypeError("mapping must be an instance of FieldNameMappingData")
        self.entries.append(entry)

    @classmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        return super().read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str, grower: str) -> "FieldNameMapping":
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path, grower=grower)

        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            # print(row)
            entry = FieldNameMappingData(**row.to_dict())
            file.add_entry(entry)

        return file

    def query_entry(
        self, field_name: str, farm_name: str | None = None
    ) -> FieldNameMappingData:
        """Returns entry with matching parameters for `field_name` and `farm_name`. If `farm_name`
        equals `None`, it matches only using the `field_name`. If no entry entry is available, returns `None`.

        NOTE: returns only the FIRST entry that matches the query parameters.

        Entries contain:

        - system: str
        - farm_name: str | None
        - name: str
        - system_acres: float | None
        - clear_name: str | None
        - clear_acres: float | None
        """

        if field_name is None:
            raise ValueError("field_name must be given for query")

        for entry in self.entries:
            if entry.farm_name == farm_name and entry.name == field_name:
                return entry
        logging.warning(
            f"No matching clear name for inputs field_name: {field_name}, farm_name: {farm_name}"
        )
        return None

    @staticmethod
    def process_file_in_gcs_folder(
        bucket_name: str, folder_path: str
    ) -> List[FieldNameMappingData]:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []

        for file in files:
            grower = file.split("/")[-2]

            if "field_name_mapping" in file.lower():
                try:
                    combined_data = FieldNameMapping.from_file(
                        file_path=file, grower=grower
                    )
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        # Converts Pydantic model to data frame with propper column names
        # pd.DataFrame([model.dict() for model in ib_table.mappings])
        return combined_data


@dataclass
class ProductNameMapping(MappingFile):
    file_name: str
    entries: List[ProductNameMappingData] = Field(default_factory=list)

    def add_entry(self, entry: ProductNameMappingData):
        if not isinstance(entry, ProductNameMappingData):
            raise TypeError("mapping must be an instance of FieldNameMappingData")
        self.entries.append(entry)

    @classmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        return super().read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str) -> "ProductNameMapping":
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)

        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            # print(row)
            entry = ProductNameMappingData(**row.to_dict())
            file.add_entry(entry)

        return file

    def query_entry(self, product_name: str) -> ProductNameMappingData:
        """Returns entry with matching parameter for `product_name`.
        If no entry entry is available, returns `None`.

        Entries contain:

        - name: str
        - clear_name: str | None
        """
        if product_name is None:
            raise ValueError("product_name must be given for query")

        for entry in self.entries:
            if entry.name == product_name:
                return entry
        logging.warning(
            f"No matching clear name for input product_name: `{product_name}`"
        )
        return None

    @staticmethod
    def process_file_in_gcs_folder(
        bucket_name: str, folder_path: str
    ) -> List[ProductNameMappingData]:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []

        for file in files:
            if "chemical_input_products_mapping_table" in file.lower():
                try:
                    combined_data = ProductNameMapping.from_file(file_path=file)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        # Converts Pydantic model to data frame with propper column names
        # pd.DataFrame([model.dict() for model in ib_table.mappings])
        return combined_data


@dataclass
class CoverCropTable(MappingFile):
    file_name: str
    entries: List[CoverCropTableData] = Field(default_factory=list)

    def add_entry(self, entry: CoverCropTableData):
        if not isinstance(entry, CoverCropTableData):
            raise TypeError("mapping must be an instance of CoverCropTableData")
        self.entries.append(entry)

    @classmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        return super().read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str) -> "CoverCropTable":
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)

        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            # print(row)
            entry = CoverCropTableData(**row.to_dict())
            file.add_entry(entry)

        return file

    def query_entry(self, cover_crop_name: str) -> CoverCropTableData | None:
        """Returns entry for `cover_crop_name`. If no entry entry is available, returns `None`.
        Entries contain:

        - Cover_crop_type: str
        - N_content_above: float
        - N_content_below: float
        - N_content_total: float
        - Yield_mt_per_hectare: float
        """
        if cover_crop_name is None:
            raise ValueError("cover_crop_name must be given for query")

        for entry in self.entries:
            if entry.Cover_crop_type == cover_crop_name:
                return entry
        logging.warning(
            f"No matching clear name for input product_name: `{cover_crop_name}`"
        )
        return None

    @staticmethod
    def process_file_in_gcs_folder(bucket_name: str, folder_path: str) -> pd.DataFrame:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []

        for file in files:
            if (
                "fd-cic-22_cover_crop_table_11.1a_template_including_yield"
                in file.lower()
            ):
                try:
                    combined_data = CoverCropTable.from_file(file_path=file)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        # Converts Pydantic model to data frame with propper column names
        # pd.DataFrame([model.dict() for model in ib_table.mappings])
        return combined_data


@dataclass
class UnitNameMapping(MappingFile):
    file_name: str
    entries: List[UnitNameMappingData] = Field(default_factory=list)

    def add_entry(self, entry: UnitNameMappingData):
        if not isinstance(entry, UnitNameMappingData):
            raise TypeError("mapping must be an instance of UnitNameMappingData")
        self.entries.append(entry)

    @classmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        return super().read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str) -> "UnitNameMapping":
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)

        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            # print(row)
            entry = UnitNameMappingData(**row.to_dict())
            file.add_entry(entry)

        return file

    def query_entry(self, unit_name: str) -> UnitNameMappingData | None:
        """Returns entry for `unit_name`. If no entry entry is available, returns `None`.
        Entries contain:

        - unit: str
        - clear_unit: StandardUnits
        - system: str | None (reference to where `unit` was used)
        - comment: str | None (further explanation to unit meaning)
        """
        if unit_name is None:
            raise ValueError("unit_name must be given for query")

        for entry in self.entries:
            if entry.unit == unit_name:
                return entry
        logging.warning(f"No matching entry for unit: `{unit_name}`")
        return None

    @staticmethod
    def process_file_in_gcs_folder(
        bucket_name: str, folder_path: str
    ) -> List[UnitNameMappingData]:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []
        grower_name = folder_path.strip("/").split("/")[-1]
        report_types = ["Harvest", "Planting", "Application"]

        for file in files:
            file_lower = file.lower()

            if (
                grower_name.lower() in file_lower
                and any(rt.lower() in file_lower for rt in report_types)
                and "_data_template_" in file_lower
            ):
                if "unit_mapping_table" in file.lower():
                    try:
                        combined_data = UnitNameMapping.from_file(file_path=file)
                    except Exception as e:
                        logging.error(f"Error processing file {file}: {e}")

        # Converts Pydantic model to data frame with propper column names
        # pd.DataFrame([model.dict() for model in ib_table.mappings])
        return combined_data


@dataclass
class UnitConversionTable(MappingFile):
    file_name: str
    entries: List[UnitConversionTableData] = Field(default_factory=list)

    def add_entry(self, entry: UnitConversionTableData):
        if not isinstance(entry, UnitConversionTableData):
            raise TypeError("mapping must be an instance of UnitConversionTableData")
        self.entries.append(entry)

    @classmethod
    def read_file(cls, file_path: str) -> pd.DataFrame:
        return super().read_file(file_path=file_path)

    @classmethod
    def from_file(cls, file_path: str) -> "UnitConversionTable":
        df = cls.read_file(file_path=file_path)
        file = cls(file_name=file_path)

        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            # print(row)
            entry = UnitConversionTableData(**row.to_dict())
            file.add_entry(entry)

        return file

    def query_entry(self, unit_name: str) -> UnitConversionTableData | None:
        """Returns entry for `unit_name`. If no entry entry is available, returns `None`.
        Entries contain:

        - unit: str
        - target_unit: StandardUnits (unit that `conversion_factor` converts `unit` to)
        - conversion_factor: float
        - comment: str | None (further explanation to units)
        """
        if self.entries == []:
            raise
        if unit_name is None:
            raise ValueError("unit_name must be given for query")

        for entry in self.entries:
            if entry.unit == unit_name.lower():
                return entry
        logging.warning(f"No matching entry for unit: `{unit_name}`")
        return None

    @staticmethod
    def process_file_in_gcs_folder(
        bucket_name: str, folder_path: str
    ) -> List[UnitConversionTableData]:
        try:
            files = GOOGLE_CLOUD_FILE_SYSTEM.ls(f"{bucket_name}/{folder_path}")
        except Exception as e:
            logging.error(
                f"Error accessing files in bucket {bucket_name} at {folder_path}: {e}"
            )
            return []

        combined_data = []

        for file in files:
            if "unit_conversions" in file.lower():
                try:
                    combined_data = UnitConversionTable.from_file(file_path=file)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

        # Converts Pydantic model to data frame with propper column names
        # pd.DataFrame([model.dict() for model in ib_table.mappings])
        return combined_data
