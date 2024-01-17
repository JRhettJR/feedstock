from enum import Enum
from typing import List, Type

from data_aggregators.files import (
    ClimateFieldViewFile,
    CoverCropTable,
    DataFile,
    DataTemplateFile,
    FarmMobileFile,
    FieldNameMapping,
    GranularFile,
    InputBreakdownTable,
    MappingFile,
    MyJohnDeereFile,
    ProductNameMapping,
    UnitConversionTable,
    UnitNameMapping,
)


class DataAggregators(Enum):
    DA_CFV = "CFV"
    DA_JDOPS = "JDOps"
    DA_GRANULAR = "Granular"
    DA_FARMMOBILE = "FM"
    DA_Template = "data_template"


class AggregatorOperationFactory:
    @staticmethod
    def get_file_class(aggregator: DataAggregators) -> Type[DataFile]:
        if aggregator.value == DataAggregators.DA_CFV.value:
            return ClimateFieldViewFile
        elif aggregator.value == DataAggregators.DA_JDOPS.value:
            return MyJohnDeereFile
        elif aggregator.value == DataAggregators.DA_GRANULAR.value:
            return GranularFile
        elif aggregator.value == DataAggregators.DA_FARMMOBILE.value:
            return FarmMobileFile
        elif aggregator.value == DataAggregators.DA_Template.value:
            return DataTemplateFile
        else:
            raise ValueError(f"Unknown data aggregator: {aggregator}")

    @staticmethod
    def process_files_in_gcs_folder(
        aggregator: DataAggregators, bucket_name: str, folder_path: str
    ) -> List:
        file_class = AggregatorOperationFactory.get_file_class(aggregator)
        return file_class.process_files_in_gcs_folder(bucket_name, folder_path)

    @staticmethod
    def process_file_by_type_in_gcs(
        aggregator: DataAggregators, bucket_name: str, folder_path: str, file_type: str
    ) -> List:
        file_class = AggregatorOperationFactory.get_file_class(aggregator)
        return file_class.process_file_by_type_in_gcs(
            bucket_name, folder_path, file_type
        )


class MappingFiles(Enum):
    InputBreakdown = "input_breakdown_table"
    FieldNameMapping = "field_name_mapping"
    ProductNameMapping = "product_name_mapping"
    CoverCropTable = "cover_crop_table"
    UnitNameMapping = "unit_mapping_table"
    UnitConversionTable = "unit_conversion_table"


class MappingFileFactory:
    @staticmethod
    def get_mapping_class(mapping: MappingFiles) -> Type[MappingFile]:
        if mapping.value == MappingFiles.InputBreakdown.value:
            return InputBreakdownTable
        elif mapping.value == MappingFiles.FieldNameMapping.value:
            return FieldNameMapping
        elif mapping.value == MappingFiles.ProductNameMapping.value:
            return ProductNameMapping
        elif mapping.value == MappingFiles.CoverCropTable.value:
            return CoverCropTable
        elif mapping.value == MappingFiles.UnitNameMapping.value:
            return UnitNameMapping
        elif mapping.value == MappingFiles.UnitConversionTable.value:
            return UnitConversionTable
        else:
            raise ValueError(f"Unknown mapping file: {mapping}")

    @staticmethod
    def process_file_in_gcs_folder(
        mapping: MappingFile, bucket_name: str, folder_path: str
    ) -> List:
        file_class = MappingFileFactory.get_mapping_class(mapping)
        return file_class.process_file_in_gcs_folder(bucket_name, folder_path)
