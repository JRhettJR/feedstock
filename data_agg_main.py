import os
import pathlib

from data_aggregators.factory import (
    AggregatorOperationFactory,
    DataAggregators,
    MappingFileFactory,
    MappingFiles,
)

from src.feedstock_aggregation_scripts.config import settings

os.environ["GOOGLE_CLOUD_PROJECT"] = "ds-drive-connection"
bucket_name = "ds-feedstock-aggregate-storage"

# folder_path = "01_data/Rieck"
# combined_climate_field_view = AggregatorOperationFactory.process_files_in_gcs_folder(
#     DataAggregators.DA_CFV, bucket_name, folder_path
# )
# print(combined_climate_field_view)

# folder_path = "01_data/Feikema"
# combined_granular = AggregatorOperationFactory.process_files_in_gcs_folder(
#     DataAggregators.DA_GRANULAR, bucket_name, folder_path
# )
# print(combined_granular)

folder_path = "01_data/Feikema"
combined_jdops = AggregatorOperationFactory.process_files_in_gcs_folder(
    DataAggregators.DA_JDOPS, bucket_name, folder_path
)
print(combined_jdops)

folder_path = "01_data/TEST_GROWER"
combined_template_data = AggregatorOperationFactory.process_files_in_gcs_folder(
    DataAggregators.DA_Template, bucket_name, folder_path
)
print(combined_template_data)

folder_path = "01_data"
input_breakdowns = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.InputBreakdown, bucket_name, folder_path
)
print(input_breakdowns)

folder_path = "01_data/TEST_GROWER"
combined_template_data = AggregatorOperationFactory.process_files_in_gcs_folder(
    DataAggregators.DA_Template, bucket_name, folder_path
)
print(combined_template_data)

folder_path = "01_data"
input_breakdowns = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.InputBreakdown, bucket_name, folder_path
)
print(input_breakdowns)

folder_path = "01_data/Feikema"
fnm = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.FieldNameMapping, bucket_name, folder_path
)
print(fnm)
print(fnm.query_entry(field_name="Vissers 146", farm_name="Circle F Farms"))
print(fnm.query_entry(field_name="Vissers 146", farm_name=None))
print(fnm.query_entry(field_name="Vissers 14", farm_name="Circle F Farms"))

folder_path = "01_data"
pnm = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.ProductNameMapping, bucket_name, folder_path
)
print(pnm)
print(pnm.query_entry(product_name="0-0-60 "))

folder_path = "01_data"
cct = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.CoverCropTable, bucket_name, folder_path
)
print(cct)
print(cct.query_entry(cover_crop_name="Rye"))

folder_path = "01_data"
unm = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.UnitNameMapping, bucket_name, folder_path
)
print(unm)
print(unm.query_entry(unit_name="floz").clear_unit)

folder_path = "01_data"
uct = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.UnitConversionTable, bucket_name, folder_path
)
print(uct)
print(uct.query_entry("fl_oz"))


""" Example using config variables """

os.environ["GOOGLE_CLOUD_PROJECT"] = settings.gcs_dev.project_id
bucket_name = settings.gcs_dev.bucket_name

grower_name = "Feikema"
folder_path = pathlib.Path(settings.bucket_folders.mapping_data).joinpath(grower_name)

fnm = MappingFileFactory.process_file_in_gcs_folder(
    MappingFiles.FieldNameMapping, bucket_name, folder_path
)
print(fnm)
