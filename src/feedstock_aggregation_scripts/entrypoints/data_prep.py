from loguru import logger as log

from ..config import settings
from ..data_prep.agg_report.helpers import identify_unmapped_verified_fields
from ..data_prep.cleaned_file.cleaned_file import create_clean_file
from ..data_prep.combine_reports import get_filtered_combined_report
from ..data_prep.complete import (
    add_fuzzy_product_match,
    create_agg_report,
    create_field_list,
    create_harvest_date_file,
    create_lime_report,
    create_split_field_report,
)
from ..data_prep.constants import (
    CC_REPORT,
    DA_GRANULAR,
    DA_LDB,
    MANURE_REPORT,
    REFERENCE_ACREAGE_REPORT,
)
from ..data_prep.cover_crop.cover_crop import create_cc_report
from ..data_prep.extract_file_types.granular import (
    create_Granular_planting_file,
    create_Granular_tillage_file,
)
from ..data_prep.extract_file_types.land_db import (
    create_LDB_fuel_file,
    create_LDB_planting_file,
    create_LDB_tillage_file,
)
from ..data_prep.grower_data_agg_mapping import grower_da_mapping
from ..data_prep.helpers import get_seeding_area
from ..data_prep.manure.manure import create_manure_report
from ..data_prep.reference_acreage.reference_acreage import (
    create_reference_acreage_report,
)

# warnings.simplefilter("ignore")


path_to_data = settings.data_prep.source_path
path_to_dest = settings.data_prep.dest_path

# Read in helper files
# unit_cleaner = pd.read_csv(
#     Path(path_to_lookup_tables).joinpath("unit_mapping_table.csv")
# )  # needed within data_prep_complete.py
# qu_converter = pd.read_csv(
#     Path(path_to_lookup_tables).joinpath("unit_conversions.csv")
# )  # needed within data_prep_complete.py
# params = pd.read_csv(path_to_data.joinpath('data_prep_params.csv')) # mapping structure for loop

# set global params
growing_cycle = int(2022)
partial_match_ratio = 86
verbose = False


def run_test():
    for grower in grower_da_mapping:
        log.info(grower)

        try:
            log.info("create_cc_report")
            create_cc_report(
                path_to_data,
                path_to_dest,
                grower,
                growing_cycle,
                data_aggregator=DA_GRANULAR,
            )
        except Exception as e:
            log.exception(str(e))


# grouped by grower, feed each data aggregator into function lists


@log.catch
def run():
    for grower in grower_da_mapping:
        log.info(f"Grower: {grower}")
        for data_aggregator in grower_da_mapping[grower]:
            log.info(data_aggregator)
            if data_aggregator == "FE":
                continue

            # Creating additional files by extracting info out of available files.
            # This currently needs to be done for the following `data_aggregator`s:
            # - Granular
            # - Land.db
            # - SMS Ag Leader (not yet implemented as it's not prioritised)
            #
            # Extracting this data will ensure that all subsequent function calls
            # to generate reports will be provided the same data input file type
            # structure:
            # - application
            # - harvest
            # - planting
            # - tillage
            # - fuel
            #
            # The cleaning method internally ensures that no double counting occurs.
            #
            # IMPORTANT: the files `field_name_mapping.csv` and
            # `chemical_input_products_mapping_table.csv` needs to be up-to-date and
            # provided to successfully create the file extractions. Otherwise there
            # will be matching issues using those extracted files.
            #
            # Proposed TO-DO:
            # The mappings for `Field_name` and `Product` could be refactored into
            # a different place instead to avoid the dependencies.

            if data_aggregator == DA_GRANULAR:
                log.info("extract_files_from_existing_GRANULAR")
                try:
                    # extract PLANTING data into separate file
                    create_Granular_planting_file(
                        path_to_data, grower, growing_cycle - 1
                    )
                    create_Granular_planting_file(path_to_data, grower, growing_cycle)
                    # extract TILLAGE data into separate file
                    create_Granular_tillage_file(
                        path_to_data, grower, growing_cycle - 1
                    )
                    create_Granular_tillage_file(path_to_data, grower, growing_cycle)
                except Exception as e:
                    log.exception(str(e))

            if data_aggregator == DA_LDB:
                log.info("extract_files_from_existing_LDB")
                try:
                    # extract PLANTING data into separate file
                    create_LDB_planting_file(path_to_data, grower, growing_cycle)
                    # extract TILLAGE data into separate file
                    create_LDB_tillage_file(path_to_data, grower, growing_cycle)
                    # extract FUEL data into separate file
                    create_LDB_fuel_file(path_to_data, grower, growing_cycle)
                except Exception as e:
                    log.exception(str(e))

            try:
                log.info("create_field_list")
                create_field_list(
                    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_reference_acreage_report")
                create_reference_acreage_report(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    verbose,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("add_fuzzy_product_match")
                add_fuzzy_product_match(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    partial_match_ratio,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_harvest_date_file")
                create_harvest_date_file(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    verbose,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_cc_report")
                create_cc_report(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    verbose,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_manure_report")
                create_manure_report(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    verbose,
                )
            except Exception as e:
                log.info(str(e))

            try:
                log.info("create_lime_report")
                create_lime_report(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    verbose,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_split_field_report")
                create_split_field_report(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    verbose,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_clean_file")
                create_clean_file(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                    verbose,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("identify_unmapped_fields")
                identify_unmapped_verified_fields(
                    path_to_data,
                    path_to_dest,
                    grower,
                    growing_cycle,
                    data_aggregator,
                )
            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_seed_area")
                get_seeding_area(
                    path_to_data,
                    grower,
                    growing_cycle,
                    data_aggregator,
                )

            except Exception as e:
                log.exception(str(e))

            try:
                log.info("create_agg_report")
                create_agg_report(
                    path_to_data, path_to_dest, grower, growing_cycle, data_aggregator
                )
            except Exception as e:
                log.exception(str(e))

        # Combine reports after all is generated.
        # These combined reports are required for the
        # CI_prep flow.
        #
        # This will generate 2 files:
        # 1. combined report
        # 2. combined report filtered for available
        #    `Reference_acreage`
        #
        # The second report is intended as a reference,
        # the first report should be used to base our
        # decisions on.
        try:
            log.info("combine_reference_acreage")
            get_filtered_combined_report(
                report_type=REFERENCE_ACREAGE_REPORT,
                path_to_processed=path_to_dest,
                grower=grower,
                growing_cycle=growing_cycle,
            )
        except Exception as e:
            log.exception(str(e))

        try:
            log.info("combine_cover_crop")
            get_filtered_combined_report(
                report_type=CC_REPORT,
                path_to_processed=path_to_dest,
                grower=grower,
                growing_cycle=growing_cycle,
            )
        except Exception as e:
            log.exception(str(e))

        try:
            log.info("combine_manure")
            get_filtered_combined_report(
                report_type=MANURE_REPORT,
                path_to_processed=path_to_dest,
                grower=grower,
                growing_cycle=growing_cycle,
            )
        except Exception as e:
            log.exception(str(e))

        log.info("--------------" * 8)
