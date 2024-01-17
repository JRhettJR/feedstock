from src.feedstock_aggregation_scripts.entrypoints import (
    bulk_to_excel,
    ci_prep,
    data_prep,
    pre_processing,
)

SQUARE_METER_TO_ACRE = 0.000247105
NOT_FOUND = False

# path_to_data = settings.data_prep.source_path

# data_prep.run_test()
pre_processing.run()
data_prep.run()
ci_prep.run()
bulk_to_excel.run()

# shp_file_overview.run()
