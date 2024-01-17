from loguru import logger as log

from ..bulk_to_excel.bulk_to_excel import prepare_for_excel_workbook
from ..config import settings
from ..data_prep.grower_data_agg_mapping import grower_da_mapping
from ..util.readers.generated_reports import read_bulk_upload_template


def run():
    path_to_processed = settings.data_prep.dest_path
    # grower = "Aughenbaugh"
    growing_cycle = 2022

    for grower in grower_da_mapping:
        log.info(f"Processing grower {grower}")
        bulk = read_bulk_upload_template(path_to_processed, grower, growing_cycle)
        if bulk.empty:
            continue

        bulk = prepare_for_excel_workbook(bulk)

        if bulk.empty:
            log.warning(
                f"nothing to save for {path_to_processed}/{grower}/{grower}_bulk_to_excel_{growing_cycle}.csv"
            )

        else:
            # save transposed table for easier copying
            bulk.transpose().to_csv(
                f"{path_to_processed}/{grower}/{grower}_bulk_to_excel_{growing_cycle}.csv"
            )
