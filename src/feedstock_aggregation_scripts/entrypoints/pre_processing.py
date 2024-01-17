from loguru import logger as log

from ..config import settings
from ..pre_processing.sms.sms_ag_prep import (
    create_SMS_application_file,
    create_SMS_harvest_file,
    create_SMS_planting_file,
)


@log.catch
def run():
    log.info("Start pre-processing...")
    # Currently this only applies to 1 grower, i.e. Osvog
    grower = "Osvog"
    growing_cycle = 2022
    path_to_data = settings.data_prep.source_path

    log.info(f"Grower: {grower}")

    apps = create_SMS_application_file(
        path_to_data=path_to_data, grower=grower, growing_cycle=growing_cycle
    )
    harvest = create_SMS_harvest_file(
        path_to_data=path_to_data, grower=grower, growing_cycle=growing_cycle
    )
    plant = create_SMS_planting_file(
        path_to_data=path_to_data, grower=grower, growing_cycle=growing_cycle
    )

    return apps, harvest, plant
