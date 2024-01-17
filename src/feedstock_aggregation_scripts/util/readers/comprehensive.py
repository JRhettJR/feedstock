import pathlib

import pandas as pd
from loguru import logger as log

from ...data_prep.constants import (
    CFV_FILE_TYPES,
    DA_CFV,
    DA_FM,
    DA_GRANULAR,
    DA_JDOPS,
    DA_LDB,
    DA_PAP,
    DA_SMS,
    FM_FILE_TYPES,
    GRAN_FILE_TYPES,
    GRAN_GENERATED,
    JD_FILE_TYPES,
    LDB_FILE_TYPES,
    LDB_GENERATED,
    PAP_FILE_TYPES,
    SMS_FILE_TYPES,
)
from ..cleaners.general import get_cleaned_file_by_file_type

# %% [markdown]
# ---
# # Comprehensive data frame


def create_comprehensive_df(
    path_to_data: str | pathlib.Path,
    grower: str,
    growing_cycle: int,
    data_aggregator: str,
    verbose: bool = True,
) -> pd.DataFrame:
    avail_dfs = []
    pd.DataFrame()

    if data_aggregator == DA_JDOPS:
        file_types = JD_FILE_TYPES

    elif data_aggregator == DA_GRANULAR:
        file_types = [*GRAN_FILE_TYPES, *GRAN_GENERATED]

    elif data_aggregator == DA_CFV:
        file_types = CFV_FILE_TYPES

    elif data_aggregator == DA_PAP:
        file_types = PAP_FILE_TYPES

    elif data_aggregator == DA_LDB:
        file_types = [*LDB_FILE_TYPES, *LDB_GENERATED]

    elif data_aggregator == DA_FM:
        file_types = FM_FILE_TYPES

    elif data_aggregator == DA_SMS:
        file_types = SMS_FILE_TYPES

    else:
        file_types = []

    for file_type in file_types:
        df = get_cleaned_file_by_file_type(
            path_to_data, grower, growing_cycle, data_aggregator, file_type, verbose
        )

        if not df.empty:
            avail_dfs.append(df)
    if len(avail_dfs) == 0:
        if verbose:
            log.warning(
                f"no {data_aggregator} data available to concatenate for grower {grower} in {growing_cycle}"
            )
        return pd.DataFrame()
    # return avail_dfs
    temp = pd.concat(avail_dfs, ignore_index=True)

    return temp
