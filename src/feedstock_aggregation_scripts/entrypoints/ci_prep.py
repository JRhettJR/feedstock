import os
import pathlib

import pandas as pd
from loguru import logger as log

from ..ci_prep.attestation_overwrite import attestation_overwrite
from ..ci_prep.prepare_bulk_upload_template import (
    create_bulk_upload,
    create_decision_matrix,
    prepare_overview_for_bulk_mapping,
)
from ..config import settings
from ..data_prep.grower_data_agg_mapping import grower_da_mapping
from ..general import read_verified_file


@log.catch
def run():
    path_to_data = settings.data_prep.source_path
    path_to_dest = settings.data_prep.dest_path
    path_to_processed = path_to_dest
    growing_cycle = int(2022)
    grower = None  # "Noem"
    growers_to_process = [grower] if grower else grower_da_mapping
    results = pd.DataFrame()

    if len(growers_to_process) == 0:
        log.error("No grower selected")
        return

    for grower in growers_to_process:
        log.info(f"Processing grower: {grower}")

        overview = prepare_overview_for_bulk_mapping(
            path_to_data,
            path_to_processed,
            path_to_dest,
            grower,
            growing_cycle,
        )

        decisions = create_decision_matrix(
            overview, path_to_data, path_to_processed, grower, growing_cycle
        )

        grower_path_to_dest = pathlib.Path(path_to_dest).joinpath(grower)
        if not os.path.exists(grower_path_to_dest):
            os.makedirs(grower_path_to_dest)

        if decisions.empty:
            log.warning(
                f"No data to save for {grower}_field_practice_decision_matrix_{growing_cycle}.csv"
            )
        else:
            decisions.to_csv(
                grower_path_to_dest.joinpath(
                    grower
                    + "_field_practice_decision_matrix_"
                    + str(growing_cycle)
                    + ".csv"
                ),
                index=False,
            )
        if overview.empty:
            log.warning(
                f"No data to save for {grower}_comprehensive_inputs_{growing_cycle}.csv"
            )
        else:
            overview.to_csv(
                grower_path_to_dest.joinpath(
                    grower + "_comprehensive_inputs_" + str(growing_cycle) + ".csv"
                ),
                index=False,
            )

        # verified_fields_df = pd.read_csv(
        #     f"{path_to_data}/{grower}/{str(growing_cycle)}_{grower}_verified_acres.csv"
        # )
        verified_fields_df = read_verified_file(path_to_data, grower)
        bulk, exclusions = create_bulk_upload(overview, decisions, verified_fields_df)

        if bulk.empty:
            log.warning(
                f"No data to save for {grower}_bulk_upload_template_{growing_cycle}.csv"
            )
        else:
            bulk.to_csv(
                grower_path_to_dest.joinpath(
                    grower + "_bulk_upload_template_" + str(growing_cycle) + ".csv"
                ),
                index=False,
            )

        # Overwrite with attestation data
        bulk_attest, exclusions = attestation_overwrite(
            bulk, grower, growing_cycle, exclusions
        )

        if bulk_attest.empty:
            log.warning(
                f"No data to save for {grower}_bulk_upload_template_{growing_cycle}.csv"
            )
        else:
            bulk_attest.to_csv(
                grower_path_to_dest.joinpath(
                    grower
                    + "_bulk_upload_template_"
                    + str(growing_cycle)
                    + "_WITH_ATTEST.csv"
                ),
                index=False,
            )

        verified_fields_df["Processed"] = verified_fields_df["Field_name"].isin(
            bulk["FIELD_NAME"]
        )
        results = pd.concat([results, verified_fields_df], ignore_index=True)

        if exclusions.empty:
            log.info(
                f"No data to save for {grower}_field_exclusions_{growing_cycle}.csv"
            )
        else:
            exclusions.to_csv(
                grower_path_to_dest.joinpath(
                    grower + "_field_exclusions_" + str(growing_cycle) + ".csv"
                ),
                index=False,
            )

        print()  # visual deliniator between growers

    results = results[results["Verified"].notnull()].reset_index()
    # Quick insight to fields that we expected to be processed by failed
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option(
        "display.max_colwidth", None
    )  # Set the maximum column width to unlimited
    pd.set_option("display.expand_frame_repr", False)  # Prevent line breaks within rows
    print("**** Results of Verified Fields vs Fields in Bulk ****")
    print(
        results[["Farm_entity", "Field_name", "Processed"]].sort_values(
            ["Farm_entity", "Field_name"]
        )
    )
    print("\n**** Summary of Processed Verified Fields by Entity ****")
    summary = (
        results.groupby("Farm_entity")["Processed"].value_counts().unstack(fill_value=0)
    )
    summary["Total Processed"] = summary.get(True, 0)
    summary["Total Not Processed"] = summary.get(False, 0)
    print(summary[["Total Processed", "Total Not Processed"]])

    print("\n**** Summary of Excluded Fields by Case ****")
    print(exclusions[["Case", "Excluded", "Crop_type", "Reason"]].fillna(""))
    pd.reset_option("display.max_rows")
    pd.reset_option("display.max_columns")
    pd.reset_option("display.max_colwidth")
    pd.reset_option("display.expand_frame_repr")
