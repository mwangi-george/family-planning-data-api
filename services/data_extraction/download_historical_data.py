import os
from datetime import date
from typing import Generator

import polars as pl
from fastapi import BackgroundTasks
from loguru import logger
from dotenv import load_dotenv

from core.env_config import env_config as config
from schemas.shared import APIResponse
from services.helpers import (
    get_org_units_ids,
    get_fp_data_elements_ids,
    extract_historical_data_from_khis,
    save_df_to_db,
    delete_existing_data_for_periods,
    first_day_of_month
)


# --- New Helper Function ---
def generate_period_strings(start: date, end: date) -> list[str]:
    """
    Generates a list of date strings (first day of each month) between start and end.
    """
    periods = []
    while start <= end:
        # Assuming your DB stores periods as 'YYYY-MM-DD'.
        # If DHIS2 returns '202310', change this line to: current.strftime("%Y%m")
        periods.append(start)

        # Increment to next month
        if start.month == 12:
            start = date(start.year + 1, 1, 1)
        else:
            start = date(start.year, start.month + 1, 1)
    #
    logger.debug(f"Periods: {periods}")
    #
    # # normalize dates
    periods = [first_day_of_month(item) for item in periods if periods]
    return periods


def extract_and_store_historical_data(trace_id: str, start_date: date, end_date: date) -> APIResponse:
    # Retrieve env files
    base_url = config.DHIS2_BASE_URL
    username = config.DHIS2_USERNAME
    password = config.DHIS2_PASSWORD
    db_connection_uri = config.FP_DB_URL

    # Normalize dates
    start_date_str = first_day_of_month(start_date)
    end_date_str = first_day_of_month(end_date)

    def iter_df_chunks(df: pl.DataFrame, size: int) -> Generator[pl.DataFrame, None, None]:
        for i in range(0, df.height, size):
            yield df.slice(i, size)

    # --- 1. Preparation ---
    logger.info(f"Starting historical data extraction pipeline for period {start_date_str} to {end_date_str}")

    # Fetch Metadata
    org_units_df = get_org_units_ids(db_connection_uri)
    data_elements_map = get_fp_data_elements_ids(db_connection_uri)

    ids_df: pl.DataFrame = data_elements_map.get("combined_consumption_service_ids")
    data_element_list = ids_df.select("id").to_series().to_list()

    facility_chunk_size: int = 400
    total_chunks = (org_units_df.height // facility_chunk_size) + 1
    total_rows_processed = 0

    # --- FIX START: Pre-emptive Cleanup ---
    # We calculate the periods involved and clean the DB ONCE before processing chunks.
    logger.info("Performing pre-computation cleanup...")
    target_periods = generate_period_strings(start_date, end_date)

    try:
        if target_periods:
            delete_existing_data_for_periods(
                periods=target_periods,
                connection_uri=db_connection_uri,
                table_name="raw_fp_data",
            )
    except Exception as e:
        # If cleanup fails, do not proceed to extraction
        logger.exception(f"Critical Error: Failed to clean existing data. Aborting. Error: {e}")
        return APIResponse(
            success=False,
            message=f"Pipeline aborted during cleanup: {str(e)}",
            data=None,
            trace_id=trace_id
        )
    # --- FIX END ---

    # --- 2. Processing Loop ---
    for index, chunk_df in enumerate(iter_df_chunks(org_units_df, facility_chunk_size)):
        try:
            current_batch_num = index + 1
            facility_ids = chunk_df.select("facility_id").to_series().to_list()

            logger.debug(f"Processing Batch {current_batch_num}/{total_chunks} "
                         f"({len(facility_ids)} facilities)")

            batch_data = extract_historical_data_from_khis(
                base_url=base_url,
                username=username,
                password=password,
                org_unit_ids=facility_ids,
                data_element_ids=data_element_list,
                start_date=start_date_str,
                end_date=end_date_str,
            )

            # --- 3. Incremental Save ---
            if batch_data.height > 0:

                # We simply APPEND. The cleanup was handled in Step 1.
                save_df_to_db(
                    df=batch_data,
                    db_url=db_connection_uri,
                    table_name="raw_fp_data",
                    if_table_exists="append"
                )
                total_rows_processed += batch_data.height
                logger.info(f"Batch {current_batch_num} saved: {batch_data.height} rows.")
            else:
                logger.warning(f"Batch {current_batch_num} returned no data.")

        except Exception as e:
            logger.exception(f"Failed to process batch {index + 1}: {e}")
            return APIResponse(
                success=False,
                message=f"Failed to process batch {index + 1}: {str(e)}",
                data=None,
                trace_id=trace_id,
            )

    logger.info(f"Total rows processed: {total_rows_processed}")
    return APIResponse(
        success=True,
        message=f"Successfully extracted and processed {total_rows_processed} rows.",
        data=None,
        trace_id=trace_id,
    )


async def extract_and_store_historical_data_in_bg(
        trace_id: str,
        start_date: date,
        end_date: date,
        bg_tasks: BackgroundTasks
) -> APIResponse:
    bg_tasks.add_task(extract_and_store_historical_data, trace_id, start_date, end_date)
    return APIResponse(
        success=True,
        message=f"Task accepted. Processing in progress.",
        data=None,
        trace_id=trace_id,
    )


if __name__ == "__main__":

    load_dotenv()

    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")
    FP_DB_URL = os.getenv("FP_DB_URL")

    # test the function
    output_df = extract_and_store_historical_data(
        trace_id="test_trace_id",
        start_date=date(2020, 1, 1),
        end_date=date(2020, 2, 1),
    )



