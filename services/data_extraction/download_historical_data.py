import os
from datetime import date, timedelta
from typing import Generator

import polars as pl
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


def extract_and_store_historical_data(trace_id: str, start_date: date, end_date: date) -> APIResponse:
    """
    Orchestrates the ETL process: fetches organization units, chunks them,
    extracts historical data from KHIS, and incrementally loads it into the database.

    Optimization Strategy:
        - Uses 'Iterative Saving': Data is written to the DB immediately after
          fetching each chunk. This acts as a checkpoint system.
        - Uses Generators: Avoids realizing the full list of chunks in memory.

    Args:
        trace_id: The id of the request for debugging purposes.
        start_date (str, optional): The start date to fetch data from.
        end_date (str, optional): The end date to fetch data from.

    Returns:
        int: The total number of rows successfully processed and saved.
    """

    # Retrieve env files from config file
    base_url = config.DHIS2_BASE_URL
    username = config.DHIS2_USERNAME
    password = config.DHIS2_PASSWORD
    db_connection_uri = config.FP_DB_URL

    # Convert date to expected string types
    start_date = first_day_of_month(start_date)
    end_date = first_day_of_month(end_date)

    # --- Helper: Generator for slicing DataFrame ---
    def iter_df_chunks(df: pl.DataFrame, size: int) -> Generator[pl.DataFrame, None, None]:
        """Yields slices of the DataFrame to avoid creating a massive list in memory."""
        for i in range(0, df.height, size):
            yield df.slice(i, size)

    # --- 1. Preparation ---
    logger.info(f"Starting historical data extraction pipeline for period {start_date} to {end_date}")

    # Fetch Metadata
    org_units_df = get_org_units_ids(db_connection_uri)
    data_elements_map = get_fp_data_elements_ids(db_connection_uri)

    # Extract the specific ID list needed
    ids_df: pl.DataFrame = data_elements_map.get("combined_consumption_service_ids")
    data_element_list = ids_df.select("id").to_series().to_list()

    facility_chunk_size: int = 400
    total_chunks = (org_units_df.height // facility_chunk_size) + 1
    total_rows_processed = 0

    # --- 2. Processing Loop ---
    # We iterate directly over the generator, saving memory
    for index, chunk_df in enumerate(iter_df_chunks(org_units_df, facility_chunk_size)):
        try:
            current_batch_num = index + 1
            facility_ids = chunk_df.select("facility_id").to_series().to_list()

            logger.debug(f"Processing Batch {current_batch_num}/{total_chunks} "
                         f"({len(facility_ids)} facilities)")

            # Call extraction function
            # Tip: Make dates dynamic or arguments to make the function reusable
            batch_data = extract_historical_data_from_khis(
                base_url=base_url,
                username=username,
                password=password,
                org_unit_ids=facility_ids,
                data_element_ids=data_element_list,
                start_date=start_date,
                end_date=end_date,
            )

            # --- 3. Incremental Save (Optimization) ---
            # If data exists, save immediately. If the script crashes on chunk 40,
            # chunks 1-39 are already safely in the DB.
            if batch_data.height > 0:
                # Identify Periods in this specific batch
                # Convert date objects to string since our DB expects strings,
                unique_periods = (
                    batch_data
                    .select("period")
                    .unique()
                    .to_series()
                    .cast(pl.String)
                    .to_list()
                )

                # Clean Old Data (Delete-Then-Write)
                # We delete ONLY the periods found in this incoming batch.
                delete_existing_data_for_periods(
                    periods=unique_periods,
                    connection_uri=db_connection_uri,
                    table_name="raw_fp_data",
                )

                # Save new data
                save_df_to_db(
                    df=batch_data,
                    db_url=db_connection_uri,
                    table_name="raw_fp_data",
                    if_table_exists="append"  # Always append for iterative/chunked inserts
                )
                total_rows_processed += batch_data.height
                logger.info(f"Batch {current_batch_num} saved: {batch_data.height} rows.")
            else:
                logger.warning(f"Batch {current_batch_num} returned no data.")

        except Exception as e:
            # Catch errors per chunk so one bad batch doesn't crash the whole pipeline
            logger.exception(f"Failed to process batch {index + 1}: {e}")
            # Optional: continue to next chunk or raise e depending on strictness required
            return APIResponse(
                success=False,
                message=(
                    f"Failed to process batch {index + 1}: "
                    f"Cause of error: {str(e)}"
                ),
                data=None,
                trace_id=trace_id,
            )

    return APIResponse(
        success=True,
        message=f"Successfully extracted and processed {total_rows_processed} rows.",
        data=None,
        trace_id=trace_id,
    )


if __name__ == "__main__":
    load_dotenv()

    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")
    FP_DB_URL = os.getenv("FP_DB_URL")

    output_df = extract_and_store_historical_data(
        trace_id="test_trace_id",
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 2),
    )



