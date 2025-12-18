import time

from fastapi import BackgroundTasks
from loguru import logger

from core.env_config import env_config
from services.data_extraction.get_indicators import get_indicators
from services.data_extraction.get_data_elements import get_data_elements
from services.data_extraction.get_organisation_units import get_organisation_units
from services.helpers import save_df_to_db
from schemas.shared import APIResponse


def extract_and_store_dhis2_metadata(trace_id: str) -> APIResponse:
    """
    Extract DHIS2 metadata objects (organisation units, data elements, and indicators)
    and save them into a relational database.

    This function:
    1. Loads required environment variables.
    2. Downloads metadata from DHIS2 using API helper functions.
    3. Measures the total extraction time.
    4. Stores each metadata DataFrame into the database using `save_df_to_db()`.

    Logs
    ----
    - Extraction runtime
    - Success messages for each dataset stored
    - Errors if any step fails
    """
    try:
        # Retrieve env files from config file
        DHIS2_BASE_URL = env_config.DHIS2_BASE_URL
        DHIS2_USERNAME = env_config.DHIS2_USERNAME
        DHIS2_PASSWORD = env_config.DHIS2_PASSWORD
        FP_DB_URL = env_config.FP_DB_URL


        logger.info("Starting DHIS2 metadata extraction...")

        start_time = time.time()

        # --- Download metadata from DHIS2 ---
        logger.info("Downloading organisation units...")
        organisation_units_df = get_organisation_units(DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD)

        logger.info("Downloading data elements...")
        data_elements_df = get_data_elements(DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD)

        logger.info("Downloading indicators...")
        indicators_df = get_indicators(DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD)

        # --- Measure extraction time ---
        elapsed = time.time() - start_time
        logger.info(f"Metadata extraction completed in {elapsed:.2f} seconds.")

        # --- Store metadata into DB ---
        logger.info("Saving organisation units to database...")
        save_df_to_db(organisation_units_df, FP_DB_URL, "organisation_units", "replace")

        logger.info("Saving data elements to database...")
        save_df_to_db(data_elements_df, FP_DB_URL, "data_elements", "replace")

        logger.info("Saving indicators to database...")
        save_df_to_db(indicators_df, FP_DB_URL, "indicators", "replace")

        msg = "All metadata successfully saved to the database."
        logger.success(msg)
        return APIResponse(success=True, message=msg, trace_id=trace_id, data=None)

    except Exception as e:
        logger.exception(f"Failed during metadata extraction or storage: {e}")
        return APIResponse(
            success=False,
            message="Metadata extraction or storage failed. Check your logs",
            trace_id=trace_id,
            data=None,
        )


async def extract_and_store_dhis2_metadata_in_bg(trace_id: str, bg_tasks: BackgroundTasks) -> APIResponse:
    bg_tasks.add_task(extract_and_store_dhis2_metadata, trace_id)

    return APIResponse(
        success=True,
        message="Task accepted. Processing in progress.",
        data=None,
        trace_id=trace_id,
    )


if __name__ == "__main__":
    extract_and_store_dhis2_metadata(trace_id="test_trace_id")
