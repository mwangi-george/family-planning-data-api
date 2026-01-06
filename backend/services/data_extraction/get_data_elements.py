import os
import polars as pl
from typing import Optional
from dotenv import load_dotenv
from loguru import logger

from backend.services.helpers import make_api_call


def get_data_elements(base_url: str, username: str, password: str) -> Optional[pl.DataFrame]:
    """
    Fetch data elements from DHIS2 and return it as a Polars DataFrame.

    Parameters
    ---------
    base_url : str
        Base URL of the DHIS2 instance (e.g., "https://play.dhis2.org/dev").
    username : str
        DHIS2 username.
    password : str
        DHIS2 password.

    Returns
    -------
    Optional[pl.DataFrame]
        A Polars DataFrame containing the extracted and cleaned data elements,
        or None if processing fails.

    Raises
    ------
    RuntimeError
        If the HTTP request fails or the DHIS2 API returns a non-200 status code.
    """
    url = f"{base_url}/api/dataElements?fields=name,id,shortName,displayName&paging=false"

    logger.info(f"Requesting data elements from DHIS2: {url}")

    # API request
    response = make_api_call(url, username, password)

    logger.success("Data elements successfully retrieved from DHIS2.")

    # Extract the JSON payload
    try:
        json_data: dict = response.json()
    except ValueError:
        raise RuntimeError("DHIS2 returned an invalid JSON response.")

    data_elements = json_data.get("dataElements", [])

    if not isinstance(data_elements, list):
        raise RuntimeError("Unexpected JSON structure: 'dataElements' is not a list.")

    try:
        # Build Polars DataFrame
        df = pl.DataFrame(data_elements)

        # Rename camelCase → snake_case
        df = df.rename({
            "shortName": "short_name",
            "displayName": "display_name",
        })

        logger.info(f"Data elements DataFrame created with {df.shape[0]} rows.")
        return df

    except Exception as e:
        logger.exception(f"Error processing data elements from DHIS2: {e}")
        return None


if __name__ == "__main__":
    load_dotenv()

    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")

    df = get_data_elements(DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD)

    if df is not None:
        logger.success("Data elements loaded and processed successfully.")
        print(df.head())
