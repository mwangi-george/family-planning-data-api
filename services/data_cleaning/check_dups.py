import polars as pl
from loguru import logger

from core.env_config import env_config


# ----------------------------------------------------------------------
# Load raw family planning data from the database into a Polars DataFrame
# ----------------------------------------------------------------------
# NOTE:
# - The query loads the entire table; ensure the table size is manageable.
# ----------------------------------------------------------------------
logger.info("Loading raw_fp_data table from database...")

raw_df = pl.read_database_uri(
    query="SELECT * FROM raw_fp_data",
    uri=env_config.FP_DB_URL
)

logger.info(f"Raw dataset loaded successfully. Shape: {raw_df.shape}")


# ----------------------------------------------------------------------
# Identify duplicate rows across ALL columns
# ----------------------------------------------------------------------
duplicated_rows_df = raw_df.filter(raw_df.is_duplicated())

dup_count = len(duplicated_rows_df)
logger.info(f"Duplicate rows identified: {dup_count}")

if dup_count == 0:
    logger.info("No duplicate rows found. Exiting cleanly.")
else:
    logger.info("Duplicate rows detected. Proceeding to summarize affected org units and periods.")

    # ----------------------------------------------------------------------
    # Extract unique (org_unit, period) combinations from the duplicated rows
    # This helps understand which reporting units and periods contain duplicates.
    # ----------------------------------------------------------------------
    results = (
        duplicated_rows_df
            .select(["org_unit", "period"])
            .unique()
            .sort(["org_unit", "period"])
    )

    # Display a preview while keeping logs small
    logger.info("Sample of duplicate org_unit/period combinations:")
    logger.info(results.head())

