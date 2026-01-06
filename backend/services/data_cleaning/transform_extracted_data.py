import gc

import polars as pl
from loguru import logger

# user imports
from backend.core.env_config import env_config
from backend.schemas.shared import APIResponse
from backend.services.helpers import save_df_to_db


class FamilyPlanningDataTransformationPipeline:
    """
    A pipeline to extract, transform, and load Family Planning (FP) data.

    This class handles:
    1. Extraction of raw FP data, organisation units, and data elements from the DB.
    2. Transformation to map IDs to readable names and apply service adjustments.
    3. Aggregation at both County and National levels.
    4. Application of specific business rules (e.g., splitting '2 Rod' data).
    5. Loading the final summary to the database.

    Attributes:
        db_url (str): Database connection string.
    """

    # Configuration: Mapping Analytic IDs to Short Names
    # Note: Flattened for easier lookup (ID -> Name)
    ANALYTIC_ID_MAP = {
        "dl4JcBnxu0X": "POPs", "uHM6lzLXDBd": "POPs",
        "tfPZ6sGgh4q": "Non-Hormonal IUCD", "hRktPfPEegP": "Non-Hormonal IUCD",
        "cV4qoKSYiBs": "Male Condoms", "AVDzuypqGt9": "Male Condoms",
        "APbXNRovb5w": "Levoplant",
        "CJdFYcZ1zOq": "Implanon", "XgJfT71Unkn": "Implanon",
        "MsS41X1GEFr": "Jadelle",
        "zXbxl6y97mi": "Hormonal IUCD", "Wv02gixbRpT": "Hormonal IUCD",
        "Fxb4iVJdw2g": "Female Condoms", "AR7RhdC90IV": "Female Condoms",
        "paDQStynGGD": "EC Pills", "qaBPR9wbWku": "EC Pills",
        "NMCIxSeGpS3": "DMPA-SC", "hXa1xyUMfTa": "DMPA-SC",
        "PgQIx7Hq1kp": "DMPA-IM", "J6qnTev1LXw": "DMPA-IM",
        "fYCo4peO0yE": "Cycle Beads", "bGGT0F7iRxt": "Cycle Beads",
        "BQmcVE8fex4": "COCs", "hH9gmEmEhH4": "COCs",
        "TUHzoPGLM3t": "2 Rod"
    }

    # Configuration: Value Multipliers for Service Data
    SERVICE_ADJUSTMENTS = {
        "COCs": 1.25,
        "POPs": 0.5,
        "Female Condoms": 10.0,
        "Male Condoms": 10.0,
    }

    def __init__(self, request_trace_id: str):
        """ Initialize the pipeline """

        self.db_url = env_config.FP_DB_URL  # The database connection URI
        self.request_trace_id = request_trace_id

    def _extract_data(self) -> dict[str, pl.DataFrame]:
        """
        Extracts raw datasets from the database.

        Returns:
            Dict[str, pl.DataFrame]: A dictionary containing 'raw_fp', 'org_units', and 'elements'.
        """
        logger.info("Starting data extraction...")

        queries = {
            "raw_fp": "SELECT analytic as analytic_id, org_unit, period, value FROM raw_fp_data",
            "org_units": "SELECT facility_id as org_unit, county_name FROM organisation_units",
            "elements": "SELECT id as analytic_id, name as analytic_name FROM data_elements"
        }

        datasets = {}
        for key, query in queries.items():
            df = pl.read_database_uri(uri=self.db_url, query=query)
            logger.info(f"Loaded {key}: shape={df.shape}, cols={df.columns}")
            datasets[key] = df

        return datasets

    @staticmethod
    def _audit_duplicates(df: pl.DataFrame, dataset_name: str = "raw_fp"):
        """
        Checks for duplicate rows across all columns and logs the findings.

        Args:
            df (pl.DataFrame): The dataframe to audit.
            dataset_name (str): Name of the dataset for logging purposes.
        """
        logger.info(f"Auditing {dataset_name} for duplicates...")

        # Check for duplicates across ALL columns
        is_dup = df.is_duplicated()
        dup_count = is_dup.sum()  # count

        if dup_count == 0:
            logger.info(f"No duplicates found in {dataset_name}.")
            return

        logger.warning(f"Found {dup_count} duplicate rows in {dataset_name}!")

        # Summarize affected Org Units and Periods
        # We filter the original DF using the boolean mask 'is_dup'
        summary = (
            df.filter(is_dup)
            .select(["org_unit", "period"])
            .unique()
            .sort(["org_unit", "period"])
        )

        logger.info(f"Sample of affected (org_unit, period) in {dataset_name}:")
        # Convert to string to ensure it logs nicely in the log file
        logger.info(f"\n{summary.head(5)}")

    def _apply_transformations(
            self,
            raw_fp: pl.DataFrame,
            org_units: pl.DataFrame,
            elements: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Joins datasets, maps names, creates 'method' column, and applies value adjustments.
        """
        logger.info("Transforming raw data to county level...")

        # 1. Join Tables
        joined_df = (
            raw_fp
            .join(org_units, on="org_unit", how="inner")
            .join(elements, on="analytic_id", how="inner")
            .drop("org_unit")
        )

        # 2. Map IDs to Short Names efficiently using replace/map
        # We can use a join or map_dict. Since map is small, pl.replace is readable.
        joined_df = joined_df.with_columns(
            analytic_short_name=pl.col("analytic_id")
            .replace_strict(
                self.ANALYTIC_ID_MAP,
                default="Unknown Product"
            )
        )

        # 3. Create 'method' column and adjust values
        processed_df = (
            joined_df
            .with_columns([
                # Determine Method
                pl.when(pl.col("analytic_name").str.contains("711")).then(pl.lit("Service"))
                .when(pl.col("analytic_name").str.contains("747")).then(pl.lit("Consumption"))
                .otherwise(pl.lit("Unknown Method"))
                .alias("method")
            ])
            .with_columns(
                # Apply Business Logic Multipliers
                value=pl.when(pl.col("method") == "Service")
                .then(
                    pl.col("value") * pl.col("analytic_short_name").replace_strict(
                        self.SERVICE_ADJUSTMENTS,
                        default=1.0,
                        return_dtype=pl.Float64
                    )
                )
                .otherwise(pl.col("value"))
            )
        )

        # 4. Final Aggregation (County Level)
        county_df = (
            processed_df
            .rename({"analytic_short_name": "analytic", "county_name": "org_unit"})
            .group_by(["analytic", "method", "org_unit", "period"])
            .agg(pl.col("value").sum())
            .sort(["analytic", "method", "org_unit", "period"])
        )

        logger.info(f"County level transformation complete. Shape: {county_df.shape}")
        return county_df

    @staticmethod
    def _generate_national_aggregates(county_df: pl.DataFrame) -> pl.DataFrame:
        """
        Aggregates county data to create national level records.
        """
        logger.info("Generating national aggregates...")

        national_df = (
            county_df
            .group_by(["analytic", "method", "period"])
            .agg(pl.col("value").sum())
            .with_columns(org_unit=pl.lit("Kenya"))
            .select(["analytic", "method", "org_unit", "period", "value"])
        )

        return national_df

    @staticmethod
    def _process_two_rod_split(df: pl.DataFrame, jadelle_ratio: float = 0.8) -> pl.DataFrame:
        """
        Splits '2 Rod' service data into 'Jadelle' and 'Levoplant' based on a ratio.
        """
        logger.info("Processing '2 Rod' split logic...")

        # Filter for the specific rows to split
        target_rows = (pl.col("method") == "Service") & (pl.col("analytic") == "2 Rod")

        two_rod_df = df.filter(target_rows)

        # If no data to split, return original
        if two_rod_df.is_empty():
            logger.warning("No '2 Rod' service data found to split.")
            return df

        # Perform the Split
        split_df = (
            two_rod_df
            .sort(["org_unit", "analytic", "period"])
            # Note: Pivot/Unpivot strategy is good, but direct calc is often faster if structure is simple.
            # Sticking to pivot/unpivot for clarity as per original logic.
            .pivot(on="analytic", index=["org_unit", "method", "period"], values="value")
            .with_columns(
                Jadelle=pl.col("2 Rod") * jadelle_ratio,
                Levoplant=pl.col("2 Rod") * (1 - jadelle_ratio),
            )
            .drop("2 Rod")
            .unpivot(
                on=["Jadelle", "Levoplant"],
                index=["org_unit", "method", "period"],
                variable_name="analytic",
                value_name="value"
            )
            .with_columns(analytic=pl.col("analytic").str.to_titlecase())  # Ensure naming consistency if needed
        )

        # Combine: (Original Data - 2 Rod) + (Split Data)
        final_df = pl.concat([
            df.filter(~target_rows),
            split_df.select(df.columns)  # Ensure column order matches
        ], how="vertical")

        return final_df

    def run(self, table_name: str = "cleaned_fp_summary_data"):
        """
        Executes the full pipeline and saves to the database.
        """

        # Initialize variables to None so 'finally' block doesn't crash if they are never created
        data = None
        county_df = None
        national_df = None
        merged_df = None
        final_df = None

        try:
            # 1. Extract
            data = self._extract_data()

            # ---------------------------------------------------------
            # 2. Run Data Quality Check immediately after extraction
            # ---------------------------------------------------------
            self._audit_duplicates(data["raw_fp"], dataset_name="raw_fp_data")

            # 3. Transform (County Level)
            county_df = self._apply_transformations(
                data["raw_fp"], data["org_units"], data["elements"]
            )

            # 4. Transform (National Level)
            national_df = self._generate_national_aggregates(county_df)

            # 5. Merge
            merged_df = pl.concat([county_df, national_df], how="vertical")
            logger.info(f"Merged Data Shape: {merged_df.shape}")

            # 6. Apply Business Rules
            final_df = self._process_two_rod_split(merged_df)
            logger.info(f"Final Data Shape: {final_df.shape}")
            logger.info(f"Final Data Head:\n{final_df.head()}")

            # 7. Load (Save back to DB)
            save_df_to_db(
                df=final_df,
                db_url=self.db_url,
                table_name=table_name,
                if_table_exists="replace",
            )
            logger.success(f"Pipeline finished successfully. Data saved to table: {table_name}")

            return APIResponse(
                success=True,
                message=f"Pipeline finished successfully. Final Data Shape: {final_df.shape}",
                data=f"Processed data saved to configured database",
                trace_id=self.request_trace_id
            )

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return APIResponse(
                success=False,
                message=f"Pipeline failed. Error: {str(e)}. Check logs for more info.",
                data=None,
                trace_id=self.request_trace_id
            )
        finally:
            # 8. Cleanup
            logger.info("Cleaning up memory...")
            # Only delete if the variable exists and is not None
            if data is not None: del data
            if county_df is not None: del county_df
            if national_df is not None: del national_df
            if merged_df is not None: del merged_df
            if final_df is not None: del final_df

            gc.collect()

            logger.info("Cleaned up complete.")


# ============================================================================================
# Execution Entry Point
# ============================================================================================

if __name__ == "__main__":
    pipeline = FamilyPlanningDataTransformationPipeline(request_trace_id="test_trace_id")
    pipeline.run()