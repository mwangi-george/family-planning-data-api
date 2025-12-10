# Family Planning Data Extraction API (DHIS2 Connector)

A high-performance FastAPI application designed to extract, transform, and load (ETL) Family Planning (FP) data from a DHIS2 instance (specifically tailored for KHIS).

This system utilizes **Polars** for rapid data processing, **FastAPI BackgroundTasks** for asynchronous extraction, and implements specific business rules to standardize consumption and service data for analytics.


## Features

* **DHIS2 Integration:** Automated extraction of Organisation Units, Data Elements, Indicators, and Historical Data.

* **High-Performance Processing:** Uses Polars dataframe library for memory-efficient and fast data manipulation.

* **Asynchronous Architecture:** Handles long-running data fetch operations using background tasks to prevent API blocking.

* **Observability:** Integrated Request Tracing (via `X-Trace-ID`) and structured logging using `Loguru` with automatic file rotation.

* **Business Logic Implementation:**

  * Automatic splitting of "2 Rod" implants into "Jadelle" and "Levoplant".

  * Application of reporting multipliers (e.g., Condoms, POPs).

  * Aggregation at County and National levels.

* **Smart Hierarchy:** Flattens DHIS2 nested organisation units into a wide-format (Country -> County -> Sub County -> Ward -> Facility).

## Tech Stack

* **Language:** Python 3.13

* **Web Framework:** FastAPI

* **Data Processing:** Polars

* **Database Interaction:** SQLAlchemy, ConnectorX, Psycopg2

* **Logging:** Loguru

* **Task Management:** FastAPI BackgroundTasks

## Project Structure

```text
Directory structure:
└── mwangi-george-family-planning-data-extraction-api/
    ├── README.md
    ├── main.py
    ├── pyproject.toml
    ├── requirements.txt
    ├── .python-version
    ├── core/
    │   ├── __init__.py
    │   ├── context.py
    │   ├── env_config.py
    │   ├── logging_config.py
    │   └── middlewares.py
    ├── routes/
    │   ├── __init__.py
    │   ├── data_cleaning.py
    │   └── data_extraction.py
    ├── schemas/
    │   ├── __init__.py
    │   └── shared.py
    └── services/
        ├── __init__.py
        ├── helpers.py
        ├── data_cleaning/
        │   ├── __init__.py
        │   └── transform_extracted_data.py
        └── data_extraction/
            ├── __init__.py
            ├── download_historical_data.py
            ├── download_metadata.py
            ├── get_data_elements.py
            ├── get_indicators.py
            └── get_organisation_units.py
```

## Setup & Installation

**Prerequisites**

* Python 3.13+
* PostgreSQL Database

1. **Clone the repository**
```text
git clone https://github.com/your-username/family-planning-data-extraction-api.git
cd family-planning-data-extraction-api
```

2. **Create a Virtual Environment**
```text
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install Dependencies**
```text
pip install -r requirements.txt
```

4. **Configure Environment Variables**

Create a .env file in the root directory:
```ini
# DHIS2 Credentials
DHIS2_BASE_URL=https://his.kenya.go.ke
DHIS2_USERNAME=your_username
DHIS2_PASSWORD=your_password

# Database Connection (PostgreSQL)
FP_DB_URL=postgresql://user:password@localhost:5432/fp_db
```


## Running the Application

Start the server using Uvicorn:
```text
uvicorn main:app --reload --port 8000
```
The API will be available at http://localhost:8000. API Documentation (Swagger UI) is available at http://localhost:8000/docs.


## API Endpoints

1. **Data Extraction**

    These endpoints trigger background tasks to fetch data from DHIS2 and store raw records in the database.

   * **POST** /data-extraction/download-metadata
     * Downloads Organisation Units, Data Elements, and Indicators. 
     * Recommended to run this first to populate lookup tables.

   * **POST** /data-extraction/download-historical-data
     * Downloads consumption and service data for a specific date range.
     * Query Parameters: start_date (YYYY-MM-DD), end_date (YYYY-MM-DD).

2. **Data Cleaning**

   * **POST** /data_cleaning/pipeline 
     * Runs the transformation pipeline on the raw data stored in the DB. 
     * Performs cleaning, mapping, logic application, and aggregation. 
     * Saves final results to the cleaned_fp_summary_data table.


## Business Logic Explained

The `FamilyPlanningDataTransformationPipeline` performs specific transformations to standardize the data:

1. **ID Mapping:** Converts cryptic DHIS2 IDs (e.g., J6qnTev1LXw) into standard names (e.g., DMPA-IM). 


2. **Service Multipliers:**

   * **Female/Male Condoms:** Multiplied by 10.

   * **COCs:** Multiplied by 1.25.

   * **POPs:** Multiplied by 0.5.

3. **Implant Splitting:** Service Data recorded as "2 Rod" is split: **80% to Jadelle** and **20% to Levoplant**.

4. **Aggregation**: Data is grouped by `Analytic` -> `Method` -> `Period` -> `Org Unit (County/National)`.

## Logging & Tracing

* **Logs:** stored in `logs/app.log`.

* **Rotation:** Logs rotate automatically when reaching 10 MB; compressed copies are kept.

* **Trace ID:** Every request generates a unique `X-Trace-ID`. This ID is injected into every log entry created during that request lifecycle, making debugging async processes significantly easier.
