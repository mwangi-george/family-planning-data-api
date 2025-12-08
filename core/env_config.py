
import os
from dotenv import load_dotenv

load_dotenv()

class EnvConfig:
    """
    Gets environment variables from env file
    """

    DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")
    FP_DB_URL = os.getenv("FP_DB_URL")


env_config = EnvConfig()