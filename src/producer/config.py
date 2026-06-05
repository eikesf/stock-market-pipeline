import os
from pathlib import Path

# BASE_DIR refers to the root of the project (stock_market_pipeline)
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# DATA_DIR points to stock_market_pipeline/data.
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))

# Define domains
PRICES_DOMAIN = "prices"
METADATA_DOMAIN = "metadata"

# Define layers
LANDING_PRICES_DIR = DATA_DIR / "landing" / PRICES_DOMAIN
LANDING_METADATA_DIR = DATA_DIR / "landing" / METADATA_DOMAIN

ARCHIVE_PRICES_DIR = DATA_DIR / "archive" / PRICES_DOMAIN
ARCHIVE_METADATA_DIR = DATA_DIR / "archive" / METADATA_DOMAIN

BRONZE_PRICES_DIR = DATA_DIR / "bronze" / PRICES_DOMAIN
BRONZE_METADATA_DIR = DATA_DIR / "bronze" / METADATA_DOMAIN

SILVER_PRICES_DIR = DATA_DIR / "silver" / PRICES_DOMAIN
SILVER_METADATA_DIR = DATA_DIR / "silver" / METADATA_DOMAIN

# Assure the directories exist before attempting to write into them
for d in [
    LANDING_PRICES_DIR,
    LANDING_METADATA_DIR,
    ARCHIVE_PRICES_DIR,
    ARCHIVE_METADATA_DIR,
    BRONZE_PRICES_DIR,
    BRONZE_METADATA_DIR,
    SILVER_PRICES_DIR,
    SILVER_METADATA_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)
