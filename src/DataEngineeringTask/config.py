import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "raw_data.json")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed", "cleaned_data.parquet")
PROCESSED_ATC_PATH = os.path.join(BASE_DIR, "data", "processed", "cleaned_atc.parquet")
DATA_PATH = os.path.join(BASE_DIR, "data", "processed", "cleaned_data.parquet")
ATC_PATH = os.path.join(BASE_DIR, "data", "processed", "cleaned_atc.parquet")

# Normalization settings
CITY_MATCH_THRESHOLD = 80  # Fuzzy matching threshold for city names

# Spark settings
SPARK_APP_NAME = "ATC_CLEANUP"

# Debugging options
DEBUG_MODE = True  # Set to False in production
