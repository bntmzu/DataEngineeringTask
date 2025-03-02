import os

# Paths for data
RAW_DATA_PATH = os.path.join("data", "raw", "raw_data.json")
PROCESSED_DATA_PATH = os.path.join("data", "processed", "cleaned_data.parquet")
PROCESSED_ATC_PATH = os.path.join("data", "processed", "cleaned_atc.parquet")

# Normalization settings
CITY_MATCH_THRESHOLD = 80  # Fuzzy matching threshold for city names

# Spark settings
SPARK_APP_NAME = "ATC_CLEANUP"

# Debugging options
DEBUG_MODE = True  # Set to False in production
