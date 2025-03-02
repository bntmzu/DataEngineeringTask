import pandas as pd
import json
from pyspark.sql import SparkSession

from src.DataEngineeringTask.config import RAW_DATA_PATH, SPARK_APP_NAME

def initialize_spark():
    """
    Initializes Spark session.
    """
    return SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate()

def load_data(file_path=RAW_DATA_PATH):
    """
    Loads JSON data as Pandas DataFrame.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.DataFrame(data)







