import pandas as pd

from src.DataEngineeringTask.config import RAW_DATA_PATH


def load_data(file_path=RAW_DATA_PATH):
    """
    Loads JSON data stored in JSON Lines format (NDJSON) as Pandas DataFrame.
    """
    return pd.read_json(file_path, lines=True)







