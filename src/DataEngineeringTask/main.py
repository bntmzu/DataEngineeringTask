import pandas as pd
from tabulate import tabulate
from src.DataEngineeringTask.utils import load_data
from src.DataEngineeringTask.data_processing import (
    clean_dataframe,
    normalize_cities,
    explode_categories,
    validate_data,
    process_atc_data
)
from src.DataEngineeringTask.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, PROCESSED_ATC_PATH, ATC_PATH, DATA_PATH


def main():
    """
    Main function to execute the data processing pipeline.
    - Loads raw data
    - Cleans business_id and removes duplicates
    - Normalizes city names
    - Splits multi-category values into separate rows
    - Validates final data
    - Converts to Spark DataFrame
    - Saves processed data in Parquet format
    """
    try:


        print(" Loading raw data...")
        df = load_data(RAW_DATA_PATH)

        print(" Cleaning business_id and removing duplicates...")
        df = clean_dataframe(df)

        print(" Normalizing city names...")
        df = normalize_cities(df)

        print(" Splitting categories into separate rows...")
        df = explode_categories(df)

        print(" Validating processed data...")
        df = validate_data(df)

        # Save business data directly
        print(" Saving processed business data as Parquet...")
        df.to_parquet(PROCESSED_DATA_PATH, index=False)

        try:
          df = pd.read_parquet(DATA_PATH, engine="pyarrow")
          print(tabulate(df.head(), headers="keys", tablefmt="grid"))
        except Exception as e:
            print(f"❌ Ошибка при чтении {DATA_PATH}: {e}")

        print(" Processing ATC (medical) data with Pandas...")
        df_atc = process_atc_data()

        print(" Saving ATC data as Parquet...")
        df_atc.to_parquet(PROCESSED_ATC_PATH, engine="pyarrow", index=False)


        df_atc = pd.read_parquet(ATC_PATH, engine="pyarrow")

        print(df_atc.dtypes)
        print(tabulate(df_atc, headers="keys", tablefmt="grid"))

        print(" All data processing complete!")

    except Exception as e:
        print(f"❌ Error encountered: {e}")


if __name__ == "__main__":
    main()

