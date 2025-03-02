import pandas as pd
import json
import re
from thefuzz import process
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType

from src.DataEngineeringTask.config import CITY_MATCH_THRESHOLD


def clean_dataframe(df):
    """
    Performs the initial data cleaning steps:
    - Converts dictionary columns to JSON strings.
    - Removes duplicate rows.
    - Handles missing business_id values by filling from matching records.
    - Ensures no missing business_id remains.
    """
    # Convert dictionary columns into JSON strings
    columns_to_convert = ["attributes", "hours"]
    for col in columns_to_convert:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Fill missing business_id based on name, city, and address
    df_filled_ids = df.dropna(subset=["business_id"]).drop_duplicates(subset=["name", "city", "address"])
    df = df.merge(df_filled_ids[["name", "city", "address", "business_id"]],
                  on=["name", "city", "address"],
                  how="left",
                  suffixes=("", "_filled"))

    # Fill business_id if it is missing
    df["business_id"] = df["business_id"].fillna(df["business_id_filled"])
    df.drop(columns=["business_id_filled"], inplace=True)

    # Final check: Assign "Unknown" prefix to remaining missing business_id
    df["business_id"] = df["business_id"].fillna("Unknown_" + df.index.to_series().astype(str))

    print(f" Remaining {df['business_id'].isna().sum()} records with NaN in business_id after update.")

    return df

def clean_city_name(city):
    """
    Cleans city names by:
    - Removing leading/trailing spaces
    - Replacing multiple spaces with a single space
    - Converting to Title Case
    - Removing dots and commas
    """
    if pd.isna(city):
        return None
    city = city.strip()
    city = re.sub(r"\s+", " ", city)
    city = city.title()
    city = re.sub(r"[.,]", "", city)
    return city

def normalize_cities(df):
    """
    Normalizes city names using fuzzy matching.
    """
    df["city"] = df["city"].apply(clean_city_name)
    unique_cities = df["city"].dropna().unique()
    city_mapping = {}

    for city in unique_cities:
        best_match, score = process.extractOne(city, city_mapping.keys(), score_cutoff=CITY_MATCH_THRESHOLD)
        city_mapping[city] = best_match if best_match else city

    df["city"] = df["city"].map(city_mapping)
    return df

def explode_categories(df):
    """
    Expands multi-category values into separate rows.
    """
    return (
        df.assign(categories=df["categories"].fillna("").str.split(", "))  # Handle NaN
        .explode("categories")
        .assign(categories=lambda x: x["categories"].str.strip().str.title())  # Clean formatting
     )

def validate_data(df):
    """
    Ensures:
    - No missing values in key columns
    - Each category appears in a separate row
    """
    missing_values = df.isna().sum()
    print(f" Missing values check:\n{missing_values}")

    unique_cities = df["city"].nunique()
    print(f" Unique cities count: {unique_cities}")

    return df

def process_atc_data(spark):
    """
    Loads and processes ATC medical data using PySpark.
    Ensures correct schema and deduplication by `valid_from` date.
    """
    schema = StructType([
    StructField("valid_from", StringType(), True),
    StructField("valid_to", StringType(), True),
    StructField("code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("description_en", StringType(), True)
    ])

    ATC = [
        ("1900-01-01", "9999-12-31", "A", "Maagdarmkanaal en Metabolisme", "Alimentary Tract and Metabolism"),
	    ("1900-01-01", "9999-12-31", "B", "Bloed en Bloedvormende Organen", "Blood and Blood Forming Organs"),
	    ("1900-01-01", "9999-12-31", "C", "Hartvaatstelsel", "Cardiovascular System"),
	    ("1900-01-01", "9999-12-31", "D", "Dermatologica", "Dermatologicals"),
	    ("1900-01-01", "9999-12-31", "G", "Urogenitale Stelsel en Geslachtshormonen", "Genito Urinary System and Sex Hormones"),
	    ("1900-01-01", "2022-01-30", "H", "Systemische Hormoonpreparaten, Excl Geslachtshormonen", "Systemic Hormonal Prep,Excl Sex Hormones"),
	    ("2022-01-31", "9999-12-31", "H", "Systemische Hormoonpreparaten, Excl Geslachtshormonen", "Systemic Hormonal Preparations, Excl. Sex Hormones and Insulins"),
	    ("1900-01-01", "2022-01-30", "J", "Antimicrobiele Middelen Voor Systemisch Gebruik", "General Antiinfectives for Systemic Use"),
	    ("2022-01-31", "9999-12-31", "J", "Antimicrobiele Middelen Voor Systemisch Gebruik", "Antiinfectives for Systemic Use"),
	    ("1900-01-01", "9999-12-31", "L", "Oncolytica en Immunomodulantia", "Antineoplastic and Immunomodulating Agents"),
	    ("1900-01-01", "9999-12-31", "M", "Skeletspierstelsel", "Musculo-Skeletal System"),
	    ("1900-01-01", "9999-12-31", "N", "Zenuwstelsel", "Nervous System"),
	    ("1900-01-01", "2022-01-30", "P", "Antiparasitica, Insecticiden en Insectenwerende Middelen", "Antiparasitic Products,Insecticides and Repellents"),
	    ("2022-01-31", "9999-12-31", "P", "Antiparasitica, Insecticiden en Insectenwerende Middelen", "Antiparasitic Products, Insecticides and Repellents"),
	    ("1900-01-01", "9999-12-31", "R", "Ademhalingsstelsel", "Respiratory System"),
	    ("1900-01-01", "9999-12-31", "S", "Zintuiglijke Organen", "Sensory Organs"),
	    ("1900-01-01", "9999-12-31", "V", "Diverse Middelen", "Various"),
	    ("1900-01-01", "2024-12-30", "Y", "Niet Ingevuld", "Not filled in"),
	    ("2024-12-31", "9999-12-31", "Y", "Niet Ingevuld", None),
	    ("1900-01-01", "2024-12-30", "Z", "Niet Van Toepassing", "Not applicable"),
	    ("2024-12-31", "9999-12-31", "Z", "Niet Van Toepassing", None)
    ]

    df_atc = spark.createDataFrame(ATC, schema=schema)
    # Convert valid_from and valid_to to DateType
    df_atc = df_atc.withColumn("valid_from", df_atc["valid_from"].cast(DateType()))
    df_atc = df_atc.withColumn("valid_to", df_atc["valid_to"].cast(DateType()))

    df_atc = df_atc.withColumn("row_num", F.row_number().over(
        Window.partitionBy("code").orderBy(F.desc("valid_from"))
    ))
    df_atc = df_atc.filter(df_atc.row_num == 1).drop("row_num")

    return df_atc

