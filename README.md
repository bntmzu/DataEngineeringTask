# Data Engineering Task

## 📌 Project Overview

This project focuses on processing medical and business data. The initial data analysis was conducted in a Jupyter Notebook, and later, an automated data pipeline was implemented in PyCharm. The pipeline ensures data cleaning, deduplication, and transformation into a structured format.

## 📂 Project Structure

```
DataEngineeringTask/
│── data/                    # Data storage directory
│   ├── raw/                 # Raw input data
│   │   ├── raw_data.json    # Original dataset
│   ├── processed/           # Processed output data
│   │   ├── cleaned_atc.parquet  # Final cleaned dataset
        ├── cleaned_data.parquet  # Final cleaned dataset
│── notebooks/
        ├── prep.ipynb       # Jupyter notebook for initial analysis
│── src/                     # Source code directory
│   ├── DataEngineeringTask/
│   │   ├── __init__.py
│   │   ├── main.py          # Entry point to execute the pipeline
│   │   ├── config.py        # Configuration settings
│   │   ├── data_processing.py  # Data transformation logic
│   │   ├── utils.py         # Utility functions
│── tests/                   # Unit tests (if applicable)
│── README.md                # Project documentation
│── .gitignore               # Files to exclude from version control
│── poetry.lock              # Poetry lock file
│── pyproject.toml           # Poetry dependency management file
```

## 🛠️ Installation & Setup

To run this project, follow these steps:

1. **Clone the repository**

   ```sh
   git clone https://github.com/your-username/DataEngineeringTask.git
   cd DataEngineeringTask
   ```

2. **Set up a virtual environment**

   ```sh
   python -m venv .venv
   source .venv/bin/activate  # For MacOS/Linux
   .venv\Scripts\activate     # For Windows
   ```

3. **Install dependencies**

   ```sh
   pip install poetry
   poetry install
   ```


## 🚀 Running the Pipeline

To execute the full pipeline, run:

```sh
poetry run python src/DataEngineeringTask/main.py
```

This will:

1. Load raw data from `data/raw/raw_data.json`
2. Perform data cleaning and deduplication
3. Normalize city names using fuzzy matching
4. Process ATC medical data
5. Save the cleaned output in Parquet format

The process and results will be displayed in the console.

## ⚙️ Key Features

- **Initial Data Analysis**: Conducted in Jupyter Notebook (`notebooks/`)
- **Automated Pipeline**: Implemented in PyCharm
- **Data Cleaning**: Handles duplicates, missing values, and ensures correct data types
- **Fuzzy Matching**: Normalizes city names to avoid inconsistencies
- **Final Output**: Cleaned data stored in Parquet format for efficient processing

## 🐟 License

This project is for educational purposes. Feel free to modify and adapt as needed.

