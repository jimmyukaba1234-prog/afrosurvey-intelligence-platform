"""
scripts/load_survey_data_to_postgres.py
Load synthetic survey CSV datasets into PostgreSQL
for realistic DB ingestion simulation.
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import create_engine

from utils.config import load_config
from utils.logger import get_logger, log_structured

logger = get_logger(__name__)
config = load_config()


def get_postgres_engine():
    """Create SQLAlchemy PostgreSQL engine."""
    db_conf = config["database"]

    connection_string = (
        f"postgresql+psycopg2://"
        f"{db_conf['user']}:{db_conf['password']}"
        f"@{db_conf['host']}:{db_conf['port']}"
        f"/{db_conf['database']}"
    )

    return create_engine(connection_string)


def discover_csv_files(base_dir: str = "data/raw") -> list:
    """Discover all CSV files recursively in data/raw/."""
    base_path = Path(base_dir)
    csv_files = list(base_path.rglob("*.csv"))
    print(f"✅ Discovered {len(csv_files)} CSV files")
    return csv_files


def load_all_csv_data(csv_files) -> pd.DataFrame:
    """Load and combine all CSV files."""
    dataframes = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            print(f"Loaded {len(df):,} rows from {file.name}")
            dataframes.append(df)
        except Exception as e:
            print(f"❌ Failed to load {file.name}: {e}")

    if not dataframes:
        raise ValueError("No CSV files could be loaded")

    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"\n✅ Combined dataset contains {len(combined_df):,} rows")
    return combined_df


def clean_survey_data(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal cleaning."""
    df.columns = [
        col.lower().strip().replace(" ", "_").replace("-", "_")
        for col in df.columns
    ]
    df = df.drop_duplicates()
    print(f"✅ Cleaned dataset contains {len(df):,} rows")
    return df


def save_to_postgres(df: pd.DataFrame, table_name: str = "survey_responses"):
    """Save DataFrame to PostgreSQL."""
    engine = get_postgres_engine()

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi"
    )

    print(f"✅ Successfully saved {len(df):,} rows to table '{table_name}'")


def validate_table_load(table_name: str = "survey_responses"):
    """Validate table load."""
    engine = get_postgres_engine()
    query = f"SELECT COUNT(*) AS total_rows FROM {table_name}"
    result = pd.read_sql(query, engine)
    total_rows = int(result.iloc[0]["total_rows"])
    print(f"✅ Validation: Table '{table_name}' contains {total_rows:,} rows")


def main():
    """Main execution flow."""
    print("🚀 Starting synthetic survey data load into PostgreSQL...\n")

    csv_files = discover_csv_files()

    df = load_all_csv_data(csv_files)

    df = clean_survey_data(df)

    save_to_postgres(df, table_name="survey_responses")

    validate_table_load(table_name="survey_responses")

    print("\n✅ Synthetic survey dataset successfully loaded into PostgreSQL!")


if __name__ == "__main__":
    main()