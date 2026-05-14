"""
scripts/generate_survey_data.py
Clean Synthetic Survey Data Generator for AfroSurvey Intelligence Platform
Generates realistic but mostly clean data so we can focus on the system, not heavy cleaning.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from faker import Faker
from pathlib import Path
from datetime import datetime
import random
from typing import Dict
import uuid

from utils.config import load_config
from utils.logger import get_logger

logger = get_logger(__name__)
config = load_config()

fake = Faker()
random.seed(42)
Faker.seed(42)
# =============================================
# CONFIGURATION - Use countries from config
# =============================================
COUNTRIES = config.get("countries", [
    "Nigeria", "Kenya", "Ghana", "Uganda", "South Africa"
])

# Environment-aware row scaling (to prevent local machine crash)
ENVIRONMENT = config.get("environment", "development")
DEFAULT_ROWS = 500 if ENVIRONMENT == "development" else 4000

ROWS_PER_COUNTRY = {
    "Nigeria": 800 if ENVIRONMENT == "development" else 6000,
    "South Africa": 700 if ENVIRONMENT == "development" else 4500,
    "Kenya": 600 if ENVIRONMENT == "development" else 4000,
    "Ghana": 500 if ENVIRONMENT == "development" else 3500,
    "Uganda": 500 if ENVIRONMENT == "development" else 3000,
}

OUTPUT_BASE = Path("data/raw")


def generate_survey_responses() -> Dict:
    """Generate realistic survey question answers."""
    return {
        "q1_democracy": random.randint(1, 5),
        "q2_economy": random.choice(["Bad", "Neutral", "Good", "Very Good"]),
        "q3_trust_govt": random.randint(1, 5),
        "q4_corruption": random.choice(["High", "Medium", "Low"]),
        "q5_election_fairness": random.randint(1, 5),
    }


def generate_row(country: str) -> Dict:
    """Generate one realistic survey row with light controlled imperfections."""
    gender_raw = random.choice(["Male", "Female"])

    # Controlled imperfections (light realism)
    if random.random() < 0.02:          # 2% missing ages
        age = None
    else:
        age = random.randint(18, 75)

    if random.random() < 0.03:          # 3% mixed gender casing
        gender_raw = random.choice(["MALE", "female", "Male", "FEMALE"])

    return {
        "response_id": str(uuid.uuid4()),
        "respondent_id": fake.uuid4(),
        "survey_id": f"survey_{random.randint(100, 999)}",
        "country": country,
        "region": fake.city(),
        "gender": gender_raw,
        "age": age,
        "education_level": random.choice(["Primary", "Secondary", "Tertiary"]),
        "employment_status": random.choice(["Employed", "Unemployed", "Self-employed"]),
        "completion_status": "completed",
        "submission_timestamp": fake.date_time_between(start_date="-6m", end_date="now").isoformat(),
        "source_system": "field_tool",
        **generate_survey_responses(),          # Flattened responses columns
    }


def generate_survey_data():
    """Main function - Generate synthetic survey data for all countries."""
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    total_rows = 0

    for country in COUNTRIES:
        num_rows = ROWS_PER_COUNTRY.get(country, DEFAULT_ROWS)
        print(f"Generating {num_rows:,} records for {country}...")

        # Generate multiple files per country (batch simulation)
        num_batches = 3 if ENVIRONMENT == "development" else 5
        rows_per_batch = num_rows // num_batches

        for batch in range(1, num_batches + 1):
            data = [generate_row(country) for _ in range(rows_per_batch)]
            df = pd.DataFrame(data)

            country_dir = OUTPUT_BASE / country.lower()
            country_dir.mkdir(parents=True, exist_ok=True)

            filename = f"{country.lower()}_survey_batch_{batch:03d}.csv"
            filepath = country_dir / filename

            df.to_csv(filepath, index=False)

            print(f"   → Batch {batch} saved: {len(df):,} rows → {filepath}")

            total_rows += len(df)

    print(f"\n✅ Synthetic data generation completed!")
    print(f"   Total records: {total_rows:,}")
    print(f"   Countries: {len(COUNTRIES)}")
    print(f"   Location: {OUTPUT_BASE.resolve()}")


if __name__ == "__main__":
    generate_survey_data()