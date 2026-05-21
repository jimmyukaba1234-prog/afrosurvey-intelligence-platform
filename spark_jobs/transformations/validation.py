"""
spark_jobs/transformations/validation.py

Reusable validation functions for the Silver layer.

This module receives Spark DataFrames and checks whether cleaned datasets
meet required quality and schema expectations before writing to Silver.

Used mainly by:
- spark_jobs/bronze_to_silver.py
"""

from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Check that required columns exist in the DataFrame
def validate_required_columns(
    df: DataFrame,
    required_columns: List[str]
) -> bool:
    missing_columns = [column for column in required_columns
        if column not in df.columns]
    return len(missing_columns) == 0


# Check that important columns do not exceed allowed null percentage
def validate_null_thresholds(
    df: DataFrame,
    column_thresholds: Dict[str, float]
) -> bool:
    total_rows = df.count()
    if total_rows == 0:
        return False

    for column_name, threshold in column_thresholds.items():
        if column_name not in df.columns:
            return False

        null_count = df.filter(F.col(column_name).isNull()).count()
        null_percentage = null_count / total_rows
        if null_percentage > threshold:
            return False
    return True


# Check that numeric values fall within expected ranges
def validate_numeric_ranges(
    df: DataFrame,
    range_rules: Dict[str, Dict[str, float]]
) -> bool:
    for column_name, rules in range_rules.items():
        if column_name not in df.columns:
            return False
        min_value = rules.get("min")
        max_value = rules.get("max")
        invalid_count = df.filter(
            (F.col(column_name) < min_value) |
            (F.col(column_name) > max_value)
        ).count()

        if invalid_count > 0:
            return False
    return True


# Check that selected key columns contain unique records
def validate_unique_keys(
    df: DataFrame,
    key_columns: List[str]
) -> bool:

    existing_key_columns = [column for column in key_columns
        if column in df.columns]
    if not existing_key_columns:
        return False
    total_rows = df.count()
    unique_rows = df.select(*existing_key_columns).distinct().count()
    return total_rows == unique_rows


# Validate survey response Silver dataset
def validate_survey_responses(df: DataFrame) -> bool:
    required_columns = [
        "response_id",
        "country",
        "gender",
        "age"
    ]

    null_thresholds = {
        "response_id": 0.0,
        "country": 0.05,
        "gender": 0.10,
        "age": 0.15
    }

    numeric_ranges = {
        "age": {
            "min": 18,
            "max": 100
        }
    }

    return (
        validate_required_columns(df, required_columns)
        and validate_null_thresholds(df, null_thresholds)
        and validate_numeric_ranges(df, numeric_ranges)
        and validate_unique_keys(df, ["response_id"])
    )

# Validate World Bank population Silver dataset
def validate_world_bank_population(df: DataFrame) -> bool:
    required_columns = [
        "country",
        "country_code",
        "year",
        "population"
    ]

    null_thresholds = {
        "country": 0.0,
        "year": 0.0,
        "population": 0.10
    }

    numeric_ranges = {
        "year": {
            "min": 1960,
            "max": 2100
        },
        "population": {
            "min": 0,
            "max": 10000000000
        }
    }

    return (
        validate_required_columns(df, required_columns)
        and validate_null_thresholds(df, null_thresholds)
        and validate_numeric_ranges(df, numeric_ranges)
        and validate_unique_keys(
            df,
            ["country", "year", "indicator"]
        )
    )

# Validate REST Countries reference Silver dataset
def validate_country_reference(df: DataFrame) -> bool:

    required_columns = [
        "country_name",
        "region",
        "population"
    ]

    null_thresholds = {
        "country_name": 0.0,
        "region": 0.10,
        "population": 0.10
    }

    numeric_ranges = {
        "population": {
            "min": 0,
            "max": 10000000000
        },
        "area": {
            "min": 0,
            "max": 20000000
        }
    }

    unique_key = ["country_name"]

    return (
        validate_required_columns(df, required_columns)
        and validate_null_thresholds(df, null_thresholds)
        and validate_numeric_ranges(df, numeric_ranges)
        #and validate_unique_keys(df, unique_key)
    )


# Route datasets to the correct validation pipeline
def apply_validation_by_dataset(
    df: DataFrame,
    dataset_name: str
) -> bool:

    dataset_name = dataset_name.lower().strip()

    validation_map = {
        "survey_responses": validate_survey_responses,
        "world_bank_population": validate_world_bank_population,
        "country_reference": validate_country_reference,
    }

    validation_function = validation_map.get(dataset_name)

    if validation_function is None:
        raise ValueError(
            f"No validation pipeline configured for dataset: {dataset_name}"
        )

    return validation_function(df)