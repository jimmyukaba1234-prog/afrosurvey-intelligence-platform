"""
spark_jobs/transformations/deduplication.py

Reusable deduplication functions for the Silver layer.

This module receives Spark DataFrames and removes duplicate records
based on dataset-specific business keys.

Used mainly by:
- spark_jobs/bronze_to_silver.py
"""

from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

# Remove completely identical duplicate rows
def remove_exact_duplicates(df: DataFrame) -> DataFrame:

    deduplicated_df = df.dropDuplicates()

    return deduplicated_df

# Remove duplicates using selected business key columns
def remove_duplicates_by_keys(
    df: DataFrame,
    key_columns: List[str]
) -> DataFrame:

    existing_key_columns = [
        column for column in key_columns
        if column in df.columns]
    if not existing_key_columns:
        return df
    deduplicated_df = df.dropDuplicates(existing_key_columns)
    return deduplicated_df


# Keep the most recent record when duplicate keys exist
def keep_latest_record_by_keys(
    df: DataFrame,
    key_columns: List[str],
    order_column: str
) -> DataFrame:

    existing_key_columns = [
        column for column in key_columns
        if column in df.columns]
    if not existing_key_columns:
        return df
    if order_column not in df.columns:
        return df

    window_spec = Window.partitionBy(
        *existing_key_columns
    ).orderBy(F.col(order_column).desc())
    ranked_df = df.withColumn( "row_number",F.row_number().over(window_spec))
    deduplicated_df = ranked_df.filter(F.col("row_number") == 1).drop("row_number")
    return deduplicated_df


# Deduplicate survey response datasets
def deduplicate_survey_responses(df: DataFrame) -> DataFrame:
    if "response_id" in df.columns:
        deduplicated_df = remove_duplicates_by_keys(
            df,
            key_columns=["response_id"])
    else:
        deduplicated_df = remove_exact_duplicates(df)
    return deduplicated_df


# Deduplicate World Bank population dataset
def deduplicate_world_bank_population(df: DataFrame) -> DataFrame:
    deduplicated_df = remove_duplicates_by_keys(
        df,
        key_columns=[
            "country",
            "year",
            "indicator"]
    )
    return deduplicated_df

# Deduplicate REST Countries reference dataset
def deduplicate_country_reference(df: DataFrame) -> DataFrame:
    if "country_code" in df.columns:
        deduplicated_df = remove_duplicates_by_keys(
            df,
            key_columns=["country_code"])
    else:
        deduplicated_df = remove_duplicates_by_keys(
            df,
            key_columns=["country"])
    return deduplicated_df



# Route datasets to the correct deduplication pipeline
def apply_deduplication_by_dataset(
    df: DataFrame,
    dataset_name: str
) -> DataFrame:
    dataset_name = dataset_name.lower().strip()
    deduplication_map = {
        "survey_responses": deduplicate_survey_responses,
        "world_bank_population": deduplicate_world_bank_population,
        "country_reference": deduplicate_country_reference
    }
    deduplication_function = deduplication_map.get(dataset_name)
    if deduplication_function is None:
        raise ValueError(
            f"No deduplication pipeline configured for dataset: {dataset_name}")
    deduplicated_df = deduplication_function(df)
    return deduplicated_df