"""
spark_jobs/transformations/cleaning.py

Reusable cleaning and standardization functions for the Silver layer.

This module does not read from MinIO and does not write output.
It only receives Spark DataFrames, applies cleaning logic, and returns cleaned DataFrames.

Used mainly by:
- spark_jobs/bronze_to_silver.py
"""

import re
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Convert DataFrame column names to clean snake_case format.

    Examples:
        "Response ID" -> "response_id"
        "Country-Code" -> "country_code"
        " IndicatorName " -> "indicator_name"
    Args:
        df: Input Spark DataFrame.
    Returns:
        Spark DataFrame with standardized column names.
    """
    renamed_df = df

    for column_name in renamed_df.columns:
        clean_name = column_name.strip()
        clean_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", clean_name)
        clean_name = clean_name.lower()
        clean_name = re.sub(r"[^a-z0-9]+", "_", clean_name)
        clean_name = re.sub(r"_+", "_", clean_name)
        clean_name = clean_name.strip("_")

        if clean_name != column_name:
            renamed_df = renamed_df.withColumnRenamed(column_name, clean_name)

    return renamed_df

# Remove leading and trailing whitespace from all string columns
def trim_string_columns(df: DataFrame) -> DataFrame:
    cleaned_df = df

    for field in cleaned_df.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned_df = cleaned_df.withColumn(
                field.name,
                F.trim(F.col(field.name)))

    return cleaned_df

# Convert empty strings and whitespace-only values to null
def replace_empty_strings_with_null(df: DataFrame) -> DataFrame:
    cleaned_df = df

    for field in cleaned_df.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned_df = cleaned_df.withColumn(
                field.name,
                F.when(
                    F.trim(F.col(field.name)) == "",
                    F.lit(None)
                ).otherwise(F.col(field.name)))

    return cleaned_df

# Standardize gender values into consistent categories
def clean_gender_values(
    df: DataFrame,
    column_name: str = "gender"
) -> DataFrame:

    if column_name not in df.columns:
        return df

    return df.withColumn(
        column_name,
        F.when(
            F.lower(F.col(column_name)).isin("male", "m"),
            F.lit("Male")
        ).when(
            F.lower(F.col(column_name)).isin("female", "f"),
            F.lit("Female")
        ).otherwise(F.lit("Unknown")))


# Clean and validate age values
def clean_age_values(
    df: DataFrame,
    column_name: str = "age",
    min_age: int = 18,
    max_age: int = 100
) -> DataFrame:

    if column_name not in df.columns:
        return df

    cleaned_df = df.withColumn(
        column_name,
        F.col(column_name).cast(IntegerType()))

    cleaned_df = cleaned_df.withColumn(
        column_name,
        F.when(
            (F.col(column_name) < min_age) |
            (F.col(column_name) > max_age),
            F.lit(None)
        ).otherwise(F.col(column_name)))

    return cleaned_df

# Standardize country names
def clean_country_names(
    df: DataFrame,
    column_name: str = "country"
) -> DataFrame:

    if column_name not in df.columns:
        return df

    return df.withColumn(
        column_name,
        F.initcap(F.trim(F.col(column_name))))

# Standardize common categorical columns
def clean_categorical_columns(
    df: DataFrame,
    columns: Optional[List[str]] = None
) -> DataFrame:

    cleaned_df = df

    if columns is None:
        columns = [
            "education_level",
            "employment_status",
            "marital_status",
            "completion_status",
            "region",
            "subregion"]

    existing_columns = [
        column for column in columns
        if column in cleaned_df.columns
    ]

    for column_name in existing_columns:
        cleaned_df = cleaned_df.withColumn(
            column_name,
            F.initcap(F.trim(F.col(column_name)))
        )

    return cleaned_df

# Cast survey response columns into proper Silver-layer data types
def cast_survey_column_types(df: DataFrame) -> DataFrame:

    column_type_mapping = {
        "age": IntegerType(),
        "response_id": StringType(),
        "respondent_id": StringType(),
        "country": StringType(),
        "country_code": StringType(),
        "submission_timestamp": TimestampType(),
        "survey_year": IntegerType(),
        "household_size": IntegerType(),
        "income": DoubleType(),
        "latitude": DoubleType(),
        "longitude": DoubleType()
    }
    casted_df = df

    for column_name, data_type in column_type_mapping.items():
        if column_name in casted_df.columns:
            casted_df = casted_df.withColumn(
                column_name,
                F.col(column_name).cast(data_type)
            )

    return casted_df


# Full cleaning pipeline for survey response datasets
def clean_survey_responses(df: DataFrame) -> DataFrame:

    cleaned_df = standardize_column_names(df)

    cleaned_df = trim_string_columns(cleaned_df)

    cleaned_df = replace_empty_strings_with_null(cleaned_df)

    cleaned_df = clean_gender_values(cleaned_df)

    cleaned_df = clean_age_values(cleaned_df)

    cleaned_df = clean_country_names(cleaned_df)

    cleaned_df = clean_categorical_columns(cleaned_df)

    cleaned_df = cast_survey_column_types(cleaned_df)

    return cleaned_df


# Clean normalized World Bank population dataset
def clean_world_bank_population(df: DataFrame) -> DataFrame:

    cleaned_df = df.select(
        F.col("country.value").alias("country"),
        F.col("countryiso3code").alias("country_code"),
        F.col("date").alias("year"),
        F.col("value").alias("population"),
        F.col("indicator.value").alias("indicator"),
        F.col("obs_status"),
        F.col("unit"),
        F.col("decimal")
    )

    cleaned_df = standardize_column_names(cleaned_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = replace_empty_strings_with_null(cleaned_df)
    cleaned_df = clean_country_names(cleaned_df)

    if "country_code" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "country_code",
            F.upper(F.trim(F.col("country_code")))
        )

    if "year" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "year",
            F.col("year").cast(IntegerType())
        )

    if "population" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "population",
            F.col("population").cast(LongType())
        )

        cleaned_df = cleaned_df.withColumn(
            "population",
            F.when(
                F.col("population") < 0,
                F.lit(None)
            ).otherwise(F.col("population"))
        )

    return cleaned_df


# Clean REST Countries reference dataset
def clean_country_reference(df: DataFrame) -> DataFrame:

    cleaned_df = df.select(
        F.col("name.common").alias("country_name"),
        F.col("name.official").alias("official_name"),
        F.col("region"),
        F.col("subregion"),
        F.col("population"),
        F.col("area")
    )

    cleaned_df = standardize_column_names(cleaned_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = replace_empty_strings_with_null(cleaned_df)
    cleaned_df = clean_country_names(cleaned_df)

    cleaned_df = clean_categorical_columns(
        cleaned_df,
        columns=["region", "subregion"]
    )

    if "population" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "population",
            F.col("population").cast(LongType())
        )

    if "area" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "area",
            F.col("area").cast(DoubleType()))

    return cleaned_df



# Add Silver-layer metadata columns
def add_silver_metadata(
    df: DataFrame,
    source_layer: str = "bronze",
    target_layer: str = "silver"
) -> DataFrame:

    metadata_df = df.withColumn(
        "silver_processed_at",
        F.current_timestamp())

    metadata_df = metadata_df.withColumn("source_layer",
        F.lit(source_layer))
    metadata_df = metadata_df.withColumn(
        "target_layer",
        F.lit(target_layer))
    return metadata_df


# Route datasets to the correct cleaning pipeline
def apply_cleaning_by_dataset(
    df: DataFrame,
    dataset_name: str
) -> DataFrame:

    dataset_name = dataset_name.lower().strip()

    cleaning_map = {
        "survey_responses": clean_survey_responses,
        "world_bank_population": clean_world_bank_population,
        "country_reference": clean_country_reference,
    }

    cleaning_function = cleaning_map.get(dataset_name)

    if cleaning_function is None:
        raise ValueError(
            f"No cleaning pipeline configured for dataset: {dataset_name}")
    cleaned_df = cleaning_function(df)
    cleaned_df = add_silver_metadata(cleaned_df)

    return cleaned_df

