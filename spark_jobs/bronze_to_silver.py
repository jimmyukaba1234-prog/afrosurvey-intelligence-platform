"""
spark_jobs/bronze_to_silver.py

Main Spark job for transforming Bronze-layer raw data into
clean, validated Silver-layer datasets.
"""

from pathlib import Path
from typing import Dict, List
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession


from spark_jobs.transformations.cleaning import apply_cleaning_by_dataset
from spark_jobs.transformations.deduplication import apply_deduplication_by_dataset
from spark_jobs.transformations.validation import apply_validation_by_dataset
from utils.config import load_config
from utils.logger import get_logger, log_structured

logger = get_logger(__name__)
config = load_config()

def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("AfroSurvey_Bronze_To_Silver")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", config["storage"]["minio"]["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", config["storage"]["minio"]["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", config["storage"]["minio"]["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        #.config("spark.hadoop.fs.s3a.committer.name", "directory")
        #.config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        #.config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_bronze_silver_paths() -> Dict[str, Dict[str, str]]:
    bronze_bucket = config["storage"]["buckets"]["bronze"]
    silver_bucket = config["storage"]["buckets"]["silver"]

    return {
        "survey_responses": {
            "input_path": f"s3a://{bronze_bucket}/country=*/year=*/month=*/day=*/csv/",
            "output_path": f"s3a://{silver_bucket}/survey_responses/",
            "file_format": "csv"
        },


        "world_bank_population": {
            "input_path": (
                f"s3a://{bronze_bucket}/"
                "country=global/year=*/month=*/day=*/world_bank/"
            ),
            "output_path": (
                f"s3a://{silver_bucket}/world_bank_population/"
            ),
            "file_format": "json"
        },

        "country_reference": {
            "input_path": (
                f"s3a://{bronze_bucket}/"
                "country=global/year=*/month=*/day=*/restcountries/"
            ),
            "output_path": (
                f"s3a://{silver_bucket}/country_reference/"
            ),
            "file_format": "json"
        }
    }


def read_bronze_dataset(
    spark: SparkSession,
    dataset_name: str,
    input_path: str,
    
    file_format: str
) -> DataFrame:

    log_structured(logger,"info","Reading Bronze dataset",
        dataset_name=dataset_name,
        input_path=input_path,
        file_format=file_format)

    if file_format == "csv":
        return (spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(input_path))
    if file_format == "json":
        return (spark.read.option("multiLine", True)
            .json(input_path))
    if file_format == "parquet":
        return spark.read.parquet(input_path)
    raise ValueError(f"Unsupported file format: {file_format}")


def transform_dataset(
    df: DataFrame,
    dataset_name: str
) -> DataFrame:

    log_structured(
        logger,
        "info",
        "Starting Bronze to Silver transformations",
        dataset_name=dataset_name
    )

    cleaned_df = apply_cleaning_by_dataset(
        df,
        dataset_name=dataset_name
    )

    print(f"\nDEBUG SCHEMA FOR: {dataset_name}")
    cleaned_df.printSchema()

    print(f"\nDEBUG SAMPLE DATA FOR: {dataset_name}")
    cleaned_df.show(5, truncate=False)

    deduplicated_df = apply_deduplication_by_dataset(
        cleaned_df,
        dataset_name=dataset_name
    )

    is_valid = apply_validation_by_dataset(
        deduplicated_df,
        dataset_name=dataset_name
    )

    if not is_valid:
        raise ValueError(
            f"Silver validation failed for dataset: {dataset_name}"
        )

    log_structured(
        logger,
        "info",
        "Bronze to Silver transformations completed",
        dataset_name=dataset_name
    )

    return deduplicated_df

def write_silver_dataset(
    df: DataFrame,
    output_path: str,
    partition_columns: Optional[List[str]] = None
) -> None:

    writer = (df.write.mode("overwrite").format("parquet"))
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(output_path)
    log_structured(logger,"info","Silver dataset written successfully",
        output_path=output_path,
        rows_written=df.count())


def run_bronze_to_silver_pipeline() -> None:

    spark = create_spark_session()
    dataset_paths = get_bronze_silver_paths()

    try:
        for dataset_name, dataset_config in dataset_paths.items():

            log_structured(logger,"info",
                "Starting Bronze to Silver dataset pipeline",
                dataset_name=dataset_name)

            bronze_df = read_bronze_dataset(
                spark=spark,
                dataset_name=dataset_name,
                input_path=dataset_config["input_path"],
                file_format=dataset_config["file_format"])

            silver_df = transform_dataset(bronze_df,
                dataset_name=dataset_name)

            partition_columns = None
            if "country" in silver_df.columns:
                partition_columns = ["country"]
            write_silver_dataset(
                df=silver_df,
                output_path=dataset_config["output_path"],
                partition_columns=partition_columns)

            log_structured(logger,"info",
                "Bronze to Silver dataset pipeline completed",
                dataset_name=dataset_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver_pipeline()