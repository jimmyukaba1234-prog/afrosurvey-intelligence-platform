from pyspark.sql import SparkSession

from gold.aggregations import (
    build_country_survey_kpis,
    build_demographic_distribution,
    build_response_volume_trends,
    build_governance_trust,
    build_democracy_perception,
    build_corruption_perception,
    build_election_fairness,
    build_population_coverage
)

from gold.quality_metrics import (
    build_data_quality_summary,
    build_reliability_index
)

from gold.platform_metrics import (
    build_pipeline_runtime,
    build_pipeline_status,
    build_data_freshness
)



spark = SparkSession.builder \
    .appName("AfroSurvey Silver To Gold Pipeline") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://afrosurvey-minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()



# Read Silver Layer
survey_df = spark.read.parquet(
    "s3a://afrosurvey-silver/survey_responses/"
)

country_df = spark.read.parquet(
    "s3a://afrosurvey-silver/country_reference/"
)

population_df = spark.read.parquet(
    "s3a://afrosurvey-silver/world_bank_population/"
)



# Register Temp Views

survey_df.createOrReplaceTempView("survey_responses")
country_df.createOrReplaceTempView("country_reference")
population_df.createOrReplaceTempView("world_bank_population")



# Build Business Gold Tables

country_kpis_df = build_country_survey_kpis(spark)
demographic_distribution_df = build_demographic_distribution(spark)
response_volume_trends_df = build_response_volume_trends(spark)
governance_trust_df = build_governance_trust(spark)
democracy_perception_df = build_democracy_perception(spark)
corruption_perception_df = build_corruption_perception(spark)
election_fairness_df = build_election_fairness(spark)
population_coverage_df = build_population_coverage(spark)



# Build Quality Metrics
data_quality_summary_df = build_data_quality_summary(spark)
reliability_index_df = build_reliability_index(spark)



# Build Platform Metrics

pipeline_runtime_df = build_pipeline_runtime(spark)
pipeline_status_df = build_pipeline_status(spark)
data_freshness_df = build_data_freshness(spark)


# =========================
# Helper: Write Gold Table
# =========================

def write_gold_table(df, table_name):
    output_path = f"s3a://afrosurvey-gold/{table_name}/"

    df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Successfully wrote Gold table: {table_name} -> {output_path}")


# =========================
# Write Business Gold Tables
# =========================

write_gold_table(country_kpis_df, "country_survey_kpis_gold")
write_gold_table(demographic_distribution_df, "demographic_distribution_gold")
write_gold_table(response_volume_trends_df, "response_volume_trends_gold")
write_gold_table(governance_trust_df, "governance_trust_gold")
write_gold_table(democracy_perception_df, "democracy_perception_gold")
write_gold_table(corruption_perception_df, "corruption_perception_gold")
write_gold_table(election_fairness_df, "election_fairness_gold")
write_gold_table(population_coverage_df, "population_coverage_gold")


# =========================
# Write Quality Gold Tables
# =========================

write_gold_table(data_quality_summary_df, "data_quality_summary_gold")
write_gold_table(reliability_index_df, "reliability_index_gold")


# =========================
# Write Platform Gold Tables
# =========================

write_gold_table(pipeline_runtime_df, "pipeline_runtime_gold")
write_gold_table(pipeline_status_df, "pipeline_status_gold")
write_gold_table(data_freshness_df, "data_freshness_gold")


# =========================
# Preview Outputs
# =========================

print("Preview: country_survey_kpis_gold")
country_kpis_df.show(10, truncate=False)

print("Preview: demographic_distribution_gold")
demographic_distribution_df.show(10, truncate=False)

print("Preview: response_volume_trends_gold")
response_volume_trends_df.show(10, truncate=False)

print("Preview: data_quality_summary_gold")
data_quality_summary_df.show(10, truncate=False)

print("Preview: pipeline_status_gold")
pipeline_status_df.show(10, truncate=False)


# =========================
# Stop Spark
# =========================

spark.stop()