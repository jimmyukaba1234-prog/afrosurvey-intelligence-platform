"""
Gold-layer platform monitoring metric functions for AfroSurvey.

This module creates pipeline/platform-focused Gold outputs used by the
Platform Monitoring dashboard.

This version matches the actual Silver schema.

Important:
The Silver survey table does NOT currently have:
- ingestion_timestamp
- validation_status
- duplicate_flag

So:
- silver_processed_at is used as the processing timestamp
- completed responses are treated as successful jobs
- incomplete responses are treated as failed jobs
- warning_jobs is set to 0 for now
"""


def build_pipeline_runtime(spark):
    """
    Build pipeline runtime metrics.

    Required temp view:
    - survey_responses

    Output:
    - pipeline_runtime_gold
    """

    return spark.sql("""
        SELECT
            'afrosurvey_silver_to_gold' AS pipeline_name,

            CURRENT_TIMESTAMP() AS execution_date,

            ROUND(
                (
                    UNIX_TIMESTAMP(MAX(silver_processed_at))
                    - UNIX_TIMESTAMP(MIN(silver_processed_at))
                ),
                2
            ) AS runtime_seconds,

            ROUND(
                (
                    UNIX_TIMESTAMP(MAX(silver_processed_at))
                    - UNIX_TIMESTAMP(MIN(silver_processed_at))
                ) / 60.0,
                2
            ) AS runtime_minutes,

            'SUCCESS' AS status,

            COUNT(*) AS records_processed,

            CURRENT_TIMESTAMP() AS gold_processed_at

        FROM survey_responses
    """)


def build_pipeline_status(spark):
    """
    Build pipeline execution status metrics.

    Required temp view:
    - survey_responses

    Output:
    - pipeline_status_gold
    """

    return spark.sql("""
        SELECT
            'afrosurvey_silver_to_gold' AS pipeline_name,

            CURRENT_TIMESTAMP() AS execution_date,

            CASE
                WHEN COUNT(*) > 0 THEN 'SUCCESS'
                ELSE 'FAILED'
            END AS pipeline_status,

            SUM(
                CASE
                    WHEN LOWER(completion_status) = 'completed' THEN 1
                    ELSE 0
                END
            ) AS successful_jobs,

            SUM(
                CASE
                    WHEN LOWER(completion_status) != 'completed' THEN 1
                    ELSE 0
                END
            ) AS failed_jobs,

            0 AS warning_jobs,

            ROUND(
                (
                    SUM(
                        CASE
                            WHEN LOWER(completion_status) = 'completed' THEN 1
                            ELSE 0
                        END
                    ) * 100.0
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS overall_health_score,

            CURRENT_TIMESTAMP() AS gold_processed_at

        FROM survey_responses
    """)


def build_data_freshness(spark):
    """
    Build dataset freshness metrics.

    Required temp view:
    - survey_responses

    Output:
    - data_freshness_gold
    """

    return spark.sql("""
        SELECT
            'survey_responses' AS dataset_name,

            MIN(silver_processed_at) AS last_ingestion_timestamp,

            MAX(silver_processed_at) AS last_update_timestamp,

            ROUND(
                (
                    UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                    - UNIX_TIMESTAMP(MAX(silver_processed_at))
                ) / 3600.0,
                2
            ) AS freshness_hours,

            CASE
                WHEN (
                    (
                        UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                        - UNIX_TIMESTAMP(MAX(silver_processed_at))
                    ) / 3600.0
                ) <= 24 THEN 'Fresh'

                WHEN (
                    (
                        UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                        - UNIX_TIMESTAMP(MAX(silver_processed_at))
                    ) / 3600.0
                ) <= 72 THEN 'Stale'

                ELSE 'Very Stale'
            END AS freshness_status,

            'Daily' AS expected_refresh_frequency,

            CURRENT_TIMESTAMP() AS gold_processed_at

        FROM survey_responses
    """)