"""
Gold-layer data quality metric functions for AfroSurvey.

This module creates quality-focused Gold outputs used by the
Platform Monitoring dashboard.

This version matches the actual Silver schema.

Important:
The Silver survey table does NOT currently have:
- validation_status
- duplicate_flag
- submission_date
- ingestion_timestamp

So:
- completed responses are treated as valid records
- duplicate records are set to 0 for now
- submission_timestamp is used instead of submission_date
- silver_processed_at is used instead of ingestion_timestamp
"""


def build_data_quality_summary(spark):
    """
    Build data quality summary metrics.

    Required temp view:
    - survey_responses

    Output:
    - data_quality_summary_gold
    """

    return spark.sql("""
        SELECT
            'survey_responses' AS dataset_name,

            COUNT(*) AS total_records,

            SUM(
                CASE
                    WHEN LOWER(completion_status) = 'completed' THEN 1
                    ELSE 0
                END
            ) AS valid_records,

            SUM(
                CASE
                    WHEN LOWER(completion_status) != 'completed' THEN 1
                    ELSE 0
                END
            ) AS invalid_records,

            0 AS duplicate_records,

            SUM(
                CASE WHEN response_id IS NULL THEN 1 ELSE 0 END
                + CASE WHEN respondent_id IS NULL THEN 1 ELSE 0 END
                + CASE WHEN survey_id IS NULL THEN 1 ELSE 0 END
                + CASE WHEN country IS NULL THEN 1 ELSE 0 END
                + CASE WHEN region IS NULL THEN 1 ELSE 0 END
                + CASE WHEN gender IS NULL THEN 1 ELSE 0 END
                + CASE WHEN age IS NULL THEN 1 ELSE 0 END
                + CASE WHEN completion_status IS NULL THEN 1 ELSE 0 END
                + CASE WHEN submission_timestamp IS NULL THEN 1 ELSE 0 END
                + CASE WHEN silver_processed_at IS NULL THEN 1 ELSE 0 END
                + CASE WHEN source_system IS NULL THEN 1 ELSE 0 END
            ) AS missing_value_count,

            ROUND(
                (
                    1 - (
                        SUM(
                            CASE WHEN response_id IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN respondent_id IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN survey_id IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN country IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN region IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN gender IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN age IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN completion_status IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN submission_timestamp IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN silver_processed_at IS NULL THEN 1 ELSE 0 END
                            + CASE WHEN source_system IS NULL THEN 1 ELSE 0 END
                        ) / (COUNT(*) * 11.0)
                    )
                ) * 100,
                2
            ) AS completeness_percentage,

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
            ) AS validity_percentage,

            100.00 AS uniqueness_percentage,

            CASE
                WHEN
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
                    ) >= 95
                THEN 'Excellent'

                WHEN
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
                    ) >= 85
                THEN 'Good'

                WHEN
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
                    ) >= 70
                THEN 'Fair'

                ELSE 'Poor'
            END AS quality_status,

            CURRENT_TIMESTAMP() AS gold_processed_at

        FROM survey_responses
    """)

def build_reliability_index(spark):
    """
    Build overall reliability index metrics.

    Required temp view:
    - survey_responses

    Output:
    - reliability_index_gold

    Note:
    Since validation_status and duplicate_flag are not currently available:
    - completed responses are treated as valid records
    - duplicate_records is set to 0
    - uniqueness_score is set to 100
    """

    return spark.sql("""
        WITH base_metrics AS (
            SELECT
                'afrosurvey_pipeline' AS pipeline_name,
                CURRENT_TIMESTAMP() AS execution_date,

                COUNT(*) AS total_records_processed,

                SUM(
                    CASE
                        WHEN LOWER(completion_status) = 'completed' THEN 1
                        ELSE 0
                    END
                ) AS valid_records,

                SUM(
                    CASE
                        WHEN LOWER(completion_status) != 'completed' THEN 1
                        ELSE 0
                    END
                ) AS invalid_records,

                0 AS duplicate_records,

                SUM(
                    CASE WHEN response_id IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN respondent_id IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN survey_id IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN country IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN region IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN gender IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN age IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN completion_status IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN submission_timestamp IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN silver_processed_at IS NULL THEN 1 ELSE 0 END
                    + CASE WHEN source_system IS NULL THEN 1 ELSE 0 END
                ) AS missing_value_count

            FROM survey_responses
        )

        SELECT
            pipeline_name,
            execution_date,
            total_records_processed,
            valid_records,
            invalid_records,
            duplicate_records,

            ROUND(
                (
                    1 - (
                        missing_value_count / NULLIF(total_records_processed * 11.0, 0)
                    )
                ) * 100,
                2
            ) AS completeness_score,

            ROUND(
                (valid_records * 100.0) / NULLIF(total_records_processed, 0),
                2
            ) AS validity_score,

            100.00 AS uniqueness_score,

            ROUND(
                (
                    (
                        (
                            1 - (
                                missing_value_count / NULLIF(total_records_processed * 11.0, 0)
                            )
                        ) * 100
                    )
                    +
                    ((valid_records * 100.0) / NULLIF(total_records_processed, 0))
                    +
                    100.00
                ) / 3,
                2
            ) AS overall_reliability_index,

            CASE
                WHEN
                    ROUND(
                        (
                            (
                                (
                                    1 - (
                                        missing_value_count / NULLIF(total_records_processed * 11.0, 0)
                                    )
                                ) * 100
                            )
                            +
                            ((valid_records * 100.0) / NULLIF(total_records_processed, 0))
                            +
                            100.00
                        ) / 3,
                        2
                    ) >= 95
                THEN 'Excellent'

                WHEN
                    ROUND(
                        (
                            (
                                (
                                    1 - (
                                        missing_value_count / NULLIF(total_records_processed * 11.0, 0)
                                    )
                                ) * 100
                            )
                            +
                            ((valid_records * 100.0) / NULLIF(total_records_processed, 0))
                            +
                            100.00
                        ) / 3,
                        2
                    ) >= 85
                THEN 'Good'

                WHEN
                    ROUND(
                        (
                            (
                                (
                                    1 - (
                                        missing_value_count / NULLIF(total_records_processed * 11.0, 0)
                                    )
                                ) * 100
                            )
                            +
                            ((valid_records * 100.0) / NULLIF(total_records_processed, 0))
                            +
                            100.00
                        ) / 3,
                        2
                    ) >= 70
                THEN 'Fair'

                ELSE 'Poor'
            END AS reliability_status,

            CURRENT_TIMESTAMP() AS gold_processed_at

        FROM base_metrics
    """)