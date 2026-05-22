"""
Gold-layer business aggregation functions for AfroSurvey.

This module uses Spark SQL to transform Silver tables into
dashboard-ready Gold tables.
"""


def build_country_survey_kpis(spark):
    """
    Build country-level survey KPIs.
    Required temp views:
    - survey_responses
    - country_reference
    - world_bank_population
    Output:
    - country_survey_kpis_gold
    """

    return spark.sql("""
        WITH survey_country_metrics AS (
            SELECT
                country,
                country_code,
                region,
                COUNT(*) AS total_responses,
                SUM(
                    CASE
                        WHEN LOWER(completion_status) = 'completed' THEN 1
                        ELSE 0
                    END
                ) AS completed_responses,
                SUM(
                    CASE
                        WHEN LOWER(completion_status) != 'completed' THEN 1
                        ELSE 0
                    END
                ) AS incomplete_responses,
                SUM(
                    CASE
                        WHEN validation_status = 'valid' THEN 1
                        ELSE 0
                    END
                ) AS valid_responses,

                SUM(
                    CASE
                        WHEN validation_status != 'valid' THEN 1
                        ELSE 0
                    END
                ) AS invalid_responses,

                SUM(
                    CASE
                        WHEN duplicate_flag = true THEN 1
                        ELSE 0
                    END
                ) AS duplicate_responses

            FROM survey_responses
            GROUP BY
                country,
                country_code,
                region
        ),

        latest_population AS (
            SELECT
                country,
                country_code,
                population,
                year
            FROM (
                SELECT
                    country,
                    country_code,
                    population,
                    year,
                    ROW_NUMBER() OVER (
                        PARTITION BY country_code
                        ORDER BY year DESC
                    ) AS rn
                FROM world_bank_population
            )
            WHERE rn = 1
        )

        SELECT
            scm.country,
            scm.country_code,
            scm.region,
            cr.subregion,

            scm.total_responses,
            scm.completed_responses,
            scm.incomplete_responses,

            ROUND(
                (scm.completed_responses * 100.0) / scm.total_responses,
                2
            ) AS completion_rate,

            scm.valid_responses,
            scm.invalid_responses,
            scm.duplicate_responses,

            lp.population AS country_population,

            ROUND(
                (scm.total_responses * 1000000.0) / lp.population,
                2
            ) AS response_rate_per_million,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM survey_country_metrics scm
        LEFT JOIN country_reference cr
            ON LOWER(scm.country) = LOWER(cr.country_name)
        LEFT JOIN latest_population lp
            ON scm.country_code = lp.country_code
    """)


def build_demographic_distribution(spark):
    """
    Build demographic distribution metrics.
    Required temp view:
    - survey_responses
    Output:
    - demographic_distribution_gold
    """
    return spark.sql("""
        WITH demographic_counts AS (
            SELECT
                country,
                region,
                gender,
                age_group,
                education_level,
                employment_status,
                COUNT(*) AS response_count
            FROM survey_responses
            GROUP BY
                country,
                region,
                gender,
                age_group,
                education_level,
                employment_status
        ),

        total_responses AS (
            SELECT
                COUNT(*) AS total_count
            FROM survey_responses)

        SELECT
            dc.country,
            dc.region,
            dc.gender,
            dc.age_group,
            dc.education_level,
            dc.employment_status,
            dc.response_count,

            ROUND(
                (dc.response_count * 100.0) / tr.total_count,
                2
            ) AS percentage_of_total,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM demographic_counts dc
        CROSS JOIN total_responses tr
    """)


def build_response_volume_trends(spark):
    """
    Build daily response volume trends.

    Required temp view:
    - survey_responses

    Output:
    - response_volume_trends_gold
    """

    return spark.sql("""
        WITH daily_response_metrics AS (
            SELECT
                submission_date,
                country,
                region,
                COUNT(*) AS daily_response_count,

                SUM(
                    CASE
                        WHEN LOWER(completion_status) = 'completed' THEN 1
                        ELSE 0
                    END
                ) AS completed_response_count,

                SUM(
                    CASE
                        WHEN duplicate_flag = true THEN 1
                        ELSE 0
                    END
                ) AS duplicate_response_count,

                SUM(
                    CASE
                        WHEN LOWER(validation_status) = 'valid' THEN 1
                        ELSE 0
                    END
                ) AS valid_response_count

            FROM survey_responses
            GROUP BY
                submission_date,
                country,
                region
        )

        SELECT
            submission_date,
            country,
            region,
            daily_response_count,
            completed_response_count,
            duplicate_response_count,
            valid_response_count,

            SUM(daily_response_count) OVER (
                PARTITION BY country
                ORDER BY submission_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_response_count,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM daily_response_metrics
        ORDER BY
            country,
            submission_date
    """)