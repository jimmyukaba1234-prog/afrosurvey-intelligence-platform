"""
Gold-layer business aggregation functions for AfroSurvey.

This module uses Spark SQL to transform Silver tables into
dashboard-ready Gold tables.

This version matches the actual Silver schema currently written to MinIO:

survey_responses columns:
- response_id
- respondent_id
- survey_id
- region
- gender
- age
- education_level
- employment_status
- completion_status
- submission_timestamp
- source_system
- q1_democracy
- q2_economy
- q3_trust_govt
- q4_corruption
- q5_election_fairness
- silver_processed_at
- source_layer
- target_layer
- country

Important:
The Silver survey table does NOT currently have:
- country_code
- validation_status
- duplicate_flag
- age_group
- submission_date
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
                ) AS incomplete_responses

            FROM survey_responses
            GROUP BY
                country,
                region
        ),

        latest_population AS (
            SELECT
                country,
                population,
                year
            FROM (
                SELECT
                    country,
                    population,
                    year,
                    ROW_NUMBER() OVER (
                        PARTITION BY LOWER(country)
                        ORDER BY year DESC
                    ) AS rn
                FROM world_bank_population
                WHERE population IS NOT NULL
            )
            WHERE rn = 1
        )

        SELECT
            scm.country,
            scm.region,
            cr.subregion,

            scm.total_responses,
            scm.completed_responses,
            scm.incomplete_responses,

            ROUND(
                (scm.completed_responses * 100.0) / NULLIF(scm.total_responses, 0),
                2
            ) AS completion_rate,

            /*
            These are kept so the downstream dashboard/reporting layer
            still gets stable columns even though validation_status and
            duplicate_flag are not currently available in Silver.
            */
            scm.completed_responses AS valid_responses,
            scm.incomplete_responses AS invalid_responses,
            0 AS duplicate_responses,

            lp.population AS country_population,

            ROUND(
                (scm.total_responses * 1000000.0) / NULLIF(lp.population, 0),
                2
            ) AS response_rate_per_million,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM survey_country_metrics scm

        LEFT JOIN country_reference cr
            ON LOWER(scm.country) = LOWER(cr.country_name)

        LEFT JOIN latest_population lp
            ON LOWER(scm.country) = LOWER(lp.country)
    """)


def build_demographic_distribution(spark):
    """
    Build demographic distribution metrics.

    Required temp view:
    - survey_responses

    Output:
    - demographic_distribution_gold

    Note:
    age_group is created dynamically from the numeric age column.
    """

    return spark.sql("""
        WITH demographic_counts AS (
            SELECT
                country,
                region,
                gender,

                CASE
                    WHEN age IS NULL THEN 'Unknown'
                    WHEN age < 25 THEN '18-24'
                    WHEN age < 35 THEN '25-34'
                    WHEN age < 45 THEN '35-44'
                    WHEN age < 55 THEN '45-54'
                    ELSE '55+'
                END AS age_group,

                education_level,
                employment_status,

                COUNT(*) AS response_count

            FROM survey_responses

            GROUP BY
                country,
                region,
                gender,

                CASE
                    WHEN age IS NULL THEN 'Unknown'
                    WHEN age < 25 THEN '18-24'
                    WHEN age < 35 THEN '25-34'
                    WHEN age < 45 THEN '35-44'
                    WHEN age < 55 THEN '45-54'
                    ELSE '55+'
                END,

                education_level,
                employment_status
        ),

        total_responses AS (
            SELECT
                COUNT(*) AS total_count
            FROM survey_responses
        )

        SELECT
            dc.country,
            dc.region,
            dc.gender,
            dc.age_group,
            dc.education_level,
            dc.employment_status,
            dc.response_count,

            ROUND(
                (dc.response_count * 100.0) / NULLIF(tr.total_count, 0),
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

    Note:
    submission_date is derived from submission_timestamp.
    """

    return spark.sql("""
        WITH daily_response_metrics AS (
            SELECT
                DATE(submission_timestamp) AS submission_date,
                country,
                region,

                COUNT(*) AS daily_response_count,

                SUM(
                    CASE
                        WHEN LOWER(completion_status) = 'completed' THEN 1
                        ELSE 0
                    END
                ) AS completed_response_count,

                /*
                duplicate_flag is not currently available in Silver,
                so this is set to 0 for now.
                */
                0 AS duplicate_response_count,

                /*
                validation_status is not currently available in Silver.
                For now, completed responses are treated as valid responses.
                */
                SUM(
                    CASE
                        WHEN LOWER(completion_status) = 'completed' THEN 1
                        ELSE 0
                    END
                ) AS valid_response_count

            FROM survey_responses

            GROUP BY
                DATE(submission_timestamp),
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

def build_governance_trust(spark):
    """
    Build governance trust metrics.

    Required temp view:
    - survey_responses

    Output:
    - governance_trust_gold
    """

    return spark.sql("""
        WITH valid_responses AS (
            SELECT
                country,
                region,
                q3_trust_govt
            FROM survey_responses
            WHERE LOWER(completion_status) = 'completed'
              AND q3_trust_govt IS NOT NULL
        )

        SELECT
            country,
            region,
            COUNT(*) AS total_responses,

            ROUND(
                AVG(CAST(q3_trust_govt AS DOUBLE)),
                2
            ) AS average_trust_score,

            SUM(
                CASE
                    WHEN CAST(q3_trust_govt AS INT) <= 2 THEN 1
                    ELSE 0
                END
            ) AS low_trust_count,

            SUM(
                CASE
                    WHEN CAST(q3_trust_govt AS INT) = 3 THEN 1
                    ELSE 0
                END
            ) AS medium_trust_count,

            SUM(
                CASE
                    WHEN CAST(q3_trust_govt AS INT) >= 4 THEN 1
                    ELSE 0
                END
            ) AS high_trust_count,

            ROUND(
                (
                    SUM(CASE WHEN CAST(q3_trust_govt AS INT) >= 4 THEN 1 ELSE 0 END) * 100.0
                    -
                    SUM(CASE WHEN CAST(q3_trust_govt AS INT) <= 2 THEN 1 ELSE 0 END) * 100.0
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS trust_index,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM valid_responses
        GROUP BY
            country,
            region
        ORDER BY
            average_trust_score DESC
    """)


def build_democracy_perception(spark):
    """
    Build democracy perception metrics.

    Required temp view:
    - survey_responses

    Output:
    - democracy_perception_gold
    """

    return spark.sql("""
        WITH valid_responses AS (
            SELECT
                country,
                region,
                q1_democracy
            FROM survey_responses
            WHERE LOWER(completion_status) = 'completed'
              AND q1_democracy IS NOT NULL
        )

        SELECT
            country,
            region,
            COUNT(*) AS total_responses,

            ROUND(
                AVG(CAST(q1_democracy AS DOUBLE)),
                2
            ) AS average_democracy_score,

            SUM(
                CASE
                    WHEN CAST(q1_democracy AS INT) <= 2 THEN 1
                    ELSE 0
                END
            ) AS negative_perception_count,

            SUM(
                CASE
                    WHEN CAST(q1_democracy AS INT) = 3 THEN 1
                    ELSE 0
                END
            ) AS neutral_perception_count,

            SUM(
                CASE
                    WHEN CAST(q1_democracy AS INT) >= 4 THEN 1
                    ELSE 0
                END
            ) AS positive_perception_count,

            ROUND(
                (
                    SUM(CASE WHEN CAST(q1_democracy AS INT) >= 4 THEN 1 ELSE 0 END) * 100.0
                    -
                    SUM(CASE WHEN CAST(q1_democracy AS INT) <= 2 THEN 1 ELSE 0 END) * 100.0
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS democracy_index,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM valid_responses
        GROUP BY
            country,
            region
        ORDER BY
            average_democracy_score DESC
    """)


def build_corruption_perception(spark):
    """
    Build corruption perception metrics.

    Required temp view:
    - survey_responses

    Output:
    - corruption_perception_gold
    """

    return spark.sql("""
        WITH valid_responses AS (
            SELECT
                country,
                region,
                q4_corruption
            FROM survey_responses
            WHERE LOWER(completion_status) = 'completed'
              AND q4_corruption IS NOT NULL
        )

        SELECT
            country,
            region,
            COUNT(*) AS total_responses,

            ROUND(
                AVG(CAST(q4_corruption AS DOUBLE)),
                2
            ) AS average_corruption_score,

            SUM(
                CASE
                    WHEN CAST(q4_corruption AS INT) <= 2 THEN 1
                    ELSE 0
                END
            ) AS low_corruption_count,

            SUM(
                CASE
                    WHEN CAST(q4_corruption AS INT) = 3 THEN 1
                    ELSE 0
                END
            ) AS medium_corruption_count,

            SUM(
                CASE
                    WHEN CAST(q4_corruption AS INT) >= 4 THEN 1
                    ELSE 0
                END
            ) AS high_corruption_count,

            ROUND(
                SUM(CASE WHEN CAST(q4_corruption AS INT) >= 4 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0),
                2
            ) AS corruption_index,

            CASE
                WHEN ROUND(AVG(CAST(q4_corruption AS DOUBLE)), 2) >= 4 THEN 'Very High'
                WHEN ROUND(AVG(CAST(q4_corruption AS DOUBLE)), 2) >= 3 THEN 'High'
                WHEN ROUND(AVG(CAST(q4_corruption AS DOUBLE)), 2) >= 2 THEN 'Medium'
                ELSE 'Low'
            END AS corruption_level,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM valid_responses
        GROUP BY
            country,
            region
        ORDER BY
            corruption_index DESC
    """)


def build_election_fairness(spark):
    """
    Build election fairness metrics.

    Required temp view:
    - survey_responses

    Output:
    - election_fairness_gold
    """

    return spark.sql("""
        WITH valid_responses AS (
            SELECT
                country,
                region,
                q5_election_fairness
            FROM survey_responses
            WHERE LOWER(completion_status) = 'completed'
              AND q5_election_fairness IS NOT NULL
        )

        SELECT
            country,
            region,
            COUNT(*) AS total_responses,

            ROUND(
                AVG(CAST(q5_election_fairness AS DOUBLE)),
                2
            ) AS average_fairness_score,

            SUM(
                CASE
                    WHEN CAST(q5_election_fairness AS INT) <= 2 THEN 1
                    ELSE 0
                END
            ) AS unfair_election_count,

            SUM(
                CASE
                    WHEN CAST(q5_election_fairness AS INT) = 3 THEN 1
                    ELSE 0
                END
            ) AS neutral_election_count,

            SUM(
                CASE
                    WHEN CAST(q5_election_fairness AS INT) >= 4 THEN 1
                    ELSE 0
                END
            ) AS fair_election_count,

            ROUND(
                (
                    SUM(CASE WHEN CAST(q5_election_fairness AS INT) >= 4 THEN 1 ELSE 0 END) * 100.0
                    -
                    SUM(CASE WHEN CAST(q5_election_fairness AS INT) <= 2 THEN 1 ELSE 0 END) * 100.0
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS fairness_index,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM valid_responses
        GROUP BY
            country,
            region
        ORDER BY
            average_fairness_score DESC
    """)


def build_population_coverage(spark):
    """
    Build population coverage metrics.

    Required temp views:
    - survey_responses
    - world_bank_population

    Output:
    - population_coverage_gold
    """

    return spark.sql("""
        WITH survey_country_counts AS (
            SELECT
                country,
                COUNT(*) AS total_responses
            FROM survey_responses
            WHERE LOWER(completion_status) = 'completed'
            GROUP BY
                country
        ),

        latest_population AS (
            SELECT
                country,
                year,
                population
            FROM (
                SELECT
                    country,
                    year,
                    population,
                    ROW_NUMBER() OVER (
                        PARTITION BY LOWER(country)
                        ORDER BY year DESC
                    ) AS rn
                FROM world_bank_population
                WHERE population IS NOT NULL
            )
            WHERE rn = 1
        )

        SELECT
            scc.country,
            lp.year,
            lp.population,
            scc.total_responses,

            ROUND(
                (scc.total_responses * 1000000.0) / NULLIF(lp.population, 0),
                2
            ) AS responses_per_million,

            CASE
                WHEN ((scc.total_responses * 1000000.0) / NULLIF(lp.population, 0)) >= 500 THEN 'Very High'
                WHEN ((scc.total_responses * 1000000.0) / NULLIF(lp.population, 0)) >= 100 THEN 'High'
                WHEN ((scc.total_responses * 1000000.0) / NULLIF(lp.population, 0)) >= 50 THEN 'Medium'
                WHEN ((scc.total_responses * 1000000.0) / NULLIF(lp.population, 0)) >= 10 THEN 'Low'
                ELSE 'Very Low'
            END AS coverage_band,

            CURRENT_TIMESTAMP() AS gold_processed_at,
            'silver' AS source_layer,
            'gold' AS target_layer

        FROM survey_country_counts scc
        LEFT JOIN latest_population lp
            ON LOWER(scc.country) = LOWER(lp.country)
        ORDER BY
            responses_per_million DESC
    """)