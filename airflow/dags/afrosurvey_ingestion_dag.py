from datetime import datetime, timedelta
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# =========================
# Project Path
# =========================

PROJECT_ROOT = Path("/opt/airflow")
sys.path.insert(0, str(PROJECT_ROOT))


# =========================
# Default Args
# =========================

default_args = {
    "owner": "afrosurvey",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# =========================
# Python Callables
# =========================

def run_csv_ingestion():
    from ingestion.csv_ingestion import run_csv_ingestion_pipeline
    return run_csv_ingestion_pipeline()


def run_api_ingestion():
    from ingestion.api_ingestion import run_api_ingestion_pipeline
    return run_api_ingestion_pipeline()


def load_survey_data_to_postgres():
    from scripts.load_survey_data_to_postgres import main
    return main()


# =========================
# DAG Definition
# =========================

with DAG(
    dag_id="afrosurvey_end_to_end_pipeline",
    description="Orchestrates ingestion, Bronze to Silver, and Silver to Gold",
    default_args=default_args,
    start_date=datetime(2026, 5, 14),
    schedule=None,
    catchup=False,
    tags=["afrosurvey", "bronze", "silver", "gold", "spark"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    load_postgres_source = PythonOperator(
        task_id="load_survey_data_to_postgres",
        python_callable=load_survey_data_to_postgres,
    )

    csv_ingestion = PythonOperator(
        task_id="csv_ingestion_to_bronze",
        python_callable=run_csv_ingestion,
    )

    api_ingestion = PythonOperator(
        task_id="api_ingestion_to_bronze",
        python_callable=run_api_ingestion,
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_spark_job",
        bash_command="""
        docker exec afrosurvey-spark bash -c '
        mkdir -p /tmp/.ivy2 &&

        export PYTHONPATH=/opt/afrosurvey &&

        /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --conf spark.executorEnv.PYTHONPATH=/opt/afrosurvey \
          --conf spark.driverEnv.PYTHONPATH=/opt/afrosurvey \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /opt/afrosurvey/spark_jobs/bronze_to_silver.py
        '
        """,
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_spark_job",
        bash_command="""
        docker exec afrosurvey-spark bash -c '
        mkdir -p /tmp/.ivy2 &&

        export PYTHONPATH=/opt/afrosurvey:/opt/afrosurvey/spark_jobs &&

        /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --conf spark.executorEnv.PYTHONPATH=/opt/afrosurvey:/opt/afrosurvey/spark_jobs \
          --conf spark.driverEnv.PYTHONPATH=/opt/afrosurvey:/opt/afrosurvey/spark_jobs \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /opt/afrosurvey/spark_jobs/silver_to_gold.py
        '
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    # =========================
    # Task Dependencies
    # =========================

    start >> load_postgres_source >> [csv_ingestion, api_ingestion]
    [csv_ingestion, api_ingestion] >> bronze_to_silver >> silver_to_gold >> end