
"""
ingestion/db_ingestion.py
Database Ingestion Pipeline for AfroSurvey Intelligence Platform
Extracts data from PostgreSQL operational tables and lands raw snapshots into the Bronze layer.
"""

from pathlib import Path
from typing import List, Dict
from datetime import datetime,timezone
import pandas as pd
from sqlalchemy import create_engine
import time


from ingestion.ingestion_utils import (
    generate_file_hash,
    is_already_processed,
    generate_bronze_object_path,
    upload_file_to_minio,
    insert_processed_file,
    insert_pipeline_run,
    validate_required_columns,
    update_pipeline_run,
    log_ingestion_event,
    get_config,
)


from utils.logger import get_logger, log_structured

logger = get_logger(__name__)

# 1. DATABASE SOURCE REGISTRY
def get_db_sources() -> List[Dict]:
    """
    Central registry of PostgreSQL tables to ingest.
    Easy to extend with new tables in the future.
    """
    return [
        {
            "name": "survey_responses",
            "table_name": "survey_responses",
            "source_system": "postgres_operational",
            "description": "Main synthetic survey data table"
        }
    ]

def extract_table_data(table_name: str) -> pd.DataFrame:
    """
    Extract full table from PostgreSQL into pandas DataFrame.
    Uses table name whitelist to prevent SQL injection.
    """
    allowed_tables = {"survey_responses", "afrobarometer_survey_data"}
    if table_name not in allowed_tables:
        raise ValueError(f"Table '{table_name}' is not in the allowed list for safety.")

    try:
        config = get_config()
        db_conf = config["database"]

        connection_string = (
            f"postgresql+psycopg2://"
            f"{db_conf['user']}:{db_conf['password']}"
            f"@{db_conf['host']}:{db_conf['port']}"
            f"/{db_conf['database']}")

        engine = create_engine(connection_string)

        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        log_structured(logger,"info","Extracted table from PostgreSQL",
            table=table_name,
            rows=len(df))
        return df
    except Exception as e:
        log_structured(logger,"error","Failed to extract table from PostgreSQL",
            table=table_name,
            error=str(e))
        raise


# 3. RAW SNAPSHOT STORAGE
def save_raw_db_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    """
    Save extracted table as raw Parquet snapshot for audit and reproducibility.
    Parquet is more efficient and Spark-friendly than CSV.
    """
    snapshot_dir = Path("data/raw/db/postgres")
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{table_name}_{timestamp}.parquet"
    file_path = snapshot_dir / filename

    # Save as Parquet for better performance and schema preservation
    df.to_parquet(file_path, index=False)

    log_structured(logger,"info","Saved raw DB snapshot as Parquet",
        table=table_name,
        rows=len(df),
        file_path=str(file_path))

    return file_path

# 4. SCHEMA VALIDATION
def validate_db_schema(df: pd.DataFrame, table_name: str) -> bool:
    """
    Validate extracted database table against the appropriate data contract.
    For now, it uses the main survey_schema.yaml (can be extended per table).
    """
    if df.empty:
        log_structured(logger, "warning", "Empty DataFrame from DB table", table=table_name)
        return False

    config = get_config()
    schema = config.get("survey_schema", {})
    fields = schema.get("fields", {})

    required_columns = [
        field for field, spec in fields.items()
        if isinstance(spec, dict) and spec.get("required", False)
    ]

    if not required_columns:
        log_structured(logger, "warning", "No required columns defined in schema", table=table_name)
        return True
    if not validate_required_columns(df, required_columns):
        log_structured(logger,
            "error",
            "Schema validation failed for DB table",
            table=table_name)
        return False
    log_structured(logger,"info","DB table passed schema validation",
        table=table_name,
        rows=len(df))
    return True


# 5. SINGLE TABLE PROCESSING (Core Orchestrator)
def process_single_table(source_config: Dict) -> bool:
    """
    Orchestrate the complete ingestion of ONE database table.
    This is the heart of the DB ingestion pipeline.
    """
    start_time = time.time()
    run_id = None
    table_name = source_config.get("table_name")

    try:
        log_structured(logger,"info", "Starting DB table ingestion",
            table=table_name)

        # 1. Start pipeline run tracking
        run_id = insert_pipeline_run(
            dag_id="db_ingestion",
            pipeline_name=f"db_{table_name}",
            run_type="db",
            status="running")

        # 2. Extract data from PostgreSQL
        df = extract_table_data(table_name)

        # 3. Schema validation
        if not validate_db_schema(df, table_name):
            update_pipeline_run(run_id=run_id, status="failed")
            return False

        # 4. Save raw snapshot locally
        local_file = save_raw_db_snapshot(df, table_name)

        # 5. Generate hash for idempotency
        file_hash = generate_file_hash(str(local_file))

        # 6. Idempotency check
        if is_already_processed(str(local_file), file_hash):
            update_pipeline_run(run_id=run_id, status="skipped")
            return True

        # 7. Generate Bronze object path
        object_name = generate_bronze_object_path(
            country="global",  # DB data is usually global/multi-country
            source_system=source_config.get("source_system", "postgres"),
            original_filename=local_file.name)

        # 8. Upload to MinIO Bronze
        upload_success = upload_file_to_minio(
            local_file_path=str(local_file),
            bucket_name=get_config()["storage"]["buckets"]["bronze"],
            object_name=object_name)

        if not upload_success:
            update_pipeline_run(run_id=run_id, status="failed")
            return False

        # 9. Record in metadata
        insert_processed_file(
            file_name=str(local_file),
            file_hash=file_hash,
            country="global",
            source_system=source_config.get("source_system", "postgres"),
            rows_loaded=len(df))

        # 10. Update pipeline run as successful
        duration = time.time() - start_time
        update_pipeline_run(
            run_id=run_id,
            status="success",
            rows_processed=len(df),
            duration_seconds=round(duration, 2))

        log_structured(logger,"info","Successfully processed DB table",
            table=table_name,
            rows=len(df))
        return True

    except Exception as e:
        duration = time.time() - start_time
        log_structured(logger,"error","Failed to process DB table",
            table=table_name,
            error=str(e))
        if run_id:
            update_pipeline_run(run_id=run_id,status="failed",
                error_message=str(e),
                duration_seconds=round(duration, 2))
        return False
    
# 6. BATCH PROCESSING
def process_db_batch(sources: List[Dict]) -> int:
    """
    Process multiple database tables with proper error isolation.
    Each table fails independently without stopping the entire batch.
    """
    processed_count = 0
    failed_count = 0

    for source_config in sources:
        try:
            success = process_single_table(source_config)
            if success:
                processed_count += 1
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            log_structured(logger,"error",
                "Unexpected error processing DB table in batch",
                table=source_config.get("table_name"),
                error=str(e))

    log_structured(logger,"info",
        "DB batch processing completed",
        total_tables=len(sources),
        successful=processed_count,
        failed=failed_count)
    return processed_count

# 7. MAIN PIPELINE RUNNER (Full Orchestrator)
def run_db_ingestion_pipeline() -> int:
    """
    Full Database Ingestion Pipeline Orchestrator.
    Entry point for manual runs or Airflow.
    Uses insert_pipeline_run() at start and update_pipeline_run() at end.
    """
    start_time = time.time()
    run_id = None

    try:
        # Start pipeline run tracking
        run_id = insert_pipeline_run(
            dag_id="db_ingestion",
            pipeline_name="db_ingestion",
            run_type="full",
            status="running")

        log_structured(logger,"info",
            "Starting full DB ingestion pipeline",
            run_id=run_id)

        # Get configured database sources
        sources = get_db_sources()

        if not sources:
            log_structured(logger, "warning", "No database sources configured")
            update_pipeline_run(run_id=run_id, status="success", rows_processed=0)
            return 0

        # Process all tables
        processed_count = process_db_batch(sources)

        duration = time.time() - start_time

        # Update pipeline run as successful
        update_pipeline_run(
            run_id=run_id,
            status="success",
            rows_processed=processed_count,
            duration_seconds=round(duration, 2))

        log_structured(logger, "info",
            "DB ingestion pipeline completed successfully",
            run_id=run_id,
            tables_processed=processed_count,
            duration_seconds=round(duration, 2))

        return processed_count

    except Exception as e:
        duration = time.time() - start_time
        log_structured(logger,"error",
            "DB ingestion pipeline failed",
            run_id=run_id,
            error=str(e),
            duration_seconds=round(duration, 2))
        
        if run_id:
            update_pipeline_run(
                run_id=run_id,
                status="failed",
                error_message=str(e),
                duration_seconds=round(duration, 2)
            )
        raise


# ENTRY POINT / TEST BLOCK
if __name__ == "__main__":
    print("🚀 Starting Database Ingestion Pipeline...\n")
    
    start_time = time.time()
    
    processed = run_db_ingestion_pipeline()
    
    duration = time.time() - start_time
    
    print(f"\n✅ Database Ingestion Pipeline Completed!")
    print(f"   Tables processed : {processed}")
    print(f"   Total duration   : {duration:.2f} seconds")