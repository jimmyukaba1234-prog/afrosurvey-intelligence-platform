"""
ingestion/ingestion_utils.py
Reusable Ingestion Framework for AfroSurvey Intelligence Platform
This is the shared engine used by all ingestion modules (CSV, API, DB).
"""

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional
import time
import psycopg2
from minio import Minio
from minio.error import S3Error

from utils.config import load_data_contract
from utils.config import load_config
from utils.logger import get_logger, log_structured

logger = get_logger(__name__)
def get_config():
    """Return fresh config on every call (better for Airflow, testing, and distributed systems)."""
    return load_config()


# CONFIG & CONNECTIONS
def get_minio_client() -> Minio:
    """Create and return a configured MinIO client"""
    minio_conf = get_config()["storage"]["minio"]
    client = Minio(
        endpoint=minio_conf["endpoint"].replace("http://", ""),
        access_key=minio_conf["access_key"],
        secret_key=minio_conf["secret_key"],
        secure=minio_conf["secure"]
    )
    return client

def get_pg_connection():
    """Create and return a PostgreSQL connection"""
    config = get_config()
    db_conf = config["database"]
    conn = psycopg2.connect(
        host=db_conf["host"],
        port=db_conf["port"],
        database=db_conf["database"],
        user=db_conf["user"],
        password=db_conf["password"]
    )
    return conn



# MINIO HELPERS
def create_bucket_if_missing(bucket_name: str) -> bool:
    """Create bucket if it doesn't exist."""
    client = get_minio_client()
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            log_structured(
                logger,
                "info",
                "Created new MinIO bucket",
                bucket_name=bucket_name
            )
            return True
        return False

    except S3Error as e:
        log_structured(logger,"error","MinIO S3 error occurred",
            bucket_name=bucket_name,
            error=str(e)
        )
        raise
    except Exception as e:
        log_structured(logger,"error",
            "Unexpected error in MinIO operation",
            bucket_name=bucket_name,
            error=str(e)
        )
        raise


def upload_file_to_minio(
    local_file_path: str,
    bucket_name: str,
    object_name: str
) -> bool:
    """
    Upload a file to MinIO Bronze layer.
    Returns True if successful.
    """
    try:
        client = get_minio_client()

        # Create bucket if missing
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            log_structured(
                logger,
                "info",
                "Created new MinIO bucket",
                bucket_name=bucket_name
            )

        # Upload file
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=local_file_path
        )

        log_structured(
            logger,
            "info",
            "File uploaded to MinIO",
            pipeline="ingestion",
            bucket=bucket_name,
            object_name=object_name,
            local_file=local_file_path
        )

        return True

    except S3Error as e:
        log_structured(
            logger,
            "error",
            "MinIO S3 error occurred during upload",
            bucket_name=bucket_name,
            object_name=object_name,
            local_file=local_file_path,
            error=str(e)
        )
        return False

    except Exception as e:
        log_structured(
            logger,
            "error",
            "Unexpected error during MinIO upload",
            bucket_name=bucket_name,
            object_name=object_name,
            local_file=local_file_path,
            error=str(e)
        )
        return False
    
# FILE HASHING HELPERS
def generate_file_hash(file_path: str, algorithm: str = "sha256") -> str:
    """
    Generate a cryptographic hash of a file for idempotency and change detection.

    Args:
        file_path (str): Path to the file to hash
        algorithm (str): Hash algorithm ('md5', 'sha256' - sha256 recommended)

    Returns:
        str: Hexadecimal hash string
    """
    hash_func = hashlib.sha256() if algorithm == "sha256" else hashlib.md5()

    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        file_hash = hash_func.hexdigest()
        
        log_structured(
            logger,
            "debug",
            "File hash generated",
            file_path=file_path,
            algorithm=algorithm,
            file_hash=file_hash[:16] + "..."  # Truncated for log cleanliness
        )
        return file_hash

    except Exception as e:
        log_structured(
            logger,
            "error",
            "Failed to generate file hash",
            file_path=file_path,
            error=str(e)
        )
        raise


# IDEMPOTENCY HELPERS
def is_already_processed(file_name: str, file_hash: str) -> bool:
    """
    Check if a file has already been successfully processed using PostgreSQL metadata.
    This is the core of idempotency.

    Returns:
        bool: True if file was already processed successfully
    """
    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT processing_status 
                FROM processed_files 
                WHERE file_name = %s 
                  AND file_hash = %s
                  AND processing_status = 'processed'
                LIMIT 1
            """, (file_name, file_hash))
            
            result = cur.fetchone()
            
            if result:
                log_structured(
                    logger,
                    "info",
                    "File already processed - skipping (idempotency)",
                    file_name=file_name,
                    file_hash=file_hash[:16] + "..."
                )
                return True
            
            return False

    except Exception as e:
        log_structured(
            logger,
            "error",
            "Failed to check processed_files table",
            file_name=file_name,
            error=str(e)
        )
        return False  # Safe default: allow processing if check fails

    finally:
        if conn:
            conn.close()


def generate_bronze_object_path(
    country: str,
    source_system: str,
    original_filename: str,
    submission_date: Optional[str] = None
) -> str:
    """
    Generate Hive-style partitioned object path for Bronze layer.
    Format: country={country}/year={year}/month={month}/day={day}/{source_system}/{filename}
    
    This format is optimized for Spark/Hive partitioning and query performance.
    """
    if not submission_date:
        submission_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    year = submission_date[:4]
    month = submission_date[5:7]
    day = submission_date[8:10]

    clean_filename = Path(original_filename).name

    # Hive-style partitioning (best for PySpark)
    object_path = (
        f"country={country.lower()}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{source_system}/"
        f"{clean_filename}"
    )

    log_structured(
        logger,
        "debug",
        "Generated Hive-style Bronze object path",
        country=country,
        source_system=source_system,
        object_path=object_path
    )
    return object_path


# PIPELINE LOGGING HELPERS
def log_ingestion_event(
    pipeline_name: str,
    status: str,
    country: Optional[str] = None,
    rows_processed: Optional[int] = None,
    duration_seconds: Optional[float] = None,
    run_id: Optional[str] = None,
    error_message: Optional[str] = None,
    **extra
) -> None:
    """
    Centralized helper to log ingestion events with consistent structure.
    Also prepares data for insertion into pipeline_runs table.
    """

    # Better log level handling
    level_map = {
        "success": "info",
        "running": "info",
        "skipped": "warning",
        "failed": "error"
    }

    log_level = level_map.get(status, "info")

    log_structured(
        logger,
        log_level,
        f"Ingestion event: {pipeline_name} - {status}",
        pipeline=pipeline_name,
        status=status,
        country=country,
        rows_processed=rows_processed,
        duration_seconds=duration_seconds,
        run_id=run_id,
        error_message=error_message,
        **extra
    )



def insert_pipeline_run(
    dag_id: str,
    pipeline_name: str,
    run_type: str,
    country: Optional[str] = None,
    source_system: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    status: str = "running",
    rows_processed: int = 0,
    rows_failed: int = 0,
    validation_pass_rate: Optional[float] = None,
    error_message: Optional[str] = None
) -> str:
    """
    Insert a new pipeline run record and return the generated run_id.
    """
    run_id = str(uuid.uuid4())
    conn = None

    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (
                    run_id, dag_id, pipeline_name, run_type, country,
                    source_system, start_time, end_time, status,
                    rows_processed, rows_failed, validation_pass_rate, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING run_id
            """, (
                run_id, dag_id, pipeline_name, run_type, country,
                source_system, start_time, end_time, status,
                rows_processed, rows_failed, validation_pass_rate, error_message
            ))
            conn.commit()

        log_ingestion_event(
            pipeline_name=pipeline_name,
            status=status,
            country=country,
            rows_processed=rows_processed,
            run_id=run_id
        )

        return run_id

    except psycopg2.Error as e:
        log_structured(
            logger,"error",
            "PostgreSQL database error in insert_pipeline_run",
            pipeline=pipeline_name,
            error=str(e))
        if conn:conn.rollback()
        raise

    except Exception as e:
        log_structured(
            logger,"error",
            "Unexpected error in insert_pipeline_run",
            pipeline=pipeline_name,
            error=str(e))
        if conn:conn.rollback()
        raise

    finally:
        if conn:
            conn.close()


# FILE VALIDATION HELPERS
def validate_file_exists(file_path: str) -> bool:
    """Check if the source file exists before processing."""
    path = Path(file_path)
    exists = path.exists() and path.is_file()
    
    if not exists:
        log_structured(
            logger,
            "error",
            "Source file not found",
            file_path=str(file_path)
        )
    return exists


def validate_file_extension(file_path: str, allowed_extensions: tuple = (".csv", ".sav", ".json", ".parquet")) -> bool:
    """Validate that the file has an allowed extension."""
    ext = Path(file_path).suffix.lower()
    is_valid = ext in allowed_extensions
    
    if not is_valid:
        log_structured(
            logger,
            "warning",
            "Invalid file extension",
            file_path=file_path,
            extension=ext,
            allowed=allowed_extensions
        )
    return is_valid


def validate_required_columns(df, required_columns: list) -> bool:
    """
    Validate that the DataFrame contains all required columns from the data contract.
    This connects directly to survey_schema.yaml.
    """
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        log_structured(
            logger,
            "error",
            "Missing required columns",
            missing_columns=missing,
            required=required_columns
        )
        return False
    
    log_structured(
        logger,
        "info",
        "All required columns present",
        columns_checked=len(required_columns)
    )
    return True


# COMMON UTILITIES
def get_current_timestamp() -> str:
    """Return current UTC timestamp as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def extract_country_from_filename(filename: str) -> Optional[str]:
    """
    Attempt to extract country from filename (e.g., survey_ng_2026.csv → Nigeria).
    Falls back to None if not detectable.
    """
    filename_lower = filename.lower()
    for country in get_config().get("countries", []):
        if country.lower() in filename_lower or country[:2].lower() in filename_lower:
            return country
    return None


def normalize_filename(filename: str) -> str:
    """Clean and normalize filename for consistent storage."""
    path = Path(filename)
    return path.name.replace(" ", "_").lower()

# PROCESSED FILES METADATA HELPER
def insert_processed_file(
    file_name: str,
    file_hash: str,
    country: str,
    source_system: str,
    rows_loaded: int,
    batch_id: Optional[str] = None,
    processing_status: str = "processed"
) -> bool:
    """
    Insert record into processed_files table after successful ingestion.
    This completes the idempotency loop.
    """
    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO processed_files (
                    file_name, file_hash, country, source_system,
                    ingestion_timestamp, processing_status, rows_loaded, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (file_name) DO NOTHING
            """, (
                file_name,
                file_hash,
                country,
                source_system,
                datetime.now(timezone.utc),
                processing_status,
                rows_loaded,
                batch_id
            ))
            conn.commit()

        log_structured(
            logger,
            "info",
            "File marked as processed in metadata",
            file_name=file_name,
            country=country,
            rows_loaded=rows_loaded
        )
        return True

    except Exception as e:
        log_structured(
            logger,
            "error",
            "Failed to insert processed_file record",
            file_name=file_name,
            error=str(e)
        )
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# RETRY UTILITIES (Basic - will be upgraded with tenacity later)
def retry_on_failure(max_attempts: int = 3, delay_seconds: int = 2):
    """
    Simple retry decorator for transient failures (network, MinIO, DB).
    To be used on critical ingestion functions.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    log_structured(
                        logger,
                        "warning",
                        f"Attempt {attempt}/{max_attempts} failed, retrying...",
                        function=func.__name__,
                        error=str(e)
                    )
                    if attempt < max_attempts:
                        time.sleep(delay_seconds)

            log_structured(logger,"error","All retry attempts failed",function=func.__name__,error=str(last_exception))
            #logger.error("All retry attempts failed", function=func.__name__, error=str(last_exception))
            raise last_exception
        return wrapper
    return decorator


# UPDATE PIPELINE RUN (Critical for status updates)
def update_pipeline_run(
    run_id: str,
    status: str,
    rows_processed: int = 0,
    rows_failed: int = 0,
    duration_seconds: Optional[float] = None,
    validation_pass_rate: Optional[float] = None,
    error_message: Optional[str] = None,
    end_time: Optional[datetime] = None
) -> bool:
    """
    Update an existing pipeline run record with final status and metrics.
    This completes the pipeline tracking lifecycle.
    """
    conn = None

    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pipeline_runs
                SET
                    status = %s,
                    rows_processed = %s,
                    rows_failed = %s,
                    validation_pass_rate = %s,
                    error_message = %s,
                    end_time = COALESCE(%s, CURRENT_TIMESTAMP),
                    duration_seconds = COALESCE(
                        %s,
                        EXTRACT(EPOCH FROM (COALESCE(%s, CURRENT_TIMESTAMP) - start_time))
                    )
                WHERE run_id = %s
            """, (
                status,
                rows_processed,
                rows_failed,
                validation_pass_rate,
                error_message,
                end_time,
                duration_seconds,
                end_time,
                run_id
            ))

            conn.commit()

        log_structured(logger,"info","Pipeline run updated",
            run_id=run_id,
            status=status,
            rows_processed=rows_processed,
            duration_seconds=duration_seconds
        )

        return True

    except Exception as e:
        log_structured(logger,"error","Failed to update pipeline run",
            run_id=run_id,
            status=status,
            error=str(e))
        if conn:conn.rollback()
        return False
    finally:
        if conn:
            conn.close()