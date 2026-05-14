"""
ingestion/csv_ingestion.py
CSV Ingestion Pipeline for AfroSurvey Intelligence Platform
Orchestrates the full ingestion flow using the shared ingestion_utils engine.
"""

from pathlib import Path
from typing import List, Optional, Dict
import time
import pyreadstat
import pandas as pd

from ingestion.ingestion_utils import (
    update_pipeline_run,
    validate_file_exists,
    validate_file_extension,
    generate_file_hash,
    is_already_processed,
    generate_bronze_object_path,
    upload_file_to_minio,
    insert_processed_file,
    insert_pipeline_run,
    log_ingestion_event,
    get_config,
    validate_required_columns,
)

from utils.logger import get_logger, log_structured

logger = get_logger(__name__)



# 1. DISCOVERY
def discover_csv_files(base_dir: str = "data/raw") -> List[Path]:
    """
    Recursively discover all CSV and SAV files in data/raw/ and its subfolders.
    Returns:
        List[Path]: List of Path objects for all discovered files.
    """
    base_path = Path(base_dir)
    if not base_path.exists():
        log_structured(logger, "warning", "Raw data directory not found", path=str(base_path))
        return []
    # Find all .csv and .sav files recursively
    files = list(base_path.rglob("*.csv")) + list(base_path.rglob("*.sav"))
    log_structured(logger,"info",f"Discovered raw files",total_files=len(files),base_dir=str(base_path))
    return sorted(files)  


# 2. METADATA EXTRACTION
def extract_metadata_from_path(file_path: Path) -> Dict:
    """
    Extract useful metadata from file path for logging and processing.
    Example:
        data/raw/nigeria/nigeria_survey_batch_001.csv
        → {"country": "Nigeria", "source_system": "csv", "filename": "..."}
    """
    try:
        parts = file_path.parts
        country = None
        
        # Look for country name in path
        for part in parts:
            for c in get_config().get("countries", []):
                if c.lower() in part.lower():
                    country = c
                    break
            if country:
                break
        
        metadata = {
            "country": country or "unknown",
            "source_system": "csv",
            "filename": file_path.name,
            "file_path": str(file_path),
        }
        
        log_structured(logger,"debug","Extracted metadata from path",**metadata)
        return metadata

    except Exception as e:
        log_structured(logger,"error","Failed to extract metadata from path",file_path=str(file_path),error=str(e))
        return {"country": "unknown", "source_system": "csv", "filename": file_path.name}


# 3. LOADING
def load_csv_file(file_path: Path) -> pd.DataFrame:
    """
    Safely load CSV or SAV file into pandas DataFrame.
    Handles common encoding and delimiter issues.
    """
    try:
        path_str = str(file_path)
        
        if path_str.lower().endswith('.sav'):
            import pyreadstat
            df, meta = pyreadstat.read_sav(path_str)
        else:
            df = pd.read_csv(path_str,encoding="utf-8",on_bad_lines='skip',low_memory=False)

        #Empty DataFrame Check
        if df.empty:
            log_structured(logger,"warning","Loaded file is empty",
                file_path=path_str)
            return pd.DataFrame() 
        log_structured(logger, "info", "Loaded file successfully", file_path=path_str, rows=len(df))
        return df

    except UnicodeDecodeError:
        df = pd.read_csv(path_str, encoding="latin1", on_bad_lines='skip', low_memory=False)
        log_structured(logger, "warning", "Loaded CSV with latin1 encoding", file_path=path_str)
        return df
    except Exception as e:
        log_structured(logger,"error","Failed to load file",file_path=str(file_path),
            error=str(e))
        raise

# 4. SCHEMA VALIDATION
def validate_schema(df: pd.DataFrame) -> bool:
    """
    Validate DataFrame against the canonical survey schema.
    Uses the data contract from survey_schema.yaml.
    """
    config = get_config()
    schema = config.get("survey_schema", {})
    fields = schema.get("fields", {})

    # Safely get required columns
    required_columns = [field for field, spec in fields.items() 
        if isinstance(spec, dict) and spec.get("required", False)]
    if not required_columns:
        log_structured(logger, "warning", "No required columns defined in schema")
        return True  # Don't block if schema is incomplete

    # Use helper from ingestion_utils
    if not validate_required_columns(df, required_columns):
        return False
    log_structured(logger,"info","Schema validation passed",
        rows=len(df),required_columns=len(required_columns))
    return True

# 5. SINGLE FILE PROCESSING (Core Orchestrator)
def process_single_csv(file_path: Path) -> bool:
    """
    Orchestrate the complete ingestion of ONE CSV file.
    This is the heart of the CSV ingestion pipeline.
    """
    start_time = time.time()
    run_id = None
    country = None

    try:
        metadata = extract_metadata_from_path(file_path)
        country = metadata["country"]

        log_structured(
            logger,
            "info",
            "Starting processing of single CSV file",
            file_path=str(file_path),
            country=country
        )

        if not validate_file_exists(file_path):
            log_structured(logger, "error", "File validation failed - file does not exist", file_path=str(file_path))
            return False

        if not validate_file_extension(file_path):
            log_structured(logger, "error", "File validation failed - invalid extension", file_path=str(file_path))
            return False

        df = load_csv_file(file_path)

        if df.empty:
            log_structured(logger, "warning", "Skipping empty file", file_path=str(file_path), country=country)
            return False

        if not validate_schema(df):
            log_structured(logger, "error", "Schema validation failed", file_path=str(file_path), country=country)
            return False

        file_hash = generate_file_hash(str(file_path))

        log_structured(
            logger,
            "info",
            "File hash generated",
            file_path=str(file_path),
            file_hash=file_hash[:16]
        )

        if is_already_processed(str(file_path), file_hash):
            log_structured(
                logger,
                "warning",
                "File already processed - skipping upload",
                file_path=str(file_path),
                country=country
            )
            return True

        object_name = generate_bronze_object_path(
            country=country,
            source_system=metadata["source_system"],
            original_filename=str(file_path)
        )

        bronze_bucket = get_config()["storage"]["buckets"]["bronze"]

        log_structured(
            logger,
            "info",
            "Uploading file to MinIO Bronze",
            file_path=str(file_path),
            bucket=bronze_bucket,
            object_name=object_name,
            country=country
        )

        upload_success = upload_file_to_minio(
            local_file_path=str(file_path),
            bucket_name=bronze_bucket,
            object_name=object_name
        )

        if not upload_success:
            log_structured(
                logger,
                "error",
                "Upload to MinIO failed",
                file_path=str(file_path),
                bucket=bronze_bucket,
                object_name=object_name,
                country=country
            )
            return False

        log_structured(
            logger,
            "info",
            "Upload to MinIO succeeded",
            file_path=str(file_path),
            bucket=bronze_bucket,
            object_name=object_name,
            country=country
        )

        insert_processed_file(
            file_name=str(file_path),
            file_hash=file_hash,
            country=country,
            source_system=metadata["source_system"],
            rows_loaded=len(df)
        )

        duration = time.time() - start_time

        log_ingestion_event(
            pipeline_name="csv_ingestion",
            status="success",
            country=country,
            rows_processed=len(df),
            duration_seconds=round(duration, 2),
            run_id=run_id
        )

        log_structured(
            logger,
            "info",
            "Successfully processed CSV file",
            file_path=str(file_path),
            country=country,
            rows=len(df),
            duration_seconds=round(duration, 2)
        )

        return True

    except Exception as e:
        duration = time.time() - start_time

        log_ingestion_event(
            pipeline_name="csv_ingestion",
            status="failed",
            country=country,
            duration_seconds=round(duration, 2),
            error_message=str(e)
        )

        log_structured(
            logger,
            "error",
            "Failed to process CSV file",
            file_path=str(file_path),
            country=country,
            error=str(e),
            duration_seconds=round(duration, 2)
        )

        return False
    
# 6. BATCH PROCESSING
def process_files_batch(files: List[Path]) -> int:
    """
    Process a batch of files with proper error isolation.
    Each file fails independently without stopping the entire batch.
    """
    processed_count = 0
    failed_count = 0

    for file_path in files:
        try:
            success = process_single_csv(file_path)
            if success:
                processed_count += 1
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            log_structured(logger,"error","Unexpected error processing file in batch",
                file_path=str(file_path),error=str(e))

    log_structured(logger,"info","Batch processing completed",total_files=len(files),successful=processed_count,
        failed=failed_count)
    return processed_count


# 7. COUNTRY-LEVEL PROCESSING (Convenience Wrapper)
def process_country_folder(country_folder: Path) -> int:
    """
    Process all CSV files inside one country folder.
    Convenience wrapper around batch processing.
    """
    files = discover_csv_files(str(country_folder))
    
    if not files:
        log_structured(logger,"info","No files found in country folder",
            country_folder=str(country_folder))
        return 0

    log_structured(logger,"info","Starting country folder processing",country_folder=str(country_folder),
        files_found=len(files))
    return process_files_batch(files)


# 8. MAIN PIPELINE RUNNER (Full Orchestrator)
def run_csv_ingestion_pipeline() -> int:
    """
    Full CSV Ingestion Pipeline Orchestrator.
    Uses insert_pipeline_run() at start and update_pipeline_run() at end.
    """
    start_time = time.time()
    run_id = None

    try:
        # 1. Create initial pipeline run record
        run_id = insert_pipeline_run(dag_id="csv_ingestion",pipeline_name="csv_ingestion",
            run_type="full",
            status="running")

        log_structured(logger,"info","Starting full CSV ingestion pipeline",run_id=run_id)

        # Discover all raw files
        all_files = discover_csv_files()

        if not all_files:
            log_structured(logger, "warning", "No raw CSV files found to process")
            # Update run as completed with zero files
            update_pipeline_run(run_id=run_id,status="success",rows_processed=0)
            return 0

        # Process files in batch
        processed_count = process_files_batch(all_files)

        duration = time.time() - start_time

        # 2. Update pipeline run with final status
        update_pipeline_run(run_id=run_id,status="success",rows_processed=processed_count,duration_seconds=round(duration, 2))

        log_structured(logger,"info","CSV ingestion pipeline completed successfully",
            run_id=run_id,
            files_processed=processed_count,
            duration_seconds=round(duration, 2))

        return processed_count

    except Exception as e:
        duration = time.time() - start_time
        log_structured(logger,"error","CSV ingestion pipeline failed",run_id=run_id,
            error=str(e),
            duration_seconds=round(duration, 2))
        # Update run as failed
        if run_id:
            update_pipeline_run(
                run_id=run_id,
                status="failed",
                error_message=str(e),
                duration_seconds=round(duration, 2)
            )
        raise



# ENTRY POINT
if __name__ == "__main__":
    print("Starting CSV Ingestion Pipeline...\n")
    
    start = time.time()
    
    processed = run_csv_ingestion_pipeline()
    
    duration = time.time() - start
    
    print(f"CSV ingestion completed!")
    print(f"   Files processed : {processed}")
    print(f"   Duration        : {duration:.2f} seconds")