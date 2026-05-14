"""
ingestion/api_ingestion.py
API Ingestion Pipeline for AfroSurvey Intelligence Platform
Handles multiple external APIs (World Bank, REST Countries, WHO GHO, etc.)
Uses countries from central config.yaml
"""
import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from pathlib import Path
from typing import List, Dict, Optional, Any
import time
import requests
import pandas as pd
from datetime import datetime, timezone
import json
from utils.config import load_data_contract

from ingestion.ingestion_utils import (
    generate_file_hash,
    is_already_processed,
    generate_bronze_object_path,
    upload_file_to_minio,
    insert_processed_file,
    insert_pipeline_run,
    update_pipeline_run,
    log_ingestion_event,
    get_config,
    validate_required_columns,
)

from utils.logger import get_logger, log_structured
from utils.config import load_config, load_yaml

logger = get_logger(__name__)
config = load_config()

# 1. API REGISTRY / CONFIG
def get_api_sources() -> List[Dict]:
    """
    Central registry of APIs to ingest.
    Countries are pulled from central config.yaml (no hardcoding).
    """
    config = get_config()
    countries = config.get("countries", ["Nigeria", "Kenya", "Ghana"])

    return [
        {
            "name": "world_bank",
            "base_url": "https://api.worldbank.org/v2",
            "description": "World Bank Open Data API",
            "countries": countries,         
            "indicator": "SP.POP.TOTL",
            "params": {"format": "json"}
        },
        {
            
            "name": "restcountries",
            "base_url": "https://restcountries.com/v3.1",
            "description": "REST Countries API",
            "endpoint": "/all?fields=name,region,subregion,population,area",
            "params": {}
        },
        {
            "name": "who_gho",
                        
            "base_url":"https://ghoapi.azureedge.net/api",
            "description": "WHO Global Health Observatory OData API",
            "endpoint": "/Indicator",
            "params": {}
        }
    ]


# 2. GENERIC API FETCH HELPER
def fetch_api_data(url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) -> Dict:
    """
    Generic robust API fetcher with basic retry and timeout handling.
    """
    params = params or {}
    headers = headers or {"User-Agent": "AfroSurvey-Ingestion/1.0"}

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url,params=params,headers=headers,timeout=30)
            response.raise_for_status()
            log_structured(
                logger,"info","API request successful",
                url=url,status_code=response.status_code)
            return response.json()

        except requests.exceptions.RequestException as e:
            log_structured(logger,"warning",
                f"API request failed (attempt {attempt}/{max_retries})",
                url=url,error=str(e))
            if attempt == max_retries:
                log_structured(logger,
                    "error","API request failed after all retries",
                    url=url,error=str(e))
                raise
            time.sleep(2 ** attempt)  



# 3. RAW JSON STORAGE
def save_raw_api_response(raw_data: Any, source_name: str, country: Optional[str] = None) -> Path:
    """
    Save raw API response as JSON file for audit, reproducibility, and debugging.
    """
    RAW_API_DIR = Path("data/raw/api") / source_name
    RAW_API_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    country_part = f"_{country.lower()}" if country else ""
    filename = f"{source_name}{country_part}_{timestamp}.json"

    file_path = RAW_API_DIR / filename

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=2)

    log_structured(
        logger,
        "info",
        "Saved raw API response",
        source=source_name,
        country=country,
        file_path=str(file_path)
    )

    return file_path


# 4. PER-API NORMALIZERS
def normalize_world_bank_data(raw_data: Dict) -> pd.DataFrame:
    """
    Normalize World Bank API response into a clean tabular DataFrame.
    """
    try:
        if isinstance(raw_data, list) and len(raw_data) > 1:
            records = raw_data[1]
        else:
            records = raw_data

        normalized_rows = []

        for row in records:
            normalized_rows.append({
                "country": row.get("country", {}).get("value"),
                "country_code": row.get("countryiso3code"),
                "year": int(row.get("date")) if row.get("date") else None,
                "population": row.get("value"),
                "indicator": row.get("indicator", {}).get("value"),
                "source_system": "world_bank",
                "ingestion_timestamp": datetime.utcnow()
            })

        df = pd.DataFrame(normalized_rows)

        log_structured(
            logger,
            "info",
            "Normalized World Bank data",
            rows=len(df)
        )

        return df

    except Exception as e:
        log_structured(
            logger,
            "error",
            "Failed to normalize World Bank data",
            error=str(e)
        )
        return pd.DataFrame()
    
def normalize_restcountries_data(raw_data: Dict) -> pd.DataFrame:
    """
    Normalize REST Countries API response into a clean tabular DataFrame.
    Handles nested country name structure correctly.
    """
    try:
        df = pd.DataFrame(raw_data)

        if not df.empty:

            # Extract common country name
            df["country"] = df["name"].apply(
                lambda x: x.get("common") if isinstance(x, dict) else x
            )

            # Extract ISO3 country code
            if "cca3" in df.columns:
                df["country_code"] = df["cca3"]
            else:
                df["country_code"] = None

            # Add ingestion metadata
            df["source_system"] = "restcountries"
            df["ingestion_timestamp"] = datetime.utcnow()

            # Keep only required/useful columns
            df = df[
                [
                    "country",
                    "country_code",
                    "region",
                    "subregion",
                    "population",
                    "area",
                    "source_system",
                    "ingestion_timestamp"
                ]
            ].copy()

        log_structured(
            logger,
            "info",
            "Normalized REST Countries data",
            rows=len(df)
        )

        return df

    except Exception as e:
        log_structured(logger,"error",
            "Failed to normalize REST Countries data",
            error=str(e))

        return pd.DataFrame()

# 5. WHO GHO NORMALIZER
def normalize_who_gho_data(raw_data: Dict) -> pd.DataFrame:
    """
    Normalize WHO GHO API response into clean tabular format.
    """

    try:
        # WHO OData APIs usually store records under "value"
        if isinstance(raw_data, dict) and "value" in raw_data:
            data = raw_data["value"]
        else:
            data = raw_data

        df = pd.DataFrame(data)
        if not df.empty:
            # Add metadata columns
            df["source_system"] = "who_gho"
            df["ingestion_timestamp"] = datetime.utcnow()
            # Keep only required schema columns
            df = df[
                [
                    "IndicatorCode",
                    "IndicatorName",
                    "Language",
                    "source_system",
                    "ingestion_timestamp"
                ]
            ].copy()

        log_structured(logger,"info","Normalized WHO GHO data",
            rows=len(df))

        return df
    except Exception as e:
        log_structured(logger,"error","Failed to normalize WHO GHO data",
            error=str(e))
        return pd.DataFrame()


# 6. SCHEMA VALIDATION
def validate_api_schema(df: pd.DataFrame, source_name: str) -> bool:
    """
    Validate normalized API data against the correct source-specific schema.
    Loads schemas from data_contracts/bronze/api/
    """

    if df.empty:
        log_structured(
            logger,
            "warning",
            "Empty DataFrame - skipping schema validation",
            source=source_name
        )
        return False

    try:
        schema_path = (
            Path(__file__).resolve().parent.parent
            / "data_contracts"
            / "bronze"
            / "api"
            / f"{source_name}_schema.yaml"
        )

        if not schema_path.exists():
            log_structured(
                logger,
                "warning",
                "Schema file not found for source",
                source=source_name,
                schema_path=str(schema_path)
            )
            return True

        schema = load_yaml(str(schema_path))

        fields = schema.get("fields", {})

        required_columns = [
            field
            for field, spec in fields.items()
            if isinstance(spec, dict) and spec.get("required", False)
        ]

        if not required_columns:
            log_structured(
                logger,
                "warning",
                "No required columns defined in API schema",
                source=source_name
            )
            return True

        missing_columns = [
            col
            for col in required_columns
            if col not in df.columns
        ]

        if missing_columns:
            log_structured(
                logger,
                "error",
                "Schema validation failed for API data",
                source=source_name,
                missing_columns=missing_columns,
                available_columns=df.columns.tolist(),
                required_columns=required_columns
            )

            print("\n========== SCHEMA VALIDATION DEBUG ==========")
            print(f"SOURCE: {source_name}")
            print(f"Missing Columns: {missing_columns}")
            print(f"Required Columns: {required_columns}")
            print(f"Available Columns: {df.columns.tolist()}")
            print("=============================================\n")

            return False

        log_structured(
            logger,
            "info",
            "API data passed schema validation",
            source=source_name,
            rows=len(df),
            required_columns=required_columns
        )

        return True

    except Exception as e:
        log_structured(
            logger,
            "error",
            "Schema validation error",
            source=source_name,
            error=str(e)
        )

        return False

# 7. SINGLE API SOURCE PROCESSING (Core Orchestrator)
def process_single_api_source(source_config: Dict) -> bool:
    """
    Orchestrate the complete ingestion of ONE API source.
    """
    start_time = time.time()
    run_id = None
    source_name = source_config.get("name", "unknown")

    try:
        log_structured(
            logger,
            "info",
            "Starting API ingestion for source",
            source=source_name
        )

        # 1. Start pipeline run tracking
        run_id = insert_pipeline_run(
            dag_id="api_ingestion",
            pipeline_name=f"api_{source_name}",
            run_type="api",
            status="running"
        )

        # 2. Build URL with special handling for World Bank
        if source_name == "world_bank":
            country_code = source_config.get("countries", ["NG"])[0]  # take first country
            indicator = source_config.get("indicator")
            url = (
                f"{source_config['base_url']}/country/"
                f"{country_code}/indicator/{indicator}"
            )
        else:
            url = source_config["base_url"] + source_config.get("endpoint", "")

        
        raw_data = fetch_api_data(url, params=source_config.get("params", {}))
        # 3. Save raw JSON for audit & reproducibility
        local_file = save_raw_api_response(raw_data, source_name)

        # 4. Generate hash for idempotency
        file_hash = generate_file_hash(str(local_file))

        # 5. Idempotency check
        if is_already_processed(str(local_file), file_hash):
            update_pipeline_run(run_id=run_id, status="skipped")
            return True

        # 6. Normalize data based on source
        if source_name == "world_bank":
            df = normalize_world_bank_data(raw_data)
        elif source_name == "restcountries":
            df = normalize_restcountries_data(raw_data)
        elif source_name == "who_gho":
            df = normalize_who_gho_data(raw_data)
        else:
            df = pd.DataFrame()  # fallback

        # 7. Schema validation
        if not validate_api_schema(df, source_name):
            update_pipeline_run(run_id=run_id, status="failed")
            return False

        # 8. Generate Bronze object path
        object_name = generate_bronze_object_path(
            country=source_config.get("country", "global"),
            source_system=source_name,
            original_filename=local_file.name)

        # 9. Upload raw file to MinIO Bronze
        upload_success = upload_file_to_minio(
            local_file_path=str(local_file),
            bucket_name=get_config()["storage"]["buckets"]["bronze"],
            object_name=object_name)

        if not upload_success:
            update_pipeline_run(run_id=run_id, status="failed")
            return False

        # 10. Record successful processing in metadata
        insert_processed_file(
            file_name=str(local_file),
            file_hash=file_hash,
            country=source_config.get("country", "global"),
            source_system=source_name,
            rows_loaded=len(df))

        # 11. Update pipeline run as successful
        duration = time.time() - start_time
        update_pipeline_run(
            run_id=run_id,
            status="success",
            rows_processed=len(df),
            duration_seconds=round(duration, 2)
        )

        log_structured(logger,"info","Successfully processed API source",source=source_name,
            rows=len(df),duration_seconds=round(duration, 2))
        return True

    except Exception as e:
        duration = time.time() - start_time
        log_structured(logger,"error","Failed to process API source",source=source_name,
            error=str(e))
        if run_id:
            update_pipeline_run(
                run_id=run_id,
                status="failed",
                error_message=str(e),
                duration_seconds=round(duration, 2)
            )
        return False
    

# 8. BATCH / MULTI-SOURCE PROCESSING
def process_api_batch(sources: List[Dict]) -> int:
    """
    Process multiple API sources with proper error isolation.
    Each source fails independently without stopping the entire batch.
    """
    processed_count = 0
    failed_count = 0

    for source_config in sources:
        try:
            success = process_single_api_source(source_config)
            if success:
                processed_count += 1
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            log_structured(logger,"error",
                "Unexpected error processing API source in batch",
                source=source_config.get("name"),
                error=str(e))

    log_structured(
        logger,"info","API batch processing completed",
        total_sources=len(sources),
        successful=processed_count,
        failed=failed_count
    )
    return processed_count

# 9. MAIN PIPELINE RUNNER (Full Orchestrator)
def run_api_ingestion_pipeline() -> int:
    """
    Full API Ingestion Pipeline Orchestrator.
    Entry point for Airflow or manual execution.
    Uses insert_pipeline_run() at start and update_pipeline_run() at end.
    """
    start_time = time.time()
    run_id = None

    try:
        # Start pipeline run tracking
        run_id = insert_pipeline_run(
            dag_id="api_ingestion",
            pipeline_name="api_ingestion",
            run_type="full",
            status="running"
        )

        log_structured(logger,"info","Starting full API ingestion pipeline",run_id=run_id)

        # Get all configured API sources
        sources = get_api_sources()

        if not sources:
            log_structured(logger, "warning", "No API sources configured")
            update_pipeline_run(run_id=run_id, status="success", rows_processed=0)
            return 0

        # Process all sources
        processed_count = process_api_batch(sources)

        duration = time.time() - start_time

        # Update pipeline run as successful
        update_pipeline_run(
            run_id=run_id,
            status="success",
            rows_processed=processed_count,
            duration_seconds=round(duration, 2)
        )

        log_structured(logger,"info",
            "API ingestion pipeline completed successfully",
            run_id=run_id,
            sources_processed=processed_count,
            duration_seconds=round(duration, 2))

        return processed_count

    except Exception as e:
        duration = time.time() - start_time
        log_structured(logger,"error","API ingestion pipeline failed",
                       run_id=run_id,error=str(e),
                       duration_seconds=round(duration, 2))
        
        if run_id:
            update_pipeline_run(
                run_id=run_id,
                status="failed",
                error_message=str(e),
                duration_seconds=round(duration, 2))
        raise



# ENTRY POINT / TEST BLOCK
if __name__ == "__main__":
    print("🚀 Starting Multi-API Ingestion Pipeline...\n")
    
    start_time = time.time()
    
    processed = run_api_ingestion_pipeline()
    
    duration = time.time() - start_time
    
    print(f"\n✅ API Ingestion Pipeline Completed!")
    print(f"   Sources processed : {processed}")
    print(f"   Total duration    : {duration:.2f} seconds")