"""
utils/config.py
Secure configuration loader for AfroSurvey Intelligence Platform
Loads config.yaml + survey_schema.yaml + .env secrets safely
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import yaml
from typing import Dict, Any

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)


def load_yaml(file_path: str) -> Dict:
    """Helper to safely load any YAML file"""
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config() -> Dict[str, Any]:
    """Load and merge config.yaml + survey_schema.yaml + secrets from .env"""
    
    base_dir = Path(__file__).parent.parent

    # 1. Load main configuration
    config_path = base_dir / "config" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)


    # 2. Load Bronze survey schema
    schema_path = (
        base_dir
        / "data_contracts"
        / "bronze"
        / "survey_bronze_schema.yaml"
    )

    survey_schema = load_yaml(str(schema_path))


    # 3. Inject secrets from .env
    # MinIO
    if config.get("storage", {}).get("minio"):
        config["storage"]["minio"]["access_key"] = os.getenv("MINIO_ROOT_USER")
        config["storage"]["minio"]["secret_key"] = os.getenv("MINIO_ROOT_PASSWORD")
    
    # PostgreSQL
    if config.get("database"):
        config["database"]["user"] = os.getenv("POSTGRES_USER")
        config["database"]["password"] = os.getenv("POSTGRES_PASSWORD")
    
    # Airflow Fernet Key
    config["airflow_fernet_key"] = os.getenv("AIRFLOW__CORE__FERNET_KEY")

    # 4. Attach survey schema and useful shortcuts
    config["survey_schema"] = survey_schema
    
    # Safe shortcuts with fallback
    config["countries"] = survey_schema.get("countries", config.get("countries", []))
    
    global_transforms = survey_schema.get("global_transformations", {}) or {}
    config["country_code_to_name"] = (
    global_transforms.get("country_code_to_name") or {}
    )
    #config["country_code_to_name"] = global_transforms.get("country_code_to_name", {})

    print("Full configuration + data contract loaded successfully!")
    return config


def load_data_contract(layer: str, schema_name: str) -> Dict:
    """
    Load a specific data contract from the data_contracts folder.

    Examples:
        load_data_contract("bronze", "survey_bronze_schema.yaml")
        load_data_contract("bronze", "api/world_bank_schema.yaml")
        load_data_contract("silver", "survey_silver_schema.yaml")
        load_data_contract("gold", "survey_gold_schema.yaml")
    """
    base_dir = Path(__file__).parent.parent
    schema_path = base_dir / "data_contracts" / layer / schema_name

    if not schema_path.exists():
        raise FileNotFoundError(f"Data contract not found: {schema_path}")

    return load_yaml(str(schema_path))

# For quick testing
if __name__ == "__main__":
    config = load_config()
    print("\n--- Quick Summary ---")
    print(f"Project Name       : {config['project']['name']}")
    print(f"Environment        : {config.get('environment')}")
    print(f"Countries loaded   : {len(config['countries'])}")
    print(f"Schema Version     : {config['survey_schema'].get('version')}")
    print(f"MinIO Bronze Bucket: {config['storage']['buckets']['bronze']}")
    print(f"Database User      : {config['database']['user']}")
    print(f"Country Mappings   : {len(config['country_code_to_name'])} countries")
    print("\nConfig is ready for use in DAGs and pipelines!")