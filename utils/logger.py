"""
utils/logger.py
Central Structured JSON Logger for AfroSurvey Intelligence Platform
Production-ready, Airflow-compatible, config-driven structured logging
"""

import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from utils.config import load_config
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Module-level constants
VALID_LEVELS = {"debug", "info", "warning", "error", "critical"}

CUSTOM_FIELDS = {
    "pipeline",
    "country",
    "rows_processed",
    "duration_seconds",
    "status",
    "duplicate_rate",
    "file_name",
    "error",
    "run_id",          
    "batch_id",
    "source_system",
    "validation_status",
}


class JsonFormatter(logging.Formatter):
    """Clean JSON formatter for structured logging"""

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add only whitelisted custom fields
        for key in CUSTOM_FIELDS:
            if hasattr(record, key):
                log_record[key] = getattr(record, key)

        return json.dumps(log_record, ensure_ascii=False)


def get_logger(name: str = "afrosurvey") -> logging.Logger:
    """Return a properly configured structured logger"""
    
    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.hasHandlers():
        return logger

    # Load config inside the function (best practice)
    config = load_config()
    log_config = config.get("logging", {})

    logger.setLevel(log_config.get("level", "INFO").upper())
    logger.propagate = False  # Prevent duplicate logs in Airflow

    formatter = JsonFormatter()

    # Console handler (development)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (always JSON)
    log_file = Path(log_config.get("file", "./logs/afrosurvey.log"))
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def log_structured(logger: logging.Logger, level: str, message: str, **kwargs):
    """Safe structured logging helper - RECOMMENDED way to log with extra context"""
    level = level.lower()
    
    if level not in VALID_LEVELS:
        raise ValueError(f"Invalid log level: {level}. Use: {VALID_LEVELS}")

    # Pass extra context correctly using 'extra' parameter
    getattr(logger, level)(message, extra=kwargs)


# Quick test when running the file directly
if __name__ == "__main__":
    logger = get_logger(__name__)   # Recommended pattern for future modules

    log_structured(logger,"info","Pipeline started",pipeline="ingestion",country="Nigeria",run_id="run_001")
    log_structured(logger,"info","Records processed",rows_processed=45230,duration_seconds=12.7,status="success")
    log_structured(logger,"warning","High duplicate rate detected",duplicate_rate=0.034,country="Kenya")
    log_structured(logger,"error","Failed to process file",file_name="survey_ng_2026.csv",error="Permission denied")
    print("✅ Logger initialized successfully.")