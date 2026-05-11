
-- AfroSurvey Intelligence Platform - Metadata Schema
-- Purpose: Enable idempotency, audit, lineage, monitoring, and incremental loading
-- All tables are created with IF NOT EXISTS for safe re-runs


-- 1. Pipeline Runs - Execution History & Monitoring
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id              VARCHAR(100) NOT NULL,           
    task_id             VARCHAR(100),                    
    pipeline_name       VARCHAR(100) NOT NULL,
    run_type            VARCHAR(50)  NOT NULL,           
    country             VARCHAR(100),
    source_system       VARCHAR(50),
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_seconds    FLOAT,
    status              VARCHAR(20)  NOT NULL,           
    rows_processed      INTEGER,
    trigger_type VARCHAR(50)
    rows_failed         INTEGER DEFAULT 0,
    validation_pass_rate FLOAT,
    environment VARCHAR(20)
    error_message       TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_status CHECK (status IN ('success', 'failed', 'running', 'skipped'))
);

-- Index for fast lookup by DAG and time
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_time ON pipeline_runs(dag_id, start_time);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);


-- 2. Processed Files - Core Idempotency Table
CREATE TABLE IF NOT EXISTS processed_files (
    file_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name           TEXT NOT NULL UNIQUE,            
    file_hash           TEXT,                            
    country             VARCHAR(100),
    source_system       VARCHAR(50),
    ingestion_timestamp TIMESTAMP,
    processing_status   VARCHAR(20) NOT NULL,            
    rows_loaded         INTEGER,
    batch_id            UUID,                          
    last_modified       TIMESTAMP,                       
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processed_files_name ON processed_files(file_name);
CREATE INDEX IF NOT EXISTS idx_processed_files_country ON processed_files(country);


-- 3. Data Quality Metrics - Detailed Quality Tracking
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id              UUID REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    country             VARCHAR(100),
    dataset_name        VARCHAR(100) NOT NULL,
    completeness_score  FLOAT,
    duplicate_rate      FLOAT,
    null_percentage     FLOAT,
    validation_status   VARCHAR(20),                  
    failed_checks       TEXT[],                          
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_run ON data_quality_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_country ON data_quality_metrics(country);


-- 4. Incremental State - Critical for Incremental Pipelines
CREATE TABLE IF NOT EXISTS incremental_state (
    source_key          VARCHAR(150) PRIMARY KEY,      
    last_successful_date DATE,                          
    last_ingestion_ts   TIMESTAMP,
    last_run_id         UUID REFERENCES pipeline_runs(run_id),
    rows_processed_total BIGINT DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comments for better documentation in pgAdmin / tools
COMMENT ON TABLE pipeline_runs IS 'Tracks every pipeline execution for audit and monitoring';
COMMENT ON TABLE processed_files IS 'Enables idempotency by tracking already ingested files';
COMMENT ON TABLE data_quality_metrics IS 'Detailed quality metrics per run and country';
COMMENT ON TABLE incremental_state IS 'Maintains state for incremental loading across runs';