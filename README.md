# AfroSurvey Intelligence Platform

### Production-Style Data Lakehouse & Analytics Platform for Multi-Country African Survey Data

<img src="images/MAP.png" width="1000">

AfroSurvey Intelligence Platform is an end-to-end Data Engineering solution designed to ingest, validate, transform, monitor, and analyze survey data collected across multiple African countries.

Built using modern data engineering principles, the platform implements a Bronze → Silver → Gold lakehouse architecture with automated orchestration, data quality monitoring, business analytics dashboards, and automated reporting capabilities.

### Key Features

* Multi-source data ingestion (CSV, APIs, Database Sources)
* Bronze → Silver → Gold Data Lakehouse Architecture
* Data Validation & Quality Monitoring Framework
* Automated Workflow Orchestration with Apache Airflow
* S3-Compatible Object Storage with MinIO
* PySpark-Based Data Transformation Pipelines
* Business Analytics Dashboard
* Platform Monitoring Dashboard
* Automated PDF & Executive Reporting
* Containerized Deployment with Docker

### Project Highlights

* 39 African Countries Analyzed
* Automated End-to-End Data Pipeline
* Data Quality & Reliability Monitoring

## Project Overview

AfroSurvey Intelligence Platform is a production-style data lakehouse and analytics solution built to process, monitor, and analyze survey data collected across multiple African countries.

The platform was designed to address common challenges faced by organizations managing large-scale survey operations, including fragmented data sources, inconsistent data quality, limited visibility into pipeline performance, and delayed reporting.

Using a modern Bronze → Silver → Gold architecture, the platform ingests raw survey data from multiple sources, applies validation and transformation rules, generates business-ready analytical datasets, and delivers actionable insights through interactive dashboards and automated reporting.

The solution combines data engineering, data quality management, workflow orchestration, business intelligence, and platform monitoring into a single integrated system.

### Core Objectives

* Centralize survey data from multiple sources.
* Improve data quality through validation and governance checks.
* Automate ingestion, transformation, and reporting workflows.
* Provide business stakeholders with real-time analytics and insights.
* Monitor platform health, pipeline performance, and data reliability.
* Enable scalable analytics across multiple African countries.


## Business Problem

Organizations conducting surveys across multiple countries often face significant operational and analytical challenges.

Survey responses may originate from different systems, formats, and collection channels, making it difficult to maintain consistency, trust, and visibility across the data lifecycle. As data volumes grow, manual validation processes become inefficient, reporting cycles become slower, and stakeholders struggle to obtain reliable insights in a timely manner.

Key challenges include:

* Inconsistent survey data formats across countries and collection channels.
* Missing, incomplete, or duplicate survey responses.
* Limited visibility into data quality and pipeline health.
* Slow turnaround time for business reporting and decision-making.
* Difficulty measuring survey coverage and participation across regions.
* Lack of centralized monitoring for ingestion and transformation workflows.

AfroSurvey Intelligence Platform addresses these challenges by implementing a scalable lakehouse architecture, automated data quality framework, workflow orchestration, business analytics dashboards, and platform monitoring capabilities that transform raw survey responses into trusted, actionable insights.

* Business Intelligence & Civic Perception Analytics
* Production-Style Data Engineering Architecture

## Architecture

<img src="images/ACTR.png" width="1000">



The platform follows a layered lakehouse architecture that separates raw ingestion, cleaned datasets, business-ready analytics, and reporting outputs.

Data flows through the following stages:

1. **Source Layer** — CSV files, REST APIs, and database sources.
2. **Ingestion Layer** — Python ingestion scripts collect and validate incoming data.
3. **Bronze Layer** — Raw data is stored in MinIO as the landing zone.
4. **Silver Layer** — Data is cleaned, standardized, validated, and deduplicated.
5. **Gold Layer** — Aggregated analytics tables are created for reporting and dashboards.
6. **Serving Layer** — Streamlit dashboards and automated reports consume Gold datasets.
7. **Monitoring Layer** — Airflow, pipeline logs, and platform metrics track performance and reliability.

## Technology Stack

| Layer | Technology |
|---|---|
| Programming Language | Python |
| Data Processing | PySpark, Pandas |
| Workflow Orchestration | Apache Airflow |
| Object Storage | MinIO |
| Database | PostgreSQL |
| Dashboard & Visualization | Streamlit, Plotly |
| Reporting | PDF generation, Email reporting |
| Containerization | Docker, Docker Compose |
| Data Contracts | YAML |
| Version Control | Git, GitHub |
| Development Environment | VS Code, DBeaver |


## Project Structure

The repository is organized into clear layers that reflect the platform’s data engineering workflow, from ingestion and transformation to analytics, monitoring, and reporting.

```text
afrosurvey-intelligence-platform/
│
├── airflow/                 # Airflow DAGs for orchestration
├── ingestion/               # Python ingestion scripts for APIs, CSVs, and PostgreSQL
├── spark_jobs/              # PySpark transformation jobs
├── data_contracts/          # Bronze, Silver, and Gold schema definitions
├── dashboard/               # Streamlit dashboard application
├── reporting/               # Automated report generation modules
├── metadata/                # SQL metadata and metrics scripts
├── utils/                   # Shared utilities and logging helpers
├── config/                  # Pipeline configuration files
├── tests/                   # Testing scripts
├── docker-compose.yml       # Multi-container infrastructure setup
└── README.md




---

## 7. Infrastructure

This is where you add **Docker, MinIO, and Airflow screenshots**.

```markdown
## Infrastructure

The platform runs on a containerized local data engineering environment using Docker Compose. The infrastructure includes object storage, metadata storage, orchestration, processing, and dashboard components.

### Docker Environment

Docker Compose is used to run the core platform services required for local development and testing.

<img src="images/docker-containers.png" width="1000">

### MinIO Object Storage

MinIO acts as an S3-compatible object storage layer for the data lakehouse. It stores raw, cleaned, and business-ready datasets across Bronze, Silver, and Gold zones.

<img src="images/minio-buckets.png" width="1000">

### Apache Airflow Orchestration

Apache Airflow orchestrates the pipeline workflows, including ingestion checks, transformation jobs, and pipeline monitoring tasks.

<img src="images/airflow-dag.png" width="1000">
