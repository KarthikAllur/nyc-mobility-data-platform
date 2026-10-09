# Project Progress

## NYC Mobility Analytics Platform

**Started**: September 2026  
**Goal**: Build an MNC-style Data Engineering platform on Azure

---

## Phase Status

| Phase | Name | Status | Completed |
|-------|------|--------|-----------|
| 0 | Setup | Completed | October 2026 |
| 1 | Architecture and Data Modeling | Completed | October 2026 |
| 2 | ADLS Gen2 | Completed | October 2026 |
| 3 | Data Sources | Completed | October 2026 |
| 4 | Azure Data Factory | Completed | October 2026 |
| 5 | Databricks | Completed | October 2026 |
| 6 | Silver Layer | In Progress | - |
| 7 | Gold Layer | Not Started | - |
| 8 | Orchestration | Not Started | - |
| 9 | Analytics | Not Started | - |
| 10 | Production Engineering | Not Started | - |
| 11 | dbt | Not Started | - |
| 12 | Synapse | Not Started | - |
| 13 | Real-Time (Event Hubs) | Not Started | - |
| 14 | Kafka | Not Started | - |
| 15 | Final MNC Simulation | Not Started | - |

---

## Phase 0 - Setup

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] GitHub repository created
- [x] Repository cloned locally
- [x] Folder structure created (Bronze/Silver/Gold, ADF, Databricks, SQL, Python, Config)
- [x] Documentation files created (README, ARCHITECTURE, DECISIONS, DATA_DICTIONARY)
- [x] Azure free account activated
- [x] Resource Group created: rg-nyc-mobility-dev (Central India)
- [x] First commit pushed to GitHub

### Key concepts learned
- Git version control (add, commit, push)
- GitHub PAT authentication for multiple accounts
- Azure Free Account vs Pay-As-You-Go
- Azure Directory/Tenant concept
- Resource Group purpose and naming conventions
- .gitkeep convention for empty folders

---

## Phase 1 - Architecture & Data Modeling

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] Defined Lakehouse Medallion Architecture (Bronze -> Silver -> Gold)
- [x] Star schema data model designed (fact_trips, dim_zone, dim_date, dim_weather)
- [x] SCD Type 2 strategy planned for zone dimension
- [x] Storage and compute separation architecture established

### Key concepts learned
- Medallion Architecture: Bronze (raw immutable), Silver (cleaned/conformed Delta), Gold (business aggregations/star schema)
- Fact vs Dimension tables in dimensional modeling
- ELT vs ETL paradigm in cloud data lakes

---

## Phase 2 - ADLS Gen2

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] ADLS Gen2 Storage Account created: `stnycmobilitydevka` (India South Central, LRS)
- [x] Hierarchical Namespace enabled (true Data Lake file system)
- [x] Root container created: `mobility`
- [x] Medallion directories created: `bronze/`, `silver/`, `gold/`

### Key concepts learned
- Blob storage vs ADLS Gen2 (Hierarchical Namespace enables directory-level atomic operations)
- Redundancy options: LRS (Locally Redundant Storage) vs GRS/ZRS for cost optimization
- Access Keys vs Managed Identity for storage security

---

## Phase 3 - Data Sources

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] NYC TLC Yellow Taxi Jan 2024 Parquet identified (50 MB, ~3M trips)
- [x] Taxi Zone Lookup CSV identified (265 zones)
- [x] Open-Meteo Weather REST API identified (hourly temperature, precipitation, wind speed)

### Key concepts learned
- Columnar Parquet format vs row-based CSV/JSON
- Public S3/CloudFront bucket access patterns
- REST API parameterization and response structures

---

## Phase 4 - Azure Data Factory (ADF)

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] ADF instance created: `adf-nyc-mobility-dev`
- [x] Linked Services created: `ls_adls_nyc_mobility`, `ls_http_tlc`, `ls_http_weather_api`
- [x] Ingestion pipeline: `pl_ingest_tlc_bronze` (HTTP Parquet -> ADLS Gen2 Binary)
- [x] Ingestion pipeline: `pl_ingest_zone_lookup_bronze` (HTTP CSV -> ADLS Gen2 Binary)
- [x] Ingestion pipeline: `pl_ingest_weather_bronze` (REST API JSON -> ADLS Gen2 Binary)
- [x] Pipeline & Dataset Dynamic Parameterization (`@concat`, `@pipeline().parameters.YearMonth`)
- [x] Master Orchestration Pipeline (`pl_master_bronze_ingestion`)

### Key concepts learned
- Linked Services (connection strings) vs Datasets (file/table references)
- Copy Activity: Binary streaming for fast, zero-overhead Bronze ingestion
- Dynamic content expressions (`@concat`, `@pipeline().parameters`, `@dataset().parameters`)
- Passing parameters from parent pipelines to child pipelines
- Master Controller pattern using Execute Pipeline activities
- Concurrency & parallel activity execution in ADF (running multiple pipelines simultaneously)

---

## Phase 5 - Azure Databricks

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] Azure Databricks Premium workspace deployed: `dbw-nyc-mobility-dev`
- [x] Azure Access Connector configured (`dbw-access-connector`)
- [x] IAM Role Assignment: Storage Blob Data Contributor on `stnycmobilitydevka`
- [x] Unity Catalog Storage Credential (`cred_adls_nyc_mobility`)
- [x] Unity Catalog External Location (`loc_adls_mobility`)
- [x] Modern Serverless Compute validated
- [x] Authenticated ADLS Gen2 read via Apache Spark without hardcoded keys

### Key concepts learned
- Unity Catalog governance & Azure Managed Identity for zero-trust security
- Databricks Serverless Compute vs Classic Clusters
- ABFS protocol driver (`abfss://`) and DFS endpoints
- SparkSession, DataFrames, and distributed reading
