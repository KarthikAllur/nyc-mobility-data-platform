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
| 6 | Silver Layer | Completed | October 2026 |
| 7 | Gold Layer | Completed | October 2026 |
| 8 | Orchestration | Completed | October 2026 |
| 9 | Analytics | Skipped (DE Focus) | October 2026 |
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

---

## Phase 6 - Silver Layer (Delta Lake & Transformations)

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] **Silver Trips Delta Table**: 2,133,221 clean rows saved to `silver/tlc/yellow_tripdata_2024-01`, partitioned by `pickup_date`
- [x] **Trips Quarantine Delta Table**: 831,385 flagged records preserved in `silver_quarantine/tlc/yellow_tripdata_2024-01` with rejection reason arrays
- [x] **Silver Weather Delta Table**: 744 hourly rows (31 days × 24 hrs) saved to `silver/weather/hourly_nyc_2024-01`, partitioned by `observation_date`
- [x] **Silver Zones Delta Table**: 265 validated zones saved to `silver/zones/taxi_zone_lookup`, primary key uniqueness verified
- [x] Automated data quality profiling audit executed across all columns

### Key concepts learned
- **Quarantine / Dead-Letter Queue Pattern**: Capturing bad records with explicit reason tags (`_reject_reasons`) rather than silent dropping
- **Feature Engineering**: Deriving domain metrics (`trip_duration_minutes`, `average_speed_mph`, `tip_percentage`, `precipitation_category`, unit conversions)
- **Complex JSON Unnesting**: Parallel array flattening via `F.arrays_zip` and `F.posexplode`
- **Delta Lake Partitioning**: Physical directory partitioning (`pickup_date`, `observation_date`) for partition pruning query optimization
- **Data Quality Assertions**: Programmatic primary key uniqueness validation (`assert total_rows == distinct_ids`)
- **ACID Transactions**: Storing datasets in open-standard Delta Lake format with transaction logs (`_delta_log/`)

---

## Phase 7 - Gold Layer (Star Schema & Business Marts)

**Status**: Completed
**Completed date**: October 2026

### Completed
- [x] **Gold Enriched Trips**: 2,133,221 rows with zone + weather context embedded per row (`gold/tlc/enriched_trips_2024-01`)
- [x] **Join Integrity Validation**: Programmatic fan-out assertion (`Silver 2,133,221 = Gold 2,133,221 ✓`)
- [x] **Gold Daily Borough Summary**: 201 rows (31 days × 6 boroughs) with revenue, fare, speed, and weather KPIs (`gold/tlc/daily_borough_summary_2024-01`)
- [x] **Gold Hourly Demand & Weather**: 744 rows (31 days × 24 hours) correlating trip demand with temperature, precipitation, and wind speed (`gold/tlc/hourly_demand_weather_2024-01`)

### Key concepts learned
- **Star Schema Joins**: Joining fact table to dimension tables (dim_zone twice — pickup + dropoff, dim_weather on date+hour multi-key join)
- **Fan-Out Detection**: `assert gold_count == silver_count` to prevent silent row multiplication from bad joins
- **Business Aggregation Marts**: `groupBy` → `.agg()` collapsing millions of rows into business-ready summary tables
- **`F.first()` vs `.avg()`**: Using `first()` for constant-within-group columns (weather) vs `avg()` for variable metrics (fare, duration)
- **Left Joins in Data Lakes**: Using `how="left"` to preserve all fact rows even when dimension data is missing
- **Gold Layer Does Not Partition Small Tables**: ~201 and ~744 row tables write without `partitionBy` to avoid file fragmentation

---

## Phase 8 - End-to-End Orchestration & Automation

**Status**: Completed  
**Completed date**: October 2026

### Completed
- [x] **Databricks Linked Service** (`ls_databricks_nyc_mobility`) configured with Databricks Access Token (`all APIs` scope) and Serverless compute.
- [x] **Databricks Serverless Job** (`job_nyc_mobility_lakehouse`, ID: `392917716271722`) created wrapping notebook `01_bronze_to_silver_tlc`.
- [x] **Dynamic Parameterization**: Databricks widgets + ADF parameters enabling zero-code multi-month processing via `YearMonth`.
- [x] **Dynamic Calendar Calculations**: Built ADF expression using `addToTime` and `addDays` to automatically calculate month boundaries and leap year ends (e.g. `2024-02-29`) for Open-Meteo REST API.
- [x] **Master End-to-End Pipeline** (`pl_master_end_to_end`):
  - Step 1: `ep_ingest_bronze` (`pl_master_bronze_ingestion`) — downloads raw TLC Parquet + Weather JSON.
  - Step 2: `job_transform_lakehouse` — triggers Databricks Serverless Job to clean Silver Delta, handle quarantine, and build Gold Delta marts.
- [x] **Enterprise Watermark Control Framework**:
  - ADLS Control File: `config/watermark.json` storing state (`last_processed_month`, `next_month`, `status`).
  - ADF `Lookup` activity (`LKP_watermark`) dynamically evaluates pending batch partition.
  - ADF `If Condition` activity (`Check_Backfill_Pending`) automatically halts pipeline once `BACKFILL_COMPLETE`.
  - Concurrency set to `1` to eliminate race conditions.
  - Guarded Databricks script programmatically advances watermark pointer to next month upon 100% Gold completion.
- [x] **Successful Verification Runs**:
  - `2024-02` (February 2024): End-to-end verified (`ep_ingest_bronze`: 29s, `job_transform_lakehouse`: 2m 15s).
  - `2024-03` (March 2024): 100% autonomous execution via Watermark (`LKP_watermark`: 14s, `ep_ingest_bronze`: 21s, `job_transform_lakehouse`: 1m 59s). Watermark auto-advanced to `2024-04`.
  - `2024-04` (April 2024): **100% Autonomous Scheduled Trigger Run** (`tr_daily_backfill_twice_a_day`) fired at 9:00:00 AM IST on schedule (total pipeline runtime: 3m 28s). Ingested April TLC + Weather, transformed Silver/Gold, and auto-advanced watermark to `2024-05`.

### Key concepts learned
- **Master-Child Orchestration Pattern**: Decoupling ingestion controller (`pl_master_bronze_ingestion`) from end-to-end lakehouse workflow (`pl_master_end_to_end`).
- **Serverless Databricks via ADF Job Activity**: Using the `Job` activity in ADF to invoke Databricks Serverless compute rather than legacy VM cluster activities.
- **Dynamic Expression Functions**: Combining `@concat`, `@addToTime`, and `@addDays` to handle variable calendar lengths and leap years programmatically.
- **Fail-Fast Data Validation**: Pre-execution input validation inside Databricks to catch missing upstream files early before Spark execution.


