# Architecture Overview

## NYC Mobility Analytics Platform

---

## High-Level Architecture

### Batch Pipeline

`
[NYC TLC Parquet]   [Weather API]   [Azure SQL]
        |                 |               |
        +--------+--------+---------------+
                 |
        [Azure Data Factory]
          (Orchestration)
                 |
        [ADLS Gen2 - Data Lake]
          /       |        \
      Bronze    Bronze    Bronze   <- Raw data preserved
          \       |        /
        [Azure Databricks + PySpark]
                 |
               Silver              <- Cleaned, validated
                 |
                Gold               <- Star schema, analytical
                 |
        [Databricks SQL Warehouse]
                 |
             [Power BI]
`

---

## Layer Definitions

| Layer | Purpose | Format | Tool |
|-------|---------|--------|------|
| Bronze | Raw ingestion, source-faithful | Parquet / JSON | ADF |
| Silver | Cleaned, typed, deduplicated | Delta | Databricks |
| Gold | Star schema, analytical model | Delta | Databricks |

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Orchestration | Azure Data Factory | Schedule and move data |
| Storage | ADLS Gen2 | Data lake (Bronze/Silver/Gold) |
| Processing | Azure Databricks + PySpark | Transform data at scale |
| Table format | Delta Lake | ACID transactions, versioning |
| Governance | Unity Catalog | Permissions, lineage |
| Serving | Databricks SQL Warehouse | Query analytical data |
| Visualization | Power BI | Business dashboards |
| Security | Azure Key Vault + Entra ID | Secrets, RBAC |
| Version Control | Git + GitHub | Code and config management |

---

## Data Sources

| Source | Type | Load Pattern |
|--------|------|-------------|
| NYC TLC Trip Data | Parquet files (monthly) | Full load per month |
| NYC Taxi Zone Lookup | CSV | Full load (static) |
| Azure SQL (simulated) | Relational database | Incremental (watermark) |
| Open-Meteo Weather API | REST API | Incremental (date-based) |

---

## Data Lake Structure (ADLS Gen2)

`
mobility/
├── bronze/
│   ├── tlc/          <- Raw TLC Parquet files
│   ├── azure_sql/    <- Raw SQL extracts
│   └── weather/      <- Raw weather API responses
├── silver/
│   ├── trips/        <- Cleaned trip data (Delta)
│   ├── zones/        <- Cleaned zone data (Delta)
│   └── weather/      <- Cleaned weather data (Delta)
└── gold/
    ├── fact_trips/   <- Fact table (Delta)
    ├── dim_zone/     <- Zone dimension (Delta)
    ├── dim_date/     <- Date dimension (Delta)
    └── dim_weather/  <- Weather dimension (Delta)
`

---

## Star Schema (Gold Layer)

`
         dim_date
            |
dim_zone -- fact_trips -- dim_weather
`

**fact_trips**: One row per trip  
**dim_zone**: Pickup/dropoff zone attributes  
**dim_date**: Date/time breakdown  
**dim_weather**: Weather conditions at trip time  

---

## Security Model

- Azure Entra ID (authentication)
- RBAC (role-based access control)
- Azure Key Vault (secrets, no hardcoded credentials)
- Managed Identity (Databricks to ADLS access)
- Unity Catalog (data governance)
