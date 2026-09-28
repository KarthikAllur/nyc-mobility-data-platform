# NYC Mobility Analytics Platform

An MNC-style Data Engineering project built on Microsoft Azure.

## Business Objective

Build a data platform that collects NYC mobility/taxi data and delivers reliable,
cleaned, and analytical data to business analysts, Power BI, data analysts, and data scientists.

## Architecture

`
[NYC TLC Data]  [Weather API]  [Azure SQL]
        |               |              |
        +-------+--------+--------------+
                |
      [Azure Data Factory]
                |
      [ADLS Gen2 - Data Lake]
         Bronze | Silver | Gold
                |
      [Azure Databricks + PySpark + Delta]
                |
      [Databricks SQL Warehouse]
                |
           [Power BI]
`

## Technology Stack

- **Orchestration**: Azure Data Factory
- **Storage**: Azure Data Lake Storage Gen2
- **Processing**: Azure Databricks + PySpark
- **Table Format**: Delta Lake
- **Governance**: Unity Catalog
- **Serving**: Databricks SQL Warehouse
- **Visualization**: Power BI
- **Security**: Azure Key Vault + Entra ID

## Data Sources

- NYC TLC Trip Record Data (monthly Parquet files)
- NYC Taxi Zone Lookup (CSV)
- Simulated Operational Database (Azure SQL)
- Open-Meteo Historical Weather API

## Project Documentation

- [PROGRESS.md](PROGRESS.md) - Phase-by-phase progress tracking
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design and architecture
- [DATA_DICTIONARY.md](DATA_DICTIONARY.md) - Field definitions for all tables
- [DECISIONS.md](DECISIONS.md) - Architecture decision records

## Project Structure

`
nyc-mobility-data-platform/
├── adf/              # Azure Data Factory pipeline definitions
├── databricks/       # Databricks notebooks and src code
├── sql/              # DDL scripts and control tables
├── python/           # API scripts and utilities
├── config/           # Environment configuration
├── docs/             # Architecture diagrams and documentation
└── .github/          # CI/CD workflows
`

## Status

Phase 0 - Setup: In Progress
