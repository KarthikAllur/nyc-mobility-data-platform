# Decisions Log

## NYC Mobility Analytics Platform

This file records key architecture and technology decisions with reasoning.
Use this to explain your choices in interviews.

---

## Decision 1: Azure as the primary cloud platform

**Decision**: Use Azure (not AWS or GCP)  
**Reason**: Azure has the tightest integration between ADF, ADLS, Databricks, and Power BI.
Entra ID provides enterprise-grade identity management across all services.
Azure is widely used in MNC Data Engineering roles.

---

## Decision 2: ADLS Gen2 as the data lake

**Decision**: Use Azure Data Lake Storage Gen2 for all raw and processed data  
**Reason**: ADLS Gen2 supports hierarchical namespace (folder structure), integrates natively
with ADF and Databricks, and is the standard Azure data lake solution.
Cost-effective for large volumes compared to Azure Blob Storage for analytics workloads.

---

## Decision 3: Bronze / Silver / Gold medallion architecture

**Decision**: Use three-layer medallion architecture  
**Reason**: Separating raw (Bronze), cleaned (Silver), and analytical (Gold) data means:
- Raw data is always preserved for reprocessing
- Consumers get clean, reliable data
- Problems can be debugged at each layer independently
- Industry-standard pattern used in most MNC Data Engineering teams

---

## Decision 4: Databricks as the primary processing engine

**Decision**: Use Azure Databricks with PySpark for all transformations  
**Reason**: Databricks is the industry standard for large-scale data processing on Azure.
PySpark handles datasets that would be too large for a single machine.
Databricks integrates natively with ADLS Gen2 and Delta Lake.
More suitable for complex transformations than ADF data flows.

---

## Decision 5: Delta Lake as the table format for Silver and Gold

**Decision**: Use Delta Lake instead of plain Parquet for curated layers  
**Reason**: Delta provides ACID transactions (reliable updates/deletes), schema enforcement,
time travel (query historical versions), and efficient MERGE operations for incremental loading.
Plain Parquet has none of these features.

---

## Decision 6: ADF as the orchestration tool

**Decision**: Use Azure Data Factory for pipeline orchestration  
**Reason**: ADF is the native Azure orchestration service. It handles scheduling, 
dependency management, retry logic, and monitoring without custom infrastructure.
It also provides metadata-driven (parameterized) pipeline patterns used in real MNC environments.

---

## Decision 7: Metadata-driven ingestion pattern

**Decision**: Use a control/metadata table to drive ingestion instead of hardcoding sources  
**Reason**: Hardcoding one pipeline per source does not scale. A metadata-driven approach
means adding a new source only requires a new row in the control table, not a new pipeline.
This is the standard pattern in enterprise Data Engineering.

---

## Decision 8: Star schema for the Gold layer

**Decision**: Model Gold layer as a star schema (fact + dimensions)  
**Reason**: Star schemas are optimized for analytical queries and BI tools.
They are simple to understand, query fast due to denormalization, and are
the standard pattern for Data Warehousing and Power BI consumption.

