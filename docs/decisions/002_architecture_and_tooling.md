# Architecture Decision Record 002: Platform Architecture & Component Boundaries (HLD)

## Status
Proposed (Awaiting Review)

## Context
Veridian Finance requires an enterprise-grade data platform to ingest transactional core-banking data, clean structural anomalies, build analytical models, and serve executive KPIs to a leadership dashboard.

The architecture must separate operational transactional workloads (OLTP) from analytical compute (OLAP/Lakehouse) and consumption workloads (Serving Mart & APIs) to prevent operational database connection starvation and table locking, while remaining fully runnable locally via Docker Compose.

## Architecture Topology

┌──────────────────┐      ┌─────────────────────────┐      ┌────────────────────────────────────────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────┐
│   Source Files   │ ───► │  Operational Source DB  │ ───► │      Analytical Lakehouse (Object Storage / Parquet)   │ ───► │    Serving Data Mart    │ ───► │   Reporting & UI    │
│  (11 Raw CSVs)   │      │   Postgres (source.*) │      │    Bronze (Raw) ──► Silver (Conformed) ──► Gold (Star) │      │  Postgres (serving.*) │      │  FastAPI + Dash UI  │
└──────────────────┘      └─────────────────────────┘      └────────────────────────────────────────────────────────┘      └─────────────────────────┘      └─────────────────────┘

## Architectural Layers & Component Responsibilities

### 1. Operational Source System (`CSV -> PG`)
* **Technology:** PostgreSQL 16 container (`source` schema).
* **Role:** Emulates Veridian's legacy core-banking OLTP database.
* **Storage & Schema:** 11 relational tables matching raw transactional entities (`branch_master`, `loan_accounts`, `loan_transactions`, etc.).
* **Design Constraint:** Columns loaded losslessly as raw text to simulate production tables prior to analytical ETL.
* **Production AWS Equivalent:** Amazon RDS PostgreSQL / Aurora Core Banking cluster.

### 2. Lakehouse Storage & Processing Engine (`PG -> Lakehouse`)
Analytical processing is decoupled from the transactional database using a medallion architecture backed by Parquet files and an in-process columnar execution engine (DuckDB / PySpark):

* **Stage / Bronze (`lakehouse/bronze/`):**
  * Raw extraction landing zone partitioned by ingestion date/batch.
  * Extracted via batch cursor/CDC from Postgres with audit metadata (`_extracted_at`, `_batch_id`, `_source_table`).
  * Types remain lossless strings; zero record filtering.
  * *Production AWS Equivalent:* Amazon S3 Raw Landing Bucket via AWS DMS / Airflow extractors.

* **Silver Layer (`lakehouse/silver/`):**
  * Conformed, deduplicated, and strictly typed enterprise entities (`branches.parquet`, `customers.parquet`, `loans.parquet`, `transactions.parquet`).
  * Enforces RFC-4180 parsing, numeric clean-up, uppercase natural key normalization (`UPPER(TRIM(branch_code))`), and date casting (`ISO`, `MM/YYYY`, `DD-MON-YYYY`).
  * Implements deterministic conflict deduplication and an isolated quarantine dataset (`lakehouse/silver/quarantine/`) for unresolvable orphan records.
  * *Production AWS Equivalent:* AWS Glue / EMR Spark processing Delta Lake or Apache Iceberg on Amazon S3.

* **Gold Layer (`lakehouse/gold/`):**
  * Dimensional business models modeled in a star schema.
  * Computes branch P&L, regional risk metrics, loan demand distribution, and staffing benchmarks.
  * *Production AWS Equivalent:* S3 Curated Gold tables queried via Amazon Redshift Spectrum / Athena.

### 3. Serving Layer & Consumption (`Gold -> DB / Reporting`)
* **Serving Data Mart (`PostgreSQL: serving` schema):**
  * Synchronizes final Gold dimensional and fact tables into indexed relational tables optimized for fast point lookups and aggregations.
  * *Production AWS Equivalent:* Amazon Redshift Serverless / Aurora Read Replica.
* **API Service:** FastAPI exposing REST endpoints (`/api/v1/branches/profitability`, `/api/v1/regions/loan-demand`, `/api/v1/regions/risk-profile`).
* **Reporting UI:** Leadership executive dashboard consuming FastAPI JSON payloads.

## Key Architecture Decisions (Trade-offs & Rationale)

1. **Why decouple Lakehouse compute from PostgreSQL?**
   Running heavy historical aggregations, window functions, and deduplication scans directly against the operational Postgres database locks production tables and causes connection starvation for core banking operations. Extracting data into local columnar Parquet files guarantees zero analytical overhead on the transactional database.

2. **Why sync Gold back to a Serving DB instead of querying Parquet directly?**
   While analytical engines can query Parquet directly, an operational serving database provides transactional indexing (B-trees on surrogate keys), predictable sub-millisecond query concurrency, and standard connection pooling for downstream API clients.