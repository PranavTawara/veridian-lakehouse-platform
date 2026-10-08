# Decision 001: Platform Architecture & Tooling

**Date:** 2026-10-08
**Context:** The assignment requires a multi-layer data platform (Bronze, Silver, Gold), orchestration, data quality, and a serving API, entirely runnable locally without cloud spend.

**Decision:**
We will utilize a single Docker Compose environment containing:
1. **Database (Storage Engine):** PostgreSQL 16.
   * *Why:* Provides ACID compliance, robust constraint handling, and isolated logical schemas (`bronze`, `silver`, `gold`, `dq_recon`) within a single lightweight container. 
   * *Production Translation:* In an enterprise AWS environment, this maps directly to S3 (Bronze/Silver) processed by PySpark/Glue, feeding Redshift (Gold).
2. **Orchestration:** Python-based DAG pipeline runner (`pipeline.py`).
   * *Why:* Keeps local memory footprint negligible while enforcing strict task dependencies (Ingestion -> Transformation -> Recon).
   * *Production Translation:* Maps directly to AWS MWAA (Apache Airflow) for distributed execution.
3. **Serving API:** FastAPI (Python).
   * *Why:* Provides high-performance async endpoints and auto-generates OpenAPI documentation for the dashboard UI.