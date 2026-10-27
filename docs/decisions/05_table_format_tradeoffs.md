## Storage Format Trade-Off Analysis: Why Delta Lake over Apache Iceberg & Apache Hudi

### 1. Executive Summary & Design Decision
For the Veridian Data Platform, **Delta Lake** was selected as the underlying open table format for Bronze and Silver layers over Apache Iceberg and Apache Hudi. This decision balances zero-dependency operational simplicity, first-class PySpark integration, and ACID merge performance against the batch nature of the upstream exports.

---

### 2. Comparative Evaluation Matrix

| Evaluation Dimension | Delta Lake (Selected) | Apache Iceberg | Apache Hudi |
| :--- | :--- | :--- | :--- |
| **Catalog Decoupling** | **Self-contained directory storage** (`_delta_log/`); zero external catalog required for local execution. | **Strictly decoupled metadata** requiring an active catalog (JDBC, Nessie, REST, Glue). | **Indexed directory storage** (`.hoodie/`) tied closely to Hive Metastore or timeline servers. |
| **Operational Overhead** | **Zero additional infrastructure**; installs via `delta-spark` Python package. | **Moderate/High**; requires running a catalog container in Docker for local ACID commits. | **High**; heavy dependency on Spark bundle JARs, native lock providers, and tuning parameters. |
| **PySpark Integration** | **Native first-class support**; seamless integration with Spark SQL and DataFrame writer APIs. | Excellent via Iceberg Spark runtime, but requires custom Spark catalog configurations. | Good, but historically tied to Spark RDD/HDFS conventions with larger runtime footprint. |
| **Workload Alignment** | **Optimized for scheduled batch file ingestion**, schema evolution, and file pruning. | **Optimized for multi-engine federated reads** (Trino, Snowflake, Spark, Presto). | **Optimized for low-latency streaming CDC** and sub-second point updates/deletes. |
| **Merge / Upsert Engine** | Fast file-level copy-on-write `MERGE INTO` with partition skipping. | Merge-on-Read (equality/position deletes) and Copy-on-Write. | Merge-on-Read with native record-level bloom filters and file indexes. |

---

### 3. Detailed Architectural Trade-Offs

#### A. Why Delta Lake Fits the Current Problem Statement
1. **Self-Contained ACID Isolation Without Catalog Complexity:**  
   Delta Lake stores JSON transaction commits directly inside the directory path alongside Parquet data. For a reproducible local architecture (and even single-engine cloud architectures), this eliminates the failure modes of running an external catalog service while preserving ACID transactions, schema enforcement (`mergeSchema = true`), and audit time travel (`VERSION AS OF`).
2. **First-Class PySpark Developer Velocity:**  
   Our data conformation, deduplication, and quarantine logic are implemented in PySpark. Delta Lake is natively co-designed with Spark's catalyst optimizer, providing seamless schema evolution and structured streaming/batch compatibility without classpath friction.
3. **Workload Appropriateness:**  
   The platform processes discrete, scheduled snapshots from core banking systems. Delta’s copy-on-write merge performance is optimal for this batch profile.

#### B. Why Not Apache Iceberg?
* **The Case for Iceberg:** Iceberg excels at multi-engine query federation (e.g., if Trino, DuckDB, Spark, and Snowflake all concurrently read and write to the same lake without routing through Spark).
* **The Trade-Off Accepted:** Because PySpark is our dedicated transformation engine and PostgreSQL is our serving engine, multi-engine lakehouse access was not an active requirement. Introducing Iceberg would have mandated provisioning a catalog service (like Project Nessie) inside Docker, adding architectural complexity without improving data fidelity or performance.

#### C. Why Not Apache Hudi?
* **The Case for Hudi:** Hudi is the industry standard for high-throughput, micro-batch streaming CDC from event queues (e.g., Kafka) requiring row-level bloom-filter indexing and low-latency point deletes.
* **The Trade-Off Accepted:** Our inputs are batch CSV exports, not high-velocity event streams. Hudi’s index maintenance, timeline cleaner overhead, and compaction jobs introduce unnecessary compute overhead for static file reconciliations.

---

### 4. Scale & Evolution Boundaries: Where Delta Lake Stops Working

To ensure the platform does not become a technical dead-end as Veridian scales, we have defined explicit architectural pivot triggers:

1. **Multi-Engine Federation Trigger (Pivot to Apache Iceberg):**  
   If Veridian transitions from single-engine PySpark processing to a federated lakehouse where data scientists query raw storage directly via Trino, Flink, and Snowflake concurrently, we will migrate to **Apache Iceberg** backed by a managed AWS Glue or REST Catalog.
2. **Streaming CDC Trigger (Pivot to Apache Hudi):**  
   If core banking systems replace periodic file dumps with continuous, sub-minute transaction CDC streams from Kafka or Debezium, we will adopt **Apache Hudi** to leverage its record-level indexing and merge-on-read compactions.

---

### 5. Incremental Processing Strategy (`second_load` Evolution)

To satisfy the requirement that the pipeline processes incremental updates without overwriting historical baselines:

* **Append-Only Streams (`loan_transactions`):**  
  Employs high-watermark filtering (`txn_timestamp > MAX(txn_timestamp)`) paired with Delta `MERGE INTO` on `txn_id` to prevent duplicate ledger entries during upstream replay.
* **Mutable Entities (`loan_accounts`, `customer_master`):**  
  Employs a Change Data Capture (CDC) pattern utilizing primary keys and deterministic **SHA-256 Row Hash Keys** across mutable attributes (e.g., `dpd`, `outstanding_amount`, `kyc_status`). Delta Lake executes an atomic upsert:
  * Inserts new contracts.
  * Updates existing contracts only when the attribute hash differs (`target.row_hash != source.row_hash`).
  * Skips unchanged records at zero I/O cost.
* **Serving Layer (`dbt run`):**  
  Converts fact and dimension models from `materialized='table'` to `materialized='incremental'`, scanning only records where `_loaded_at > MAX(_loaded_at)`.