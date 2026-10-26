# Veridian Lakehouse Platform

An enterprise financial data lakehouse and API serving platform implementing Medallion Architecture (Bronze -> Silver -> Gold) on Delta Lake and PostgreSQL. The system is orchestrated via Apache Airflow, modeled via dbt, exposed via a Hasura GraphQL/REST API gateway, and audited by an automated 10-gate financial reconciliation engine.

> **Execution Environment:** 100% Local Containerized Lakehouse (Zero-Cloud-Cost Reference Architecture). Fully reproducible on a local workstation without external cloud dependencies, with a direct 1:1 migration path to AWS (S3, RDS, MWAA, ECS).

---

## 1. Quickstart: Clean Checkout Runbook

### Prerequisites
* Docker & Docker Compose
* Python 3.10+ (tested on Ubuntu 24.04 with Python 3.12)
* Java 8/11/17 (required for local PySpark execution)

---

### A. Environment Setup & Services

1. Clone the repository:
   ```bash
   git clone [https://github.com/PranavTawara/veridian-lakehouse-platform.git](https://github.com/PranavTawara/veridian-lakehouse-platform.git)
   cd veridian-lakehouse-platform