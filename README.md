# 🚚 GreenTech Logistics – Real-Time Data Pipeline

**Flow:** Kafka → Airflow → S3 → Glue → Delta Lake → Snowflake
*Fully Event-Driven, Production-Ready Logistics Data Pipeline*

---

## 🌟 Project Overview

This project implements a **real-time, event-driven logistics data pipeline**:

* Produces truck telemetry to Kafka topics (GPS, fuel, temperature, delivery status).
* Airflow dynamically triggers consumer ETL and Glue jobs **only when new data arrives**.
* Validates data with Great Expectations inside AWS Glue.
* Splits data into **curated** and **rejected** datasets in Delta format.
* Loads validated data into Snowflake tables via Snowpipe.

**Goal:** Scalable, validated, and fully production-ready pipeline.

---

## 🏗️ Architecture

**1. Kafka Producer (EC2)**

* Produces batches of telemetry messages to topic `first-topic`.
* Injects simulated anomalies for testing validation.
* Checks Airflow DAG status before triggering new runs.

**2. Kafka Consumer ETL (Airflow Task)**

* Reads Kafka messages in short-run batches.
* Stores raw batches in S3 (`raw/`).
* Pushes batch metadata to Airflow for downstream processing.

**3. Apache Airflow DAG (EC2)**

* Event-driven: triggers Glue jobs **only when new raw files exist**.
* Prevents concurrent Glue runs.
* Tasks: `consume_kafka` → `detect_new_raw_files` → `trigger_glue`.

**4. AWS Glue Job**

* Reads raw JSON from S3.
* Performs **data cleaning & validation** using PySpark.
* Applies **Great Expectations** data quality checks.
* Splits into curated (`VALID`) and rejected (`REJECTED`) datasets.
* Writes outputs in **Delta format** to S3.

**5. Snowflake via Snowpipe**

* Auto-ingests curated & rejected Delta data into:

  * `LOGISTICS_CURATED_TABLE`
  * `LOGISTICS_REJECTED_TABLE`

---

## 📂 Folder Structure

```
greentech-logistics-data-pipeline/
│
├── kafka/
│ ├── producer_etl.py
│ └── consumer_etl.py
│
├── airflow/
│ └── kafka_to_glue_dag.py
│
├── glue/
│ └── glue_job.py
│
├── great_expectations/
│ └── expectations/validation_rules.json
│
├── snowflake/
│ ├── storage_integration.sql
│ ├── stage_curated.sql
│ ├── stage_rejected.sql
│ ├── pipe_curated.sql
│ └── pipe_rejected.sql
│
├── architecture/
│ └── architecture_diagram.png
│
├── README.md
└── requirements.txt
```

---

## 🔧 Technology Stack

| Layer               | Technology           |
| ------------------- | -------------------- |
| Real-time ingestion | Kafka (EC2)          |
| Event Trigger/API   | Python + Flask (EC2) |
| Orchestration       | Apache Airflow       |
| ETL                 | AWS Glue + PySpark   |
| Data Quality        | Great Expectations   |
| Data Format         | Delta Lake           |
| Storage             | Amazon S3            |
| Warehouse           | Snowflake + Snowpipe |

---

## 🧪 Data Quality Rules

* **NOT NULL:** `truck_id`, `timestamp`, `location.lat`, `location.lon`
* **Type Checks:** `fuel_level` & `temperature` → float
* **Range Checks:**

  * `fuel_level`: 0 → 100
  * `temperature`: -10 → 60
* **Delivery Status:** Must be one of `in_transit`, `delivered`, `delayed`
* **JSON Schema Validation:** Ensures all required fields exist

> Invalid records are sent to the **rejected** folder in S3 (`REJECTED`).

---

## ▶️ How to Run

### Kafka Producer

```bash
pip install kafka-python requests
python kafka/producer_airflow_trigger.py
```

### Airflow DAG

* DAG is event-driven; triggers consumer → detects new raw files → triggers Glue job.
* No fixed schedule; runs **only when new data is available**.

### Check Outputs

```bash
# S3 Buckets
s3://first-project-greentech-logistics-datalake/raw/
s3://first-project-greentech-logistics-datalake/curated/
s3://first-project-greentech-logistics-datalake/rejected/

# Snowflake Tables
SELECT * FROM LOGISTICS_CURATED_TABLE;
SELECT * FROM LOGISTICS_REJECTED_TABLE;
```

---

## 📸 Architecture Diagram

![Architecture](architecture_diagram.png)

---

## 👤 Author
**K Syed Khalid Hameed**
