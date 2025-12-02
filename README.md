# Project-1-greentech-logistics-data-pipeline
Kafka → Airflow → Glue → S3 → Delta Lake → Snowflake (Event Driven Pipeline)

🚀 Project Overview

This project implements a real-time logistics data pipeline that:

Ingests truck telemetry events from a Kafka producer.

Orchestrates processing using Apache Airflow.

Validates data using Great Expectations inside AWS Glue.

Stores curated/rejected data in Delta format on S3.

Loads validated data into Snowflake tables through Snowpipe.

This pipeline ensures real-time, validated, and scalable data processing for logistics operations.

🏗️ High-Level Architecture
Kafka Producer (EC2)

Generates truck GPS + sensor events.

Produces messages to Kafka topic logistics-topic.

Kafka Consumer API (EC2)

Exposes API /start-consumer.

Sends success response to Airflow once data is received.

Apache Airflow (EC2)

Calls consumer API → waits for data → triggers Glue job.

Prevents unnecessary Glue executions.

Implements dynamic orchestration (event-driven logic).

AWS Glue Job

Reads raw JSON from S3.

Performs data quality checks using Great Expectations.

Writes curated + rejected data in Delta format:

s3://bucket/curated/
s3://bucket/rejected/
Snowflake Integration via Snowpipe

Auto-ingests both curated and rejected data into:

LOGISTICS_CURATED_TABLE
LOGISTICS_REJECTED_TABLE
📂 Project Folder Structure
GreenTech-Logistics/
│
├── airflow/
│   └── kafka_to_glue_dag.py
├── kafka/
│   ├── producer.py
│   └── consumer_api.py
├── glue/
│   └── glue_job.py
├── snowflake/
│   ├── storage_integration.sql
│   ├── stage_curated.sql
│   ├── stage_rejected.sql
│   ├── pipe_curated.sql
│   └── pipe_rejected.sql
├── s3/
│   ├── raw/
│   ├── curated/
│   └── rejected/
├── architecture_diagram.png
├── README.md
└── requirements.txt
🔧 Technologies Used
Layer	Tech
Real-time ingestion	Kafka (EC2)
API Trigger	Python + Flask (EC2)
Orchestration	Apache Airflow
ETL	AWS Glue + PySpark
Data Quality	Great Expectations
Data Format	Delta Lake
Storage	Amazon S3
Warehouse	Snowflake + Snowpipe
🧪 Data Quality Rules (Great Expectations)

NOT NULL checks: truck_id, timestamp, latitude, longitude

Type Validation:

latitude & longitude → float

speed → float or integer

Value Range Validation:

latitude between -90 to 90

longitude between -180 to 180

speed ≥ 0

JSON Structure Validation:

Fields must match schema: truck_id, location.latitude, location.longitude, speed, timestamp

Records failing these rules → rejected folder.

🪄 Airflow Dynamic Orchestration

DAG calls consumer API: http://ec2-public-ip/start-consumer

Consumer listens to Kafka.

Once a message is received, consumer responds to Airflow.

Airflow waits 5 minutes for more data → triggers Glue job.

Avoids schedule-based waste; pipeline is fully event-driven.

🧰 Glue Job Workflow

Read raw JSON from S3.

Apply Great Expectations checks.

Split into:

curated_df → valid

rejected_df → invalid

Write both to S3 in Delta format.

Snowpipe automatically ingests them into Snowflake.

❄️ Snowflake Setup

Inside snowflake/ folder:

Storage integration

External stages

File formats

Snowpipes for curated & rejected data

Snowpipe continuously loads new Delta files into Snowflake tables.

▶️ How to Run

Start Kafka Producer

python kafka/producer.py

Start Kafka Consumer API

python kafka/consumer_api.py

Trigger Airflow DAG
Airflow UI → Trigger DAG → waits for data → runs Glue job.

Check S3 Outputs

s3://bucket/curated/
s3://bucket/rejected/

Check Snowflake Tables

SELECT * FROM LOGISTICS_CURATED_TABLE;
SELECT * FROM LOGISTICS_REJECTED_TABLE;
📸 Architecture Diagram

Refer to architecture_diagram.png

👤 Author

K Syed Khalid Hameed
