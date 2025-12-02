# kafka_to_glue_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from consumer_etl import airflow_trigger
import time

# ---------- DAG Default Args ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

# ---------- DAG Definition ----------
dag = DAG(
    "kafka_to_glue_dag",
    default_args=default_args,
    description="Kafka → S3 Raw → Glue ETL (event-driven, production-ready)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1
)

S3_BUCKET = "first-project-greentech-logistics-datalake"
RAW_PREFIX = "raw/"
AWS_REGION = "us-east-1"
GLUE_JOB_NAME = "Logistics_greentech_glue_job_script"
GLUE_CONCURRENT_STATES = ["RUNNING", "STARTING", "STOPPING"]  # prevent concurrency

# ---------- Step 1: Detect New Raw Files ----------
def detect_new_raw_files(ti):
    """Check if new raw files arrived in S3 since last run."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    last_processed = ti.xcom_pull(task_ids="trigger_glue", key="last_processed_key")

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_PREFIX)
    if "Contents" not in response:
        ti.xcom_push(key="new_file_key", value=None)
        return

    files = sorted([o["Key"] for o in response["Contents"]])
    latest_file = files[-1]

    if latest_file == last_processed:
        ti.xcom_push(key="new_file_key", value=None)
    else:
        ti.xcom_push(key="new_file_key", value=latest_file)

# ---------- Step 2: Trigger Glue Job Safely ----------
def trigger_glue_job(ti):
    """Trigger Glue only if new file exists and no concurrent run is active."""
    new_file = ti.xcom_pull(task_ids="detect_new_raw_files", key="new_file_key")
    if not new_file:
        print("⚠️ No new raw data → Glue NOT triggered.")
        return

    glue = boto3.client("glue", region_name=AWS_REGION)

    # Check for running Glue jobs to prevent ConcurrentRunsExceededException
    try:
        runs = glue.get_job_runs(JobName=GLUE_JOB_NAME, MaxResults=1)
        if runs['JobRuns'] and runs['JobRuns'][0]['JobRunState'] in GLUE_CONCURRENT_STATES:
            print(f"⚠️ Glue job {GLUE_JOB_NAME} is already running. Skipping this trigger.")
            return
    except Exception as e:
        print(f"⚠️ Could not check Glue job runs: {e}. Proceeding to trigger.")

    # Start Glue job
    resp = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={"--RAW_S3_KEY": new_file}
    )
    print(f"✅ Triggered Glue job: {resp['JobRunId']}")

    # Store last processed file
    ti.xcom_push(key="last_processed_key", value=new_file)

# ---------- Airflow Tasks ----------
consume_task = PythonOperator(
    task_id="consume_kafka",
    python_callable=airflow_trigger,
    provide_context=True,
    dag=dag
)

detect_task = PythonOperator(
    task_id="detect_new_raw_files",
    python_callable=detect_new_raw_files,
    provide_context=True,
    dag=dag
)

glue_task = PythonOperator(
    task_id="trigger_glue",
    python_callable=trigger_glue_job,
    provide_context=True,
    dag=dag
)

# ---------- Task Dependencies ----------
consume_task >> detect_task >> glue_task