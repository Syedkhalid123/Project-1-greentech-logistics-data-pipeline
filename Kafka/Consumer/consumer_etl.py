# consumer_etl.py
import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
import boto3

log = logging.getLogger("consumer_etl")
log.setLevel(logging.INFO)


def airflow_trigger(ti,
                    bootstrap_servers=None,
                    topic=None,
                    s3_bucket=None,
                    s3_prefix="raw/",
                    poll_timeout_ms=1000,
                    max_wait_seconds=60,
                    batch_max_size=500):

    bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "54.234.242.19:9092")
    topic = topic or os.getenv("KAFKA_TOPIC", "first-topic")
    s3_bucket = s3_bucket or os.getenv("S3_BUCKET", "first-project-greentech-logistics-datalake")
    region = os.getenv("AWS_REGION", "us-east-1")

    log.info("Starting short-run consumer: topic=%s bootstrap=%s", topic, bootstrap_servers)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-consumer-group',
        value_deserializer=lambda v: v.decode('utf-8') if v else None,
        consumer_timeout_ms=poll_timeout_ms
    )

    messages = []
    start = time.time()

    while time.time() - start < max_wait_seconds and len(messages) < batch_max_size:
        polled = consumer.poll(timeout_ms=poll_timeout_ms)
        for tp, recs in polled.items():
            for r in recs:
                try:
                    parsed = json.loads(r.value)
                except Exception:
                    parsed = r.value
                messages.append(parsed)
        if not polled:
            time.sleep(0.5)

    consumer.close()

    count = len(messages)
    if count == 0:
        ti.xcom_push(key='messages_count', value=0)
        ti.xcom_push(key='s3_batch_key', value=None)
        return []

    s3 = boto3.client("s3", region_name=region)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    s3_key = f"{s3_prefix}kafka_batch_{ts}.json"

    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=json.dumps(messages))

    log.info("Uploaded batch to s3://%s/%s (count=%d)", s3_bucket, s3_key, count)

    ti.xcom_push(key='messages_count', value=count)
    ti.xcom_push(key='s3_batch_key', value=s3_key)
    return messages
