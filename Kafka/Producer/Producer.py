#!/usr/bin/env python3
"""
Production-grade Kafka Producer with Airflow dynamic trigger.

Behavior:
- Produce batches of telemetry to Kafka topic (default: 'first-topic').
- After each batch, check Airflow DAG runs via Airflow REST API.
- Trigger DAG only when it appears free (no running/queued runs).
- Uses environment variables for configuration.

Usage:
  pip install kafka-python requests
  export KAFKA_BOOTSTRAP="54.234.242.19:9092"
  export AIRFLOW_API_BASE="http://44.200.93.26:8080/api/v1"
  python producer_airflow_trigger.py
"""
import os
import json
import time
import random
import logging
import signal
from datetime import datetime
from kafka import KafkaProducer, errors as kafka_errors
import requests

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

# ---------- Config (env vars with defaults) ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "54.234.242.19:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "first-topic")
AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://44.200.93.26:8080/api/v1")
DAG_ID = os.getenv("AIRFLOW_DAG_ID", "kafka_to_glue_dag")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin123")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
SLEEP_BETWEEN_MESSAGES = float(os.getenv("SLEEP_BETWEEN_MESSAGES", "1.0"))
PRODUCER_RETRIES = int(os.getenv("PRODUCER_RETRIES", "5"))
ACKS = os.getenv("KAFKA_ACKS", "all")

# ---------- Graceful shutdown ----------
running = True
def shutdown(signum, frame):
    global running
    log.info("Received shutdown signal. Stopping producer loop gracefully...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ---------- Kafka Producer setup ----------
def create_producer():
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=PRODUCER_RETRIES,
            linger_ms=10,
            acks=ACKS
        )
        log.info("Kafka producer created")
        return p
    except Exception as e:
        log.exception("Failed to create KafkaProducer: %s", e)
        raise

producer = create_producer()

# ---------- Helpers ----------
def inject_data_anomalies(data):
    anomaly_type = random.choice(["none", "null", "out_of_range", "nan_str"])
    field = random.choice(["fuel_level", "temperature", "location"])
    if anomaly_type == "none":
        return data
    if anomaly_type == "null":
        data[field] = None
    elif anomaly_type == "nan_str":
        data[field] = "NaN"
    elif anomaly_type == "out_of_range":
        if field == "fuel_level":
            data[field] = random.choice([-10, 150, 9999])
        elif field == "temperature":
            data[field] = random.choice([-50, 200, 999])
        else:
            data[field] = {"lat": random.uniform(-999, 999), "lon": random.uniform(-999, 999)}
    return data

def is_dag_running(timeout=5):
    """Return True if DAG has running/queued runs; conservative on API errors."""
    endpoint = f"{AIRFLOW_API_BASE}/dags/{DAG_ID}/dagRuns"
    try:
        r = requests.get(endpoint, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=timeout)
    except Exception as e:
        log.warning("Cannot reach Airflow API (%s): %s", endpoint, e)
        return True  # conservative: assume busy
    if r.status_code != 200:
        log.warning("Airflow API returned status %s: %s", r.status_code, r.text)
        return True
    runs = r.json().get("dag_runs", [])
    for run in runs:
        if run.get("state") in ("running", "queued"):
            log.info("Found DAG run in state '%s' -> DAG is busy", run.get("state"))
            return True
    log.info("No running/queued DAG runs found -> DAG is free")
    return False

def trigger_airflow(timeout=10):
    endpoint = f"{AIRFLOW_API_BASE}/dags/{DAG_ID}/dagRuns"
    payload = {"conf": {}}
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(endpoint, json=payload, headers=headers, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=timeout)
        if r.status_code in (200, 201):
            log.info("Triggered Airflow DAG successfully")
            return True
        log.warning("Failed to trigger DAG: %s %s", r.status_code, r.text)
        return False
    except Exception as e:
        log.exception("Exception triggering Airflow DAG: %s", e)
        return False

# ---------- Producer main loop ----------
truck_ids = [f"TRUCK_{i:03d}" for i in range(1, 16)]

def send_batch():
    batch = []
    for _ in range(BATCH_SIZE):
        data = {
            "truck_id": random.choice(truck_ids),
            "timestamp": datetime.utcnow().isoformat(),
            "location": {"lat": 17.38 + random.random() / 100, "lon": 78.48 + random.random() / 100},
            "fuel_level": round(random.uniform(50, 100), 2),
            "temperature": round(random.uniform(15, 25), 1),
            "delivery_status": random.choice(["in_transit", "delivered", "delayed"])
        }
        if random.random() < 0.15:
            data = inject_data_anomalies(data)
        batch.append(data)
        try:
            future = producer.send(TOPIC, value=data)
            # optional: block on send metadata with timeout
            record_metadata = future.get(timeout=10)
            log.debug("Sent to partition %s offset %s", record_metadata.partition, record_metadata.offset)
        except kafka_errors.KafkaTimeoutError as e:
            log.warning("KafkaTimeoutError while sending: %s", e)
        except Exception as e:
            log.exception("Unexpected error sending message: %s", e)
        time.sleep(SLEEP_BETWEEN_MESSAGES)
    return batch

def main_loop():
    log.info("Producer loop starting. Topic=%s Broker=%s", TOPIC, KAFKA_BOOTSTRAP)
    backoff = 1
    while running:
        try:
            batch = send_batch()
            log.info("Batch of %d messages sent", len(batch))
            # Try to trigger Airflow if DAG free. Try small number of attempts with backoff.
            for attempt in range(3):
                if is_dag_running():
                    log.info("DAG busy; waiting 10s before re-check (attempt %d)", attempt+1)
                    time.sleep(10)
                    continue
                if trigger_airflow():
                    break
                log.info("Trigger attempt failed; waiting 10s (attempt %d)", attempt+1)
                time.sleep(10)
            backoff = 1  # reset
        except Exception as e:
            log.exception("Error in main loop: %s", e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
    # flush before exit
    try:
        producer.flush(timeout=10)
        producer.close()
    except Exception:
        pass
    log.info("Producer stopped")

if __name__ == "__main__":
    main_loop()
