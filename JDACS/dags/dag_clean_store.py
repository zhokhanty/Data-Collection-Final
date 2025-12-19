from datetime import datetime, timedelta
import json
import sqlite3
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from clean_transform import clean_event

KAFKA_TOPIC = "raw_events"
KAFKA_BROKER = "localhost:9092"
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
SQLITE_DB = os.path.join(AIRFLOW_HOME, 'events.db')

default_args = {
    "owner": "zhalgas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def consume_and_store(**context):
    """Потребление событий из Kafka и сохранение в SQLite."""
    print(f"🚀 Starting batch processing")
    print(f"📅 Execution date: {context.get('execution_date')}")
    print(f"🗄️  Database: {SQLITE_DB}")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="airflow_batch_group",
            consumer_timeout_ms=10000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        print("✅ Kafka consumer connected")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        raise
    
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            event_date TEXT NOT NULL,
            location TEXT NOT NULL,
            magnitude REAL NOT NULL,
            source TEXT NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    inserted = 0
    skipped = 0
    
    try:
        for message in consumer:
            cleaned = clean_event(message.value)
            if not cleaned:
                skipped += 1
                continue
            
            cur.execute("""
                INSERT OR IGNORE INTO events
                (event_id, event_type, event_date, location, magnitude, source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                cleaned["event_id"],
                cleaned["event_type"],
                cleaned["event_date"],
                cleaned["location"],
                cleaned["magnitude"],
                cleaned["source"],
            ))
            
            if cur.rowcount > 0:
                inserted += 1
        
        conn.commit()
        print(f"✅ Inserted: {inserted}, Skipped: {skipped}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()
        consumer.close()

with DAG(
    dag_id="dag_hourly_clean_store",
    description="Hourly Kafka batch clean & store to SQLite",
    schedule_interval="@hourly",
    start_date=datetime(2025, 12, 19),
    catchup=False,
    default_args=default_args,
    tags=["kafka", "sqlite"],
) as dag:
    
    task = PythonOperator(
        task_id="consume_clean_store",
        python_callable=consume_and_store,
        provide_context=True,
    )