import sqlite3
import logging
from pathlib import Path
from config import DB_PATH

logger = logging.getLogger(__name__)

def init_db() -> sqlite3.Connection:
    """
    Initialize SQLite database with required events table.
    Returns connection object.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE,
        event_type TEXT,
        country TEXT,
        severity REAL,
        latitude REAL,
        longitude REAL,
        event_time TEXT,
        ingestion_time TEXT
    )
    """)
    
    conn.commit()
    logger.info(f"Database initialized at {DB_PATH}")
    return conn

def insert_events(records):
    if not records:
        return 0
    conn = sqlite3.connect(DB_PATH)
    inserted = 0
    try:
        for rec in records:
            try:
                conn.execute(f"""
                    INSERT OR IGNORE INTO events 
                    (event_id, event_type, country, severity, latitude, longitude, event_time, ingestion_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec['event_id'],
                    rec['event_type'],
                    rec['country'],
                    rec['severity'],
                    rec['latitude'],
                    rec['longitude'],
                    rec['event_time'],
                    rec['ingestion_time']
                ))
                inserted += 1
            except Exception as e:
                logger.warning(f"Failed to insert record {rec.get('event_id')}: {e}")
        conn.commit()
        return inserted
    finally:
        conn.close()
