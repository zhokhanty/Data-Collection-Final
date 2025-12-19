import sqlite3

DB_NAME = "events.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
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
    conn.close()


def insert_event(event: dict):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
    INSERT OR IGNORE INTO events
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, tuple(event.values()))

    conn.commit()
    conn.close()
