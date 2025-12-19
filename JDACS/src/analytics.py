import sqlite3
from datetime import date

DB_NAME = "analytics.db"

def run_analytics():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_summary (
        summary_date DATE,
        event_type TEXT,
        total_events INTEGER,
        avg_severity REAL,
        max_severity REAL
    )
    """)

    cur.execute("""
    INSERT INTO daily_summary
    SELECT
        DATE('now'),
        event_type,
        COUNT(*),
        AVG(severity),
        MAX(severity)
    FROM events
    GROUP BY event_type
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_analytics()
