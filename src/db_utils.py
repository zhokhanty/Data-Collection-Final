import sqlite3
import logging
from pathlib import Path
from config import DB_PATH, EVENTS_TABLE_NAME, SUMMARY_TABLE_NAME

logger = logging.getLogger(__name__)


def init_db():
    """
    Initialize SQLite database with required tables.
    Creates 'events' and 'daily_summary' tables if they don't exist.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {EVENTS_TABLE_NAME} (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            country TEXT,
            severity REAL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            event_time TEXT,
            ingestion_time TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Create daily_summary table
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE_NAME} (
            summary_date DATE NOT NULL,
            event_type TEXT NOT NULL,
            total_events INTEGER,
            avg_severity REAL,
            max_severity REAL,
            min_severity REAL,
            affected_countries INTEGER,
            PRIMARY KEY (summary_date, event_type),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {DB_PATH}")
        return True

    except sqlite3.Error as e:
        logger.error(f"Database initialization failed: {e}")
        raise


def drop_tables():
    """
    Drop all tables (use for testing/reset only).
    WARNING: This will delete all data!
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {EVENTS_TABLE_NAME}")
        cursor.execute(f"DROP TABLE IF EXISTS {SUMMARY_TABLE_NAME}")
        conn.commit()
        conn.close()
        logger.warning("All tables dropped!")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error dropping tables: {e}")
        raise

def insert_events(events: list) -> int:
    """
    Insert batch of cleaned events into SQLite.
    
    Args:
        events: List of event dictionaries with keys:
                (event_id, event_type, country, severity, latitude, longitude, 
                 event_time, ingestion_time)
    
    Returns:
        Number of events inserted
    """
    if not events:
        logger.warning("No events to insert")
        return 0

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.executemany(f"""
            INSERT OR IGNORE INTO {EVENTS_TABLE_NAME}
            (event_id, event_type, country, severity, latitude, longitude, event_time, ingestion_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                e['event_id'],
                e['event_type'],
                e['country'],
                e['severity'],
                e['latitude'],
                e['longitude'],
                e['event_time'],
                e['ingestion_time']
            )
            for e in events
        ])

        conn.commit()
        inserted = cursor.rowcount
        conn.close()
        logger.info(f"Inserted {inserted} events into database")
        return inserted

    except sqlite3.Error as e:
        logger.error(f"Error inserting events: {e}")
        raise

def get_events_count() -> int:
    """Get total number of events in database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {EVENTS_TABLE_NAME}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except sqlite3.Error as e:
        logger.error(f"Error counting events: {e}")
        return 0


def get_summary_count() -> int:
    """Get total number of summary records"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {SUMMARY_TABLE_NAME}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except sqlite3.Error as e:
        logger.error(f"Error counting summaries: {e}")
        return 0


def get_events_by_date(target_date: str):
    """
    Get all events for a specific date.
    
    Args:
        target_date: Date string (YYYY-MM-DD)
    
    Returns:
        List of event dictionaries
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {EVENTS_TABLE_NAME}
            WHERE DATE(ingestion_time) = ?
            ORDER BY ingestion_time DESC
        """, (target_date,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logger.error(f"Error retrieving events: {e}")
        return []


def get_summary_by_date(target_date: str):
    """Get summary records for a specific date"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {SUMMARY_TABLE_NAME}
            WHERE summary_date = ?
        """, (target_date,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logger.error(f"Error retrieving summary: {e}")
        return []


def insert_summary(summaries: list) -> int:
    """
    Insert aggregated summary records.
    
    Args:
        summaries: List of summary dictionaries with keys:
                   (summary_date, event_type, total_events, avg_severity,
                    max_severity, min_severity, affected_countries)
    
    Returns:
        Number of summaries inserted
    """
    if not summaries:
        logger.warning("No summaries to insert")
        return 0

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.executemany(f"""
            INSERT OR REPLACE INTO {SUMMARY_TABLE_NAME}
            (summary_date, event_type, total_events, avg_severity, max_severity, min_severity, affected_countries)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                s['summary_date'],
                s['event_type'],
                s['total_events'],
                s['avg_severity'],
                s['max_severity'],
                s['min_severity'],
                s['affected_countries']
            )
            for s in summaries
        ])

        conn.commit()
        inserted = cursor.rowcount
        conn.close()
        logger.info(f"Inserted {inserted} summary records")
        return inserted

    except sqlite3.Error as e:
        logger.error(f"Error inserting summary: {e}")
        raise

def get_db_info() -> dict:
    """Get database statistics"""
    return {
        'db_path': DB_PATH,
        'events_count': get_events_count(),
        'summary_count': get_summary_count(),
        'db_exists': Path(DB_PATH).exists()
    }


if __name__ == '__main__':
    # Test database initialization
    import logging
    logging.basicConfig(level=logging.INFO)
    init_db()
    print("Database info:", get_db_info())