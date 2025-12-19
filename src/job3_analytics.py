import logging
import pandas as pd
from datetime import datetime
from db_utils import init_db
from kafka_utils import get_kafka_config, create_producer, send_to_kafka

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run_analytics():
    kafka_config = get_kafka_config()
    analytics_topic = kafka_config['analytics_topic']

    producer = create_producer()

    with init_db() as conn:
        df = pd.read_sql_query("SELECT * FROM events", conn)

        if df.empty:
            logger.info("Нет данных для анализа")
            return

        logger.info(f"Всего событий: {len(df)}")
        logger.info(f"Уникальных стран: {df['country'].nunique()}")

        event_type_stats = df.groupby('event_type')['severity'].agg(['count','mean','max','min']).reset_index()
        logger.info("Метрики по типу событий:\n%s", event_type_stats)

        country_stats = df.groupby('country')['severity'].agg(['count','mean','max','min']).reset_index()
        logger.info("Метрики по странам (топ 10):\n%s", country_stats.head(10))

        top_severe = df.nlargest(3, 'severity')
        logger.info("Топ-3 самых серьезных событий:\n%s", top_severe[['event_type','country','severity','event_time']])

        summary = {
            'calculation_date': datetime.utcnow().isoformat(),
            'total_events': len(df),
            'unique_countries': df['country'].nunique(),
            'max_severity': df['severity'].max(),
            'top_event_type': df.groupby('event_type')['severity'].sum().idxmax(),
            'top_country': df.groupby('country')['severity'].sum().idxmax()
        }

        # Сохранение в SQLite
        conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            calculation_date TEXT UNIQUE,
            total_events INTEGER,
            unique_countries INTEGER,
            max_severity REAL,
            top_event_type TEXT,
            top_country TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.execute("""
        INSERT OR REPLACE INTO daily_summary (
            calculation_date, total_events, unique_countries, max_severity, top_event_type, top_country
        ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            summary['calculation_date'],
            summary['total_events'],
            summary['unique_countries'],
            summary['max_severity'],
            summary['top_event_type'],
            summary['top_country']
        ))
        conn.commit()
        logger.info("Ежедневная сводка сохранена в daily_summary")

        try:
            send_to_kafka(analytics_topic, [summary], producer)
            logger.info(f"Сводка отправлена в Kafka топик '{analytics_topic}'")
        except Exception as e:
            logger.error(f"Ошибка отправки в Kafka: {e}")

        return summary

if __name__ == "__main__":
    summary = run_analytics()
    if summary:
        logger.info("Сводка аналитики:\n%s", summary)
