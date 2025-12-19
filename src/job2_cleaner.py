import logging
from db_utils import init_db, insert_events
from kafka_utils import get_kafka_config, create_consumer, create_topic
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

VALID_EVENT_TYPES = {'EQ', 'TC', 'FL', 'DR', 'VO', 'WF', 'EW', 'OT'}
VALID_SEVERITY_RANGE = (0, 10)
VALID_LATITUDE_RANGE = (-90, 90)
VALID_LONGITUDE_RANGE = (-180, 180)

def clean_record(raw):
    """Clean and validate a single GDACS event"""
    props = raw.get('properties', {})
    geom = raw.get('geometry', {})
    coords = geom.get('coordinates', [0, 0])

    record = {
        'event_id': str(props.get('eventid', '')),
        'event_type': props.get('eventtype', ''),
        'country': props.get('country', ''),
        'severity': float(props.get('alertscore', 0)),
        'latitude': float(coords[1] or 0),
        'longitude': float(coords[0] or 0),
        'event_time': props.get('fromdate'),
        'ingestion_time': raw.get('ingestion_timestamp', datetime.utcnow().isoformat())
    }

    if not record['event_id'] or record['event_type'] not in VALID_EVENT_TYPES:
        logger.warning(f"Invalid record skipped: {record}")
        return None
    if not (VALID_SEVERITY_RANGE[0] <= record['severity'] <= VALID_SEVERITY_RANGE[1]):
        logger.warning(f"Invalid severity, record skipped: {record}")
        return None
    if not (VALID_LATITUDE_RANGE[0] <= record['latitude'] <= VALID_LATITUDE_RANGE[1]):
        logger.warning(f"Invalid latitude, record skipped: {record}")
        return None
    if not (VALID_LONGITUDE_RANGE[0] <= record['longitude'] <= VALID_LONGITUDE_RANGE[1]):
        logger.warning(f"Invalid longitude, record skipped: {record}")
        return None

    return record

def clean_batch(raw_messages):
    """Clean a batch of raw events"""
    cleaned = []
    for msg in raw_messages:
        rec = clean_record(msg)
        if rec:
            cleaned.append(rec)
    return cleaned


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run_cleaner(batch_size=1000):
    """Reads raw messages from Kafka, cleans, stores to SQLite"""
    config = get_kafka_config()
    create_topic(config['raw_topic'])
    if not init_db():
        return

    consumer = create_consumer(config['raw_topic'], group_id=config['consumer_groups']['raw'])
    raw_messages = []
    try:
        for msg in consumer:
            if msg.value:
                raw_messages.append(msg.value)
            if len(raw_messages) >= batch_size:
                break
    finally:
        consumer.close()

    if not raw_messages:
        logger.info("No new messages to process")
        return

    cleaned = clean_batch(raw_messages)
    inserted = insert_events(cleaned)
    logger.info(f"Inserted {inserted}/{len(cleaned)} cleaned events into DB")

if __name__ == "__main__":
    run_cleaner()
