import json
import logging
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW, KAFKA_CONSUMER_GROUP

logger = logging.getLogger(__name__)

def create_producer(bootstrap_servers=None):
    """
    Create and return a Kafka producer.
    
    Args:
        bootstrap_servers: List of broker addresses
    
    Returns:
        KafkaProducer instance
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"Kafka producer created: {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


def send_to_kafka(topic: str, events: list, producer=None) -> int:
    """
    Send events to Kafka topic.
    
    Args:
        topic: Kafka topic name
        events: List of event dictionaries
        producer: Optional KafkaProducer instance
    
    Returns:
        Number of events sent
    """
    if not events:
        logger.warning("No events to send")
        return 0

    close_producer = False
    if producer is None:
        producer = create_producer()
        close_producer = True

    sent_count = 0
    try:
        for event in events:
            future = producer.send(topic, event)
            try:
                record_metadata = future.get(timeout=10)
                logger.debug(f"Event sent to {topic}: partition {record_metadata.partition}, offset {record_metadata.offset}")
                sent_count += 1
            except KafkaError as e:
                logger.error(f"Failed to send event: {e}")

        producer.flush()
        logger.info(f"Sent {sent_count}/{len(events)} events to topic '{topic}'")
        return sent_count

    except Exception as e:
        logger.error(f"Error sending events: {e}")
        raise
    finally:
        if close_producer:
            producer.close()


def create_consumer(topic: str, group_id=None, auto_offset_reset='earliest', bootstrap_servers=None):
    """
    Create and return a Kafka consumer.
    
    Args:
        topic: Kafka topic to consume from
        group_id: Consumer group ID
        auto_offset_reset: 'earliest' or 'latest'
        bootstrap_servers: List of broker addresses
    
    Returns:
        KafkaConsumer instance
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
    if group_id is None:
        group_id = KAFKA_CONSUMER_GROUP

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000,  
            max_poll_records=500  
        )
        logger.info(f"Kafka consumer created for topic '{topic}', group '{group_id}'")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise


def consume_batch(topic: str, group_id=None) -> list:
    """
    Consume all available messages from a Kafka topic (batch mode).
    
    Args:
        topic: Kafka topic to consume from
        group_id: Consumer group ID
    
    Returns:
        List of message values
    """
    consumer = create_consumer(topic, group_id)
    messages = []

    try:
        for message in consumer:
            messages.append(message.value)
            logger.debug(f"Consumed message from offset {message.offset}")

        logger.info(f"Consumed {len(messages)} messages from topic '{topic}'")
        return messages

    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        return []
    finally:
        consumer.close()


def topic_exists(topic: str, bootstrap_servers=None) -> bool:
    """Check if a topic exists"""
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=5000
        )
        topics = admin.list_topics()
        admin.close()
        return topic in topics
    except Exception as e:
        logger.error(f"Error checking topic: {e}")
        return False


def create_topic(topic: str, num_partitions=1, replication_factor=1, bootstrap_servers=None):
    """Create a Kafka topic"""
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    if topic_exists(topic, bootstrap_servers):
        logger.info(f"Topic '{topic}' already exists")
        return True

    try:
        from kafka.admin import NewTopic
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topic_list = [NewTopic(topic, num_partitions, replication_factor)]
        admin.create_topics(new_topics=topic_list, validate_only=False)
        admin.close()
        logger.info(f"Topic '{topic}' created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        return False


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print(f"Checking Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic exists: {topic_exists(KAFKA_TOPIC_RAW)}")