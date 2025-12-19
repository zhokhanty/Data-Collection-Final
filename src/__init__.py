"""
GDACS Data Collection Pipeline

A real-time streaming + batch pipeline for collecting, cleaning, and analyzing
disaster event data from the Global Disaster Alert and Coordination System (GDACS).

Modules:
    job1_producer: API ingestion and Kafka publishing
    job2_cleaner: Kafka consumption and data cleaning
    job3_analytics: Daily aggregation and summary statistics
    db_utils: Database operations
    kafka_utils: Kafka producer/consumer utilities
    config: Configuration management
"""

__version__ = '1.0.0'
__author__ = 'Data Team'
__all__ = [
    'job1_producer',
    'job2_cleaner',
    'job3_analytics',
    'db_utils',
    'kafka_utils',
    'config'
]