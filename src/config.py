import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Directories
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
LOGS_DIR = PROJECT_ROOT / 'logs'
AIRFLOW_HOME = PROJECT_ROOT / 'airflow'

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
AIRFLOW_HOME.mkdir(exist_ok=True)

# DB config
DB_PATH = str(DATA_DIR / 'app.db')
DB_URL = f'sqlite:///{DB_PATH}'

# Kafka config
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_BOOTSTRAP_SERVERS = [KAFKA_BROKER]
KAFKA_TOPIC_RAW = 'raw_events'
KAFKA_CONSUMER_GROUP = 'batch_cleaner'
KAFKA_AUTO_OFFSET_RESET = 'earliest'
KAFKA_CONSUMER_TIMEOUT_MS = 10000

API_URL = (
    "https://www.gdacs.org/gdacsapi/api/Events/"
    "geteventlist/SEARCH?eventlist=EQ;TC;FL"
)
API_TIMEOUT = 30
API_FETCH_INTERVAL_SECONDS = 60 # Fetch new events every minute

# Logging config
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = str(LOGS_DIR / 'pipeline.log')

# Airflow config
AIRFLOW_DAG_FOLDER = str(AIRFLOW_HOME / 'dags')
AIRFLOW_LOG_FOLDER = str(LOGS_DIR / 'airflow')
AIRFLOW_EXECUTOR = os.getenv('AIRFLOW_EXECUTOR', 'SequentialExecutor')
AIRFLOW_DB_URL = f'sqlite:///{AIRFLOW_HOME / "airflow.db"}'

# Data validation constants
VALID_EVENT_TYPES = {'EQ', 'TC', 'FL', 'DR', 'VO', 'WF', 'EW', 'OT'}
VALID_SEVERITY_RANGE = (0, 10)
VALID_LATITUDE_RANGE = (-90, 90)
VALID_LONGITUDE_RANGE = (-180, 180)


SUMMARY_TABLE_NAME = 'daily_summary'
EVENTS_TABLE_NAME = 'events'

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

if __name__ == '__main__':
    print("=" * 80)
    print("CONFIGURATION LOADED")
    print("=" * 80)
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Database: {DB_PATH}")
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"API URL: {API_URL}")
    print(f"Log Level: {LOG_LEVEL}")
    print(f"Debug Mode: {DEBUG}")
    print("=" * 80)
