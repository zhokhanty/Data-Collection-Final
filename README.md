# GDACS Pipeline - Quick Setup Guide

## Prerequisites
- Python 3.8+
- Docker & Docker Compose (for Kafka)
- Git

## Step 1: Clone Repository
```bash
git clone <your-repo-url>
cd Data-Collection-Final
```

## Step 2: Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

## Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

## Step 4: Create Environment File
```bash
cp .env.example .env
# Edit .env with your local settings (usually defaults are fine)
```

## Step 5: Create Data & Log Directories
```bash
mkdir -p data logs airflow/logs
```

## Step 6: Initialize Database
```bash
python -c "from src.db_utils import init_db; init_db()"
```

## Step 7: Start Kafka (Using Docker Compose)

Create `docker-compose.yml` in project root:

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Then run:
```bash
docker-compose up -d
```

## Step 8: Initialize Airflow
```bash
export AIRFLOW_HOME=./airflow
airflow db init
```

## Step 9: Create Airflow User
```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

## Step 10: Start Airflow Services
```bash
# Terminal 1: Start scheduler
airflow scheduler

# Terminal 2: Start web server
airflow webserver --port 8080
```

Then visit: http://localhost:8080 (username: admin, password: admin)

## Step 11: Verify Configuration
```bash
python -c "from config import *; print('Config loaded successfully!')"
python -c "from src.kafka_utils import topic_exists; print(f'Kafka connected: {topic_exists(\"raw_events\")}')"
```

## Testing Individual Components

### Test Database
```bash
python -c "from src.db_utils import get_db_info; print(get_db_info())"
```

### Test Kafka Connection
```bash
python -c "from src.kafka_utils import topic_exists; print(topic_exists('raw_events'))"
```

### Test API Connection
```bash
python -c "from src.job1_producer import fetch_from_api; events = fetch_from_api(); print(f'API returned {len(events)} events')"
```

## Project Structure
```
Data-Collection-Final/
├── src/
│   ├── job1_producer.py       # Person 1
│   ├── job2_cleaner.py        # Person 2
│   ├── job3_analytics.py      # Person 3
│   ├── db_utils.py            # Shared
│   ├── kafka_utils.py         # Shared
│   └── config.py              # Shared
├── airflow/
│   └── dags/
│       ├── job1_ingestion_dag.py    # Person 1
│       ├── job2_clean_store_dag.py  # Person 2
│       └── job3_daily_summary_dag.py # Person 3
├── data/
│   └── app.db                 # SQLite (auto-created)
├── logs/
├── requirements.txt
├── setup.py
├── .env
├── .env.example
├── .gitignore
└── README.md
```

## Troubleshooting

### Kafka connection refused
- Make sure Docker containers are running: `docker-compose up -d`
- Check: `docker-compose ps`

### Database locked
- Close any other Python processes accessing the database
- Delete and reinitialize: `rm data/app.db && python -c "from src.db_utils import init_db; init_db()"`

### Airflow DAGs not showing
- Ensure `AIRFLOW_HOME` is set correctly
- Restart scheduler: `airflow scheduler`
- Check logs: `cat logs/airflow/scheduler.log`

### ImportError: No module named 'src'
- Make sure you're in project root directory
- Check PYTHONPATH: `export PYTHONPATH="${PYTHONPATH}:$(pwd)"`
