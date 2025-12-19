import json
import time
import logging
import requests
from datetime import datetime
from typing import List, Dict
from config import (
   API_URL,
   API_TIMEOUT,
   API_FETCH_INTERVAL_SECONDS,
   MAX_RETRIES,
   RETRY_DELAY_SECONDS,
   DEBUG
)
from kafka_utils import create_producer, send_to_kafka

# Setup logging
logging.basicConfig(
   level=logging.DEBUG if DEBUG else logging.INFO,
   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_from_api(retry_count: int = 0) -> List[Dict]:
   try:
      logger.info(f"Fetching from GDACS API: {API_URL}")
      
      response = requests.get(
         API_URL,
         timeout=API_TIMEOUT,
         headers={
               'User-Agent': 'GDACS-Pipeline/1.0',
               'Accept': 'application/json'
         }
      )
      
      response.raise_for_status()
      
      data = response.json()
      
      if not isinstance(data, dict):
         logger.error(f"Invalid API response format: {type(data)}")
         return []
      
      features = data.get("features", [])
      
      if not features:
         logger.warning("API returned no events")
         return []
      
      logger.info(f"Successfully fetched {len(features)} events from API")
      logger.debug(f"API Response metadata: type={data.get('type')}, "
                  f"features_count={len(features)}")
      
      return features
   
   except requests.exceptions.Timeout as e:
      logger.error(f"API request timeout: {e}")
      return handle_retry(retry_count, f"Timeout: {e}")
   
   except requests.exceptions.ConnectionError as e:
      logger.error(f"Connection error: {e}")
      return handle_retry(retry_count, f"Connection error: {e}")
   
   except requests.exceptions.HTTPError as e:
      logger.error(f"HTTP error: {e.response.status_code} - {e}")
      return handle_retry(retry_count, f"HTTP {e.response.status_code}")
   
   except json.JSONDecodeError as e:
      logger.error(f"Failed to parse API response as JSON: {e}")
      return handle_retry(retry_count, f"JSON decode error: {e}")
   
   except requests.RequestException as e:
      logger.error(f"Request exception: {e}")
      return handle_retry(retry_count, f"Request error: {e}")
   
   except Exception as e:
      logger.error(f"Unexpected error fetching from API: {e}", exc_info=True)
      return []


def handle_retry(retry_count: int, error_msg: str) -> List[Dict]:
   if retry_count < MAX_RETRIES:
      retry_count += 1
      wait_time = RETRY_DELAY_SECONDS * retry_count
      logger.warning(
         f"Retrying API call ({retry_count}/{MAX_RETRIES}) "
         f"after {wait_time}s delay. Error: {error_msg}"
      )
      time.sleep(wait_time)
      return fetch_from_api(retry_count)
   else:
      logger.error(f"Max retries ({MAX_RETRIES}) exceeded. Giving up.")
      return []



def send_events_to_kafka(events: List[Dict], topic: str = "raw_events") -> int:
   if not events:
      logger.warning("No events to send to Kafka")
      return 0
   
   try:
      logger.info(f"Sending {len(events)} events to Kafka topic '{topic}'")
      sent_count = send_to_kafka(topic, events)
      logger.info(f"Successfully sent {sent_count}/{len(events)} events")
      return sent_count
   
   except Exception as e:
      logger.error(f"Failed to send events to Kafka: {e}", exc_info=True)
      return 0


def run_producer(once: bool = False) -> Dict:
   start_time = datetime.utcnow()
   logger.info("=" * 80)
   logger.info("STARTING DATA INGESTION JOB (Job 1)")
   logger.info("=" * 80)
   
   try:
      logger.info("Step 1: Fetching data from GDACS API...")
      events = fetch_from_api()
      total_fetched = len(events)
      
      if total_fetched == 0:
         logger.warning("No events fetched from API")
         return {
            'timestamp': start_time.isoformat(),
            'total_fetched': 0,
            'total_sent': 0,
            'failed': 0,
            'status': 'no_data'
         }
      
      logger.info(f"✓ Fetched {total_fetched} events")
      
      logger.info("Step 2: Sending events to Kafka...")
      total_sent = send_events_to_kafka(events)
      failed = total_fetched - total_sent
      
      logger.info(f"✓ Sent {total_sent} events to Kafka")
      
      end_time = datetime.utcnow()
      duration = (end_time - start_time).total_seconds()
      
      result = {
         'timestamp': start_time.isoformat(),
         'total_fetched': total_fetched,
         'total_sent': total_sent,
         'failed': failed,
         'duration_seconds': duration,
         'status': 'success' if total_sent > 0 else 'partial_failure'
      }
      
      logger.info("=" * 80)
      logger.info(f"JOB COMPLETE - Status: {result['status']}")
      logger.info(f"  Fetched: {total_fetched}")
      logger.info(f"  Sent: {total_sent}")
      logger.info(f"  Failed: {failed}")
      logger.info(f"  Duration: {duration:.2f}s")
      logger.info("=" * 80)
      
      return result
   
   except Exception as e:
      logger.error(f"Producer job failed: {e}", exc_info=True)
      return {
         'timestamp': start_time.isoformat(),
         'total_fetched': 0,
         'total_sent': 0,
         'failed': 1,
         'status': 'error',
         'error': str(e)
      }

def run_continuous(interval_seconds: int = API_FETCH_INTERVAL_SECONDS):
   logger.info(f"Starting continuous ingestion (interval: {interval_seconds}s)")
   
   try:
      while True:
         run_producer(once=True)
         logger.info(f"Waiting {interval_seconds}s before next fetch...")
         time.sleep(interval_seconds)
   
   except KeyboardInterrupt:
      logger.info("Continuous ingestion stopped by user")
   except Exception as e:
      logger.error(f"Continuous ingestion error: {e}", exc_info=True)

if __name__ == "__main__":
   import sys
   
   if "--continuous" in sys.argv:
      run_continuous()
   else:
      result = run_producer(once=True)
      print(json.dumps(result, indent=2))