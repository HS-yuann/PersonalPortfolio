"""
Fear and Greed Index producer for crypto market sentiment.

Fetches the crypto Fear and Greed Index from Alternative.me API and streams to Kafka.
This index provides a measure of market sentiment from 0 (Extreme Fear) to 100 (Extreme Greed).
"""

import requests
import json
import time
from datetime import datetime
from confluent_kafka import Producer
import socket


class FearGreedProducer:
    """
    Streams crypto Fear and Greed Index data to Kafka.

    Fetches sentiment data from Alternative.me API and publishes to Kafka
    for correlation with price movements and news sentiment.
    """

    def __init__(self, kafka_topic='crypto-fear-greed'):
        """
        Initialize Fear and Greed Index producer.

        Args:
            kafka_topic (str): Kafka topic for fear/greed data
        """
        self.topic = kafka_topic
        self.api_url = 'https://api.alternative.me/fng/'
        self.message_count = 0
        self.start_time = time.time()
        self.last_value = None
        self.last_timestamp = None

        conf = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': socket.gethostname(),
            'linger.ms': 10,
            'compression.type': 'snappy',
        }

        self.kafka_producer = Producer(conf)
        print("[OK] Kafka producer initialized")
        print(f"[OK] Fear and Greed Index API: {self.api_url}\n")

    def delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            print(f'[ERROR] Delivery failed: {err}')
        else:
            if self.message_count % 5 == 0:
                print(f'[OK] {self.message_count} fear/greed updates sent to Kafka')

    def fetch_fear_greed_index(self):
        """Fetch latest Fear and Greed Index from API."""
        try:
            response = requests.get(
                f"{self.api_url}?limit=1&format=json",
                timeout=10
            )
            response.raise_for_status()

            data = response.json()

            if data.get('metadata', {}).get('error'):
                print(f"API error: {data['metadata']['error']}")
                return None

            if data.get('data') and len(data['data']) > 0:
                return data['data'][0]

            return None

        except Exception as e:
            print(f"Error fetching Fear and Greed Index: {e}")
            return None

    def process_fear_greed_data(self, raw_data):
        """Process Fear and Greed Index data."""
        value = int(raw_data['value'])
        classification = raw_data['value_classification']
        timestamp = int(raw_data['timestamp'])
        time_until_update = raw_data.get('time_until_update')

        next_update = None
        if time_until_update:
            next_update = int(time.time()) + int(time_until_update)

        return {
            'type': 'fear_greed_index',
            'value': value,
            'classification': classification,
            'index_timestamp': timestamp,
            'index_human_time': datetime.fromtimestamp(timestamp).isoformat(),
            'time_until_update': int(time_until_update) if time_until_update else None,
            'next_update': next_update,
            'timestamp': int(time.time() * 1000),
            'human_time': datetime.now().isoformat()
        }

    def send_to_kafka(self, data):
        """Send processed data to Kafka."""
        try:
            self.kafka_producer.produce(
                topic=self.topic,
                key=data['classification'],
                value=json.dumps(data),
                callback=self.delivery_report
            )
            self.kafka_producer.poll(0)
            self.message_count += 1

        except Exception as e:
            print(f"Error sending to Kafka: {e}")

    def get_sentiment_emoji(self, classification):
        """Get label for sentiment classification."""
        label_map = {
            'Extreme Fear': '[EXTREME FEAR]',
            'Fear': '[FEAR]',
            'Neutral': '[NEUTRAL]',
            'Greed': '[GREED]',
            'Extreme Greed': '[EXTREME GREED]'
        }
        return label_map.get(classification, '[UNKNOWN]')

    def start(self):
        """Start polling Fear and Greed Index and streaming to Kafka."""
        print(f"Starting Fear and Greed Index -> Kafka pipeline")
        print(f"API: {self.api_url}")
        print(f"Kafka topic: {self.topic}")
        print(f"Poll interval: 21,600 seconds (6 hours)\n")

        try:
            while True:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching Fear and Greed Index...")

                raw_data = self.fetch_fear_greed_index()

                if raw_data:
                    current_timestamp = raw_data['timestamp']
                    current_value = raw_data['value']

                    if (self.last_timestamp != current_timestamp or
                        self.last_value != current_value):

                        processed = self.process_fear_greed_data(raw_data)
                        self.send_to_kafka(processed)

                        self.last_timestamp = current_timestamp
                        self.last_value = current_value

                        sentiment_label = self.get_sentiment_emoji(processed['classification'])
                        print(f"\n  {sentiment_label}")
                        print(f"  Value: {processed['value']}/100")
                        print(f"  Classification: {processed['classification']}")
                        print(f"  Index Time: {processed['index_human_time']}")

                        if processed['time_until_update']:
                            hours_until_update = processed['time_until_update'] / 3600
                            print(f"  Next update in: {hours_until_update:.1f} hours")

                        print()
                    else:
                        print(f"  -> No new update (still {raw_data['value_classification']}: {raw_data['value']}/100)\n")

                elapsed = time.time() - self.start_time
                rate = self.message_count / (elapsed / 60) if elapsed > 0 else 0
                print(f"--- Stats: {self.message_count} updates sent, "
                      f"{rate:.2f} updates/hour ---\n")

                poll_interval = 21600
                print(f"Waiting {poll_interval} seconds (6 hours) before next poll...\n")
                time.sleep(poll_interval)

        except KeyboardInterrupt:
            print(f"\n\n[OK] Stopping. Total: {self.message_count} updates streamed.")
            self.kafka_producer.flush()


if __name__ == "__main__":
    producer = FearGreedProducer()
    producer.start()
