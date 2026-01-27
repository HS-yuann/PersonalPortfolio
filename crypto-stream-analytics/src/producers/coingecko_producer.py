"""
CoinGecko API producer for crypto market data.

Fetches market metrics (market cap, volume, price changes) and
streams to Kafka for correlation with news and sentiment.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pycoingecko import CoinGeckoAPI
import json
import time
from datetime import datetime
from confluent_kafka import Producer
import socket
from dotenv import load_dotenv

load_dotenv()


class CoinGeckoProducer:
    """
    Streams crypto market data from CoinGecko API to Kafka.

    Fetches market metrics like market cap, 24h volume, price changes,
    and trending coins.
    """

    def __init__(self, kafka_topic='crypto-market-data', api_key=None):
        """
        Initialize CoinGecko producer.

        Args:
            kafka_topic (str): Kafka topic for market data
            api_key (str): CoinGecko API key (uses env var if not provided)
        """
        self.topic = kafka_topic
        self.api_key = api_key or os.getenv('COINGECKO_API_KEY')
        self.message_count = 0
        self.start_time = time.time()

        self.cg = CoinGeckoAPI(demo_api_key=self.api_key)
        print("[OK] CoinGecko API client initialized")

        self.coin_ids = ['bitcoin', 'ethereum', 'binancecoin']

        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        conf = {
            'bootstrap.servers': kafka_servers,
            'client.id': socket.gethostname(),
            'linger.ms': 10,
            'compression.type': 'snappy',
        }

        self.kafka_producer = Producer(conf)
        print(f"[OK] Kafka producer initialized\n")

    def delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            print(f'[ERROR] Delivery failed: {err}')
        else:
            if self.message_count % 10 == 0:
                print(f'[OK] {self.message_count} market updates sent to Kafka')

    def fetch_market_data(self):
        """Fetch current market data for tracked coins."""
        try:
            data = self.cg.get_coins_markets(
                vs_currency='usd',
                ids=','.join(self.coin_ids),
                order='market_cap_desc',
                sparkline=False,
                price_change_percentage='1h,24h,7d'
            )
            return data
        except Exception as e:
            print(f"Error fetching CoinGecko data: {e}")
            return []

    def fetch_trending_coins(self):
        """Fetch currently trending coins."""
        try:
            trending = self.cg.get_search_trending()
            return trending.get('coins', [])
        except Exception as e:
            print(f"Error fetching trending coins: {e}")
            return []

    def process_market_data(self, coin_data):
        """Extract and format market data."""
        return {
            'type': 'market_data',
            'coin_id': coin_data['id'],
            'symbol': coin_data['symbol'].upper(),
            'name': coin_data['name'],
            'current_price': coin_data['current_price'],
            'market_cap': coin_data['market_cap'],
            'market_cap_rank': coin_data['market_cap_rank'],
            'total_volume': coin_data['total_volume'],
            'high_24h': coin_data.get('high_24h'),
            'low_24h': coin_data.get('low_24h'),
            'price_change_24h': coin_data.get('price_change_24h'),
            'price_change_percentage_24h': coin_data.get('price_change_percentage_24h'),
            'price_change_percentage_1h': coin_data.get('price_change_percentage_1h_in_currency'),
            'price_change_percentage_7d': coin_data.get('price_change_percentage_7d_in_currency'),
            'market_cap_change_24h': coin_data.get('market_cap_change_24h'),
            'market_cap_change_percentage_24h': coin_data.get('market_cap_change_percentage_24h'),
            'circulating_supply': coin_data.get('circulating_supply'),
            'total_supply': coin_data.get('total_supply'),
            'ath': coin_data.get('ath'),
            'ath_change_percentage': coin_data.get('ath_change_percentage'),
            'timestamp': int(time.time() * 1000),
            'human_time': datetime.now().isoformat()
        }

    def send_to_kafka(self, data):
        """Send processed data to Kafka."""
        try:
            self.kafka_producer.produce(
                topic=self.topic,
                key=data['symbol'],
                value=json.dumps(data),
                callback=self.delivery_report
            )
            self.kafka_producer.poll(0)
            self.message_count += 1
        except Exception as e:
            print(f"Error sending to Kafka: {e}")

    def start(self):
        """Start polling CoinGecko and streaming to Kafka."""
        print(f"Starting CoinGecko -> Kafka pipeline")
        print(f"Tracking: {', '.join(self.coin_ids)}")
        print(f"Kafka topic: {self.topic}")
        print(f"Poll interval: 120 seconds\n")

        try:
            while True:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching market data...")

                market_data = self.fetch_market_data()

                for coin_data in market_data:
                    processed = self.process_market_data(coin_data)
                    self.send_to_kafka(processed)

                    print(f"  {processed['symbol']}: ${processed['current_price']:,.2f} "
                          f"| MCap: ${processed['market_cap']/1e9:.2f}B "
                          f"| 24h: {processed['price_change_percentage_24h']:+.2f}% "
                          f"| Vol: ${processed['total_volume']/1e9:.2f}B")

                trending = self.fetch_trending_coins()
                if trending:
                    trending_names = [c['item']['name'] for c in trending[:5]]
                    print(f"\n  [TRENDING]: {', '.join(trending_names)}\n")

                elapsed = time.time() - self.start_time
                rate = self.message_count / elapsed if elapsed > 0 else 0
                print(f"\n--- Stats: {self.message_count} updates sent, "
                      f"{rate:.2f} updates/min ---\n")

                print(f"Waiting 120 seconds before next poll...\n")
                time.sleep(120)

        except KeyboardInterrupt:
            print(f"\n\n[OK] Stopping. Total: {self.message_count} updates streamed.")
            self.kafka_producer.flush()


if __name__ == "__main__":
    producer = CoinGeckoProducer()
    producer.start()
