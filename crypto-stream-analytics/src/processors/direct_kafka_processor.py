"""
Direct Kafka Consumer Processor - Simple Python-based stream processing.

Processes insights directly from Kafka without Spark/Flink complexity.
Actually gets data into the database!
"""

from confluent_kafka import Consumer, KafkaError
import json
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
import time

class DirectKafkaProcessor:
    def __init__(self, db_path='enhanced_crypto_insights.db'):
        self.db_path = db_path
        self.init_database()

        # In-memory windowed data
        self.price_windows = defaultdict(list)  # symbol -> [(timestamp, price, volume)]
        self.sentiment_data = None  # Latest Fear & Greed data

        # Kafka consumers
        self.consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'direct-processor',
            'auto.offset.reset': 'latest'
        }

    def init_database(self):
        """Initialize SQLite database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Volume spikes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS volume_spikes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spike_time TIMESTAMP,
                symbol TEXT,
                volume REAL,
                price_at_spike REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Price movements
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                symbol TEXT,
                price_change_pct REAL,
                avg_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Market sentiment
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_sentiment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_time TIMESTAMP,
                fear_greed_index INTEGER,
                fear_greed_class TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Sentiment-price correlation
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sentiment_price_correlation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                fear_greed_index INTEGER,
                fear_greed_class TEXT,
                btc_price_change REAL,
                eth_price_change REAL,
                market_direction TEXT,
                correlation_signal TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()
        print(f"[OK] Database initialized: {self.db_path}\n")

    def process_volume_spikes(self, data):
        """Detect and store volume spikes."""
        volume = data.get('volume', 0)
        if volume > 1000000:  # Simple threshold
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            spike_time = datetime.fromtimestamp(data['timestamp'] / 1000)
            cursor.execute('''
                INSERT INTO volume_spikes (spike_time, symbol, volume, price_at_spike)
                VALUES (?, ?, ?, ?)
            ''', (spike_time, data['symbol'], volume, data['price']))

            conn.commit()
            conn.close()
            print(f"[SPIKE] {data['symbol']}: Volume {volume:,.0f} at ${data['price']:,.2f}")

    def process_price_window(self, data):
        """Process 15-minute price windows."""
        symbol = data['symbol']
        timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
        price = data['price']
        volume = data.get('volume', 0)

        # Add to window
        self.price_windows[symbol].append((timestamp, price, volume))

        # Clean old data (keep last 20 minutes)
        cutoff = datetime.now() - timedelta(minutes=20)
        self.price_windows[symbol] = [
            (t, p, v) for t, p, v in self.price_windows[symbol]
            if t > cutoff
        ]

        # Calculate 15-min window if we have enough data
        if len(self.price_windows[symbol]) >= 10:  # At least 10 price points
            prices = [p for t, p, v in self.price_windows[symbol]]
            volumes = [v for t, p, v in self.price_windows[symbol]]
            timestamps = [t for t, p, v in self.price_windows[symbol]]

            start_price = prices[0]
            end_price = prices[-1]
            avg_price = sum(prices) / len(prices)
            avg_volume = sum(volumes) / len(volumes)
            price_change_pct = ((end_price - start_price) / start_price * 100)

            # Store to database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            window_start = timestamps[0]
            window_end = timestamps[-1]

            cursor.execute('''
                INSERT INTO price_movements
                (window_start, window_end, symbol, price_change_pct, avg_price)
                VALUES (?, ?, ?, ?, ?)
            ''', (window_start, window_end, symbol, price_change_pct, avg_price))

            conn.commit()
            conn.close()

            direction = "UP" if price_change_pct > 0 else "DOWN"
            print(f"[WINDOW] {symbol}: {direction} {price_change_pct:+.2f}% (${avg_price:,.2f} avg)")

    def process_sentiment(self, data):
        """Process Fear & Greed Index."""
        self.sentiment_data = {
            'value': data['value'],
            'classification': data['classification'],
            'timestamp': datetime.fromtimestamp(data['timestamp'] / 1000)
        }

        # Store to database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO market_sentiment (recorded_time, fear_greed_index, fear_greed_class)
            VALUES (?, ?, ?)
        ''', (self.sentiment_data['timestamp'], data['value'], data['classification']))

        conn.commit()
        conn.close()

        print(f"[SENTIMENT] {data['classification']}: {data['value']}/100")

        # Check for correlation with prices
        self.check_sentiment_correlation()

    def check_sentiment_correlation(self):
        """Correlate sentiment with recent price movements."""
        if not self.sentiment_data:
            return

        # Get recent BTC and ETH price changes
        btc_data = self.price_windows.get('BTCUSDT', [])
        eth_data = self.price_windows.get('ETHUSDT', [])

        if len(btc_data) < 5 or len(eth_data) < 5:
            return

        # Calculate price changes
        btc_prices = [p for t, p, v in btc_data[-10:]]
        eth_prices = [p for t, p, v in eth_data[-10:]]

        btc_change = ((btc_prices[-1] - btc_prices[0]) / btc_prices[0] * 100)
        eth_change = ((eth_prices[-1] - eth_prices[0]) / eth_prices[0] * 100)

        # Determine market direction
        if btc_change > 0 and eth_change > 0:
            market_direction = "BULLISH"
        elif btc_change < 0 and eth_change < 0:
            market_direction = "BEARISH"
        else:
            market_direction = "MIXED"

        # Correlation signal
        fear_greed = self.sentiment_data['value']
        if fear_greed < 25 and market_direction == "BEARISH":
            signal = "BUY_SIGNAL"
        elif fear_greed > 75 and market_direction == "BULLISH":
            signal = "SELL_SIGNAL"
        elif fear_greed < 25 and market_direction == "BULLISH":
            signal = "REVERSAL_CONFIRMED"
        else:
            signal = "NEUTRAL"

        # Store correlation
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        window_end = datetime.now()
        window_start = window_end - timedelta(minutes=15)

        cursor.execute('''
            INSERT INTO sentiment_price_correlation
            (window_start, window_end, fear_greed_index, fear_greed_class,
             btc_price_change, eth_price_change, market_direction, correlation_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (window_start, window_end, fear_greed, self.sentiment_data['classification'],
              btc_change, eth_change, market_direction, signal))

        conn.commit()
        conn.close()

        print(f"[CORRELATION] {signal}: F&G={fear_greed}, Market={market_direction}, BTC={btc_change:+.2f}%, ETH={eth_change:+.2f}%")

    def start(self):
        """Start processing Kafka streams."""
        print("Starting Direct Kafka Processor")
        print("Reading from Kafka topics:")
        print("  - crypto-prices (Binance)")
        print("  - crypto-fear-greed (Fear & Greed Index)")
        print(f"Writing to: {self.db_path}\n")

        # Create consumers for each topic
        price_consumer = Consumer(self.consumer_config)
        sentiment_consumer = Consumer(self.consumer_config)

        price_consumer.subscribe(['crypto-prices'])
        sentiment_consumer.subscribe(['crypto-fear-greed'])

        print("[OK] Started processing. Press Ctrl+C to stop\n")

        message_count = 0

        try:
            while True:
                # Poll price stream
                msg = price_consumer.poll(0.1)
                if msg is not None and not msg.error():
                    try:
                        data = json.loads(msg.value().decode('utf-8'))
                        self.process_volume_spikes(data)
                        self.process_price_window(data)
                        message_count += 1
                    except Exception as e:
                        print(f"Error processing price: {e}")

                # Poll sentiment stream
                msg = sentiment_consumer.poll(0.1)
                if msg is not None and not msg.error():
                    try:
                        data = json.loads(msg.value().decode('utf-8'))
                        self.process_sentiment(data)
                        message_count += 1
                    except Exception as e:
                        print(f"Error processing sentiment: {e}")

                # Print stats every 100 messages
                if message_count % 100 == 0 and message_count > 0:
                    print(f"\n[STATS] Processed {message_count} messages\n")

        except KeyboardInterrupt:
            print(f"\n\nStopping... Processed {message_count} messages total")
            price_consumer.close()
            sentiment_consumer.close()


if __name__ == "__main__":
    processor = DirectKafkaProcessor()
    processor.start()
