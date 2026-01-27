"""
Optimized Direct Kafka Consumer Processor with Dynamic Thresholds.

Improvements:
- Dynamic volume spike detection based on rolling averages
- Adaptive price movement thresholds
- Statistical anomaly detection
- Better sentiment correlation logic
- Configurable sensitivity levels
"""

from confluent_kafka import Consumer, KafkaError
import json
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict, deque
import time
import statistics

class OptimizedKafkaProcessor:
    def __init__(self, db_path='enhanced_crypto_insights.db'):
        self.db_path = db_path
        self.init_database()

        # In-memory windowed data with deques for efficient rolling windows
        self.price_windows = defaultdict(lambda: deque(maxlen=100))  # symbol -> deque[(timestamp, price, volume)]
        self.volume_history = defaultdict(lambda: deque(maxlen=50))  # symbol -> deque[volume] for baseline
        self.sentiment_data = None  # Latest Fear & Greed data

        # Configuration thresholds (more sensitive for faster results)
        self.config = {
            # Volume spike: 1.5x rolling average (more sensitive)
            'volume_spike_multiplier': 1.5,
            'volume_baseline_periods': 15,  # fewer periods for faster baseline

            # Price movements: detect moves > 0.1% (very sensitive)
            'price_change_threshold': 0.1,  # % change to record
            'min_window_size': 3,  # fewer data points needed

            # Correlation settings
            'correlation_window_minutes': 10,
            'fear_threshold': 30,  # < 30 = fear
            'greed_threshold': 70,  # > 70 = greed
        }

        # Kafka consumers
        self.consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'optimized-processor',
            'auto.offset.reset': 'latest'
        }

    def init_database(self):
        """Initialize SQLite database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Volume spikes with baseline tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS volume_spikes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spike_time TIMESTAMP,
                symbol TEXT,
                volume REAL,
                volume_baseline REAL,
                spike_multiplier REAL,
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
                volatility REAL,
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

    def calculate_volume_baseline(self, symbol):
        """Calculate rolling average volume baseline."""
        if len(self.volume_history[symbol]) < 10:
            return None
        return statistics.mean(self.volume_history[symbol])

    def process_volume_spikes(self, data):
        """Detect and store volume spikes using dynamic baseline."""
        volume = data.get('volume', 0)
        symbol = data['symbol']

        # Update volume history
        self.volume_history[symbol].append(volume)

        # Get baseline
        baseline = self.calculate_volume_baseline(symbol)
        if baseline is None:
            return  # Not enough data yet

        # Detect spike
        spike_multiplier = volume / baseline if baseline > 0 else 0

        if spike_multiplier >= self.config['volume_spike_multiplier']:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            spike_time = datetime.fromtimestamp(data['timestamp'] / 1000)
            cursor.execute('''
                INSERT INTO volume_spikes
                (spike_time, symbol, volume, volume_baseline, spike_multiplier, price_at_spike)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (spike_time, symbol, volume, baseline, spike_multiplier, data['price']))

            conn.commit()
            conn.close()
            print(f"[SPIKE] {symbol}: Volume {volume:,.0f} ({spike_multiplier:.1f}x baseline) at ${data['price']:,.2f}")

    def calculate_volatility(self, prices):
        """Calculate price volatility (standard deviation)."""
        if len(prices) < 2:
            return 0
        try:
            return statistics.stdev(prices)
        except:
            return 0

    def process_price_window(self, data):
        """Process price windows with volatility tracking."""
        symbol = data['symbol']
        timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
        price = data['price']
        volume = data.get('volume', 0)

        # Add to window
        self.price_windows[symbol].append((timestamp, price, volume))

        # Clean old data (keep last 20 minutes)
        cutoff = datetime.now() - timedelta(minutes=20)
        while self.price_windows[symbol] and self.price_windows[symbol][0][0] < cutoff:
            self.price_windows[symbol].popleft()

        # Calculate window if we have enough data
        if len(self.price_windows[symbol]) >= self.config['min_window_size']:
            data_points = list(self.price_windows[symbol])
            prices = [p for t, p, v in data_points]
            timestamps = [t for t, p, v in data_points]

            start_price = prices[0]
            end_price = prices[-1]
            avg_price = statistics.mean(prices)
            volatility = self.calculate_volatility(prices)
            price_change_pct = ((end_price - start_price) / start_price * 100)

            # Only record significant price movements
            if abs(price_change_pct) >= self.config['price_change_threshold']:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                window_start = timestamps[0]
                window_end = timestamps[-1]

                cursor.execute('''
                    INSERT INTO price_movements
                    (window_start, window_end, symbol, price_change_pct, avg_price, volatility)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (window_start, window_end, symbol, price_change_pct, avg_price, volatility))

                conn.commit()
                conn.close()

                direction = "UP" if price_change_pct > 0 else "DOWN"
                print(f"[MOVE] {symbol}: {direction} {price_change_pct:+.2f}% (volatility: {volatility:.2f})")

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
        """Enhanced sentiment-price correlation with better signals."""
        if not self.sentiment_data:
            return

        # Get recent BTC and ETH price changes
        btc_data = list(self.price_windows.get('BTCUSDT', []))
        eth_data = list(self.price_windows.get('ETHUSDT', []))

        if len(btc_data) < 5 or len(eth_data) < 5:
            return

        # Calculate price changes (last 10 data points)
        btc_prices = [p for t, p, v in btc_data[-10:]]
        eth_prices = [p for t, p, v in eth_data[-10:]]

        btc_change = ((btc_prices[-1] - btc_prices[0]) / btc_prices[0] * 100)
        eth_change = ((eth_prices[-1] - eth_prices[0]) / eth_prices[0] * 100)

        # Determine market direction
        if btc_change > 0.3 and eth_change > 0.3:
            market_direction = "BULLISH"
        elif btc_change < -0.3 and eth_change < -0.3:
            market_direction = "BEARISH"
        else:
            market_direction = "MIXED"

        # Enhanced correlation signal logic (more sensitive)
        fear_greed = self.sentiment_data['value']
        signal = "NEUTRAL"

        # Strong contrarian signals
        if fear_greed < self.config['fear_threshold'] and market_direction == "BEARISH":
            signal = "STRONG_BUY"  # Extreme fear + bearish = buying opportunity
        elif fear_greed > self.config['greed_threshold'] and market_direction == "BULLISH":
            signal = "STRONG_SELL"  # Extreme greed + bullish = selling opportunity
        # Reversal signals
        elif fear_greed < self.config['fear_threshold'] and market_direction == "BULLISH":
            signal = "REVERSAL_UP"  # Fear but market rising = strong reversal
        elif fear_greed > self.config['greed_threshold'] and market_direction == "BEARISH":
            signal = "REVERSAL_DOWN"  # Greed but market falling = potential crash
        # Moderate signals (more sensitive thresholds)
        elif fear_greed < 45 and btc_change > 0.3:
            signal = "BUY_SIGNAL"  # Moderate fear with good price action
        elif fear_greed > 55 and btc_change < -0.3:
            signal = "SELL_SIGNAL"  # Moderate greed with negative price action
        # Always store correlation when we have valid data
        elif market_direction != "MIXED":
            signal = "CORRELATION"  # Track correlation even if neutral

        # Only store if signal is not neutral
        if signal != "NEUTRAL":
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            window_end = datetime.now()
            window_start = window_end - timedelta(minutes=self.config['correlation_window_minutes'])

            cursor.execute('''
                INSERT INTO sentiment_price_correlation
                (window_start, window_end, fear_greed_index, fear_greed_class,
                 btc_price_change, eth_price_change, market_direction, correlation_signal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (window_start, window_end, fear_greed, self.sentiment_data['classification'],
                  btc_change, eth_change, market_direction, signal))

            conn.commit()
            conn.close()

            print(f"[SIGNAL] {signal}: F&G={fear_greed}, Market={market_direction}, BTC={btc_change:+.2f}%, ETH={eth_change:+.2f}%")

    def start(self):
        """Start processing Kafka streams."""
        print("="*60)
        print("OPTIMIZED DIRECT KAFKA PROCESSOR")
        print("="*60)
        print("\nConfiguration:")
        print(f"  Volume spike threshold: {self.config['volume_spike_multiplier']}x rolling average")
        print(f"  Price change threshold: {self.config['price_change_threshold']}%")
        print(f"  Fear threshold: < {self.config['fear_threshold']}")
        print(f"  Greed threshold: > {self.config['greed_threshold']}")
        print(f"\nReading from Kafka topics:")
        print("  - crypto-prices (Binance)")
        print("  - crypto-fear-greed (Fear & Greed Index)")
        print(f"\nWriting to: {self.db_path}\n")
        print("="*60 + "\n")

        # Create consumers for each topic
        price_consumer = Consumer(self.consumer_config)
        sentiment_consumer = Consumer(self.consumer_config)

        price_consumer.subscribe(['crypto-prices'])
        sentiment_consumer.subscribe(['crypto-fear-greed'])

        print("[OK] Started processing. Press Ctrl+C to stop\n")

        message_count = 0
        last_stats_time = time.time()

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

                # Print stats every 30 seconds
                if time.time() - last_stats_time > 30:
                    print(f"\n[STATS] Processed {message_count} messages total\n")
                    last_stats_time = time.time()

        except KeyboardInterrupt:
            print(f"\n\nStopping... Processed {message_count} messages total")
            price_consumer.close()
            sentiment_consumer.close()


if __name__ == "__main__":
    processor = OptimizedKafkaProcessor()
    processor.start()
