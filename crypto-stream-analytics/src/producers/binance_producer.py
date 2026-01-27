"""
Binance WebSocket producer for cryptocurrency price streaming.

Uses websocket-client for real-time price data ingestion.
"""

import json
import time
from datetime import datetime
import websocket
import threading
from confluent_kafka import Producer
import socket


class BinanceStreamProducer:
    """
    Produces real-time cryptocurrency price data from Binance WebSocket to Kafka.

    Streams data from Binance and publishes to Kafka topics for downstream processing.
    """

    def __init__(self, symbols, kafka_topic='crypto-prices'):
        """
        Initialize producer with Kafka configuration.

        Args:
            symbols (list): Trading pairs to stream
            kafka_topic (str): Kafka topic to publish to
        """
        self.symbols = symbols
        self.topic = kafka_topic
        self.ws_connections = []
        self.message_count = 0
        self.kafka_message_count = 0
        self.start_time = time.time()
        self.running = False

        # Kafka Producer Configuration
        conf = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': socket.gethostname(),
            'linger.ms': 10,
            'compression.type': 'snappy',
        }

        self.kafka_producer = Producer(conf)
        print(f"Kafka producer initialized: {conf['bootstrap.servers']}")

    def delivery_report(self, err, msg):
        """
        Callback for Kafka delivery reports.

        Args:
            err: Error object if delivery failed
            msg: Message object with metadata
        """
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            self.kafka_message_count += 1
            if self.kafka_message_count % 100 == 0:
                print(f'[OK] Delivered to Kafka: {msg.topic()} [{msg.partition()}] '
                      f'(Total: {self.kafka_message_count})')

    def on_message(self, ws, message):
        """
        Process WebSocket message and send to Kafka.

        Args:
            ws: WebSocket connection
            message (str): JSON string from Binance
        """
        try:
            data = json.loads(message)

            processed_data = {
                'symbol': data['s'],
                'price': float(data['c']),
                'volume': float(data['v']),
                'price_change_percent': float(data['P']),
                'high_24h': float(data['h']),
                'low_24h': float(data['l']),
                'timestamp': data['E'],
                'human_time': datetime.fromtimestamp(data['E']/1000).isoformat()
            }

            # Send to Kafka with symbol as partition key
            self.kafka_producer.produce(
                topic=self.topic,
                key=processed_data['symbol'],
                value=json.dumps(processed_data),
                callback=self.delivery_report
            )

            self.kafka_producer.poll(0)

            change = processed_data['price_change_percent']
            direction = "UP" if change > 0 else "DOWN" if change < 0 else "FLAT"

            print(f"[{processed_data['human_time']}] "
                  f"{direction} {processed_data['symbol']}: "
                  f"${processed_data['price']:,.2f} "
                  f"({change:+.2f}%)")

            self.message_count += 1

        except Exception as e:
            print(f"Error processing message: {e}")

    def on_error(self, ws, error):
        """WebSocket error handler."""
        print(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """WebSocket close handler."""
        print(f"WebSocket closed: {close_status_code} - {close_msg}")

    def on_open(self, ws):
        """WebSocket open handler."""
        print(f"WebSocket connection opened")

    def print_stats(self):
        """Print throughput statistics."""
        while self.running:
            time.sleep(10)
            elapsed = time.time() - self.start_time
            msg_per_sec = self.message_count / elapsed if elapsed > 0 else 0
            print(f"\n--- Stats: {self.message_count} received, "
                  f"{self.kafka_message_count} sent to Kafka, "
                  f"{msg_per_sec:.2f} msg/sec ---\n")

    def start(self):
        """Start streaming to Kafka."""
        print(f"Starting Binance -> Kafka pipeline")
        print(f"Symbols: {', '.join(self.symbols)}")
        print(f"Kafka topic: {self.topic}\n")

        self.running = True

        stats_thread = threading.Thread(target=self.print_stats, daemon=True)
        stats_thread.start()

        for symbol in self.symbols:
            ws_url = f"wss://stream.binance.com:9443/ws/{symbol}@ticker"

            ws = websocket.WebSocketApp(
                ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )

            ws_thread = threading.Thread(target=ws.run_forever, daemon=True)
            ws_thread.start()

            self.ws_connections.append(ws)
            time.sleep(0.5)

        print("Streaming to Kafka. Press Ctrl+C to stop.\n")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nStopping streams...")
            self.stop()

    def stop(self):
        """Gracefully stop and flush remaining messages."""
        self.running = False

        for ws in self.ws_connections:
            ws.close()

        print("Flushing remaining messages to Kafka...")
        self.kafka_producer.flush(timeout=10)

        print(f"\nFinal stats:")
        print(f"  Messages received: {self.message_count}")
        print(f"  Messages sent to Kafka: {self.kafka_message_count}")
        print(f"  Average throughput: {self.message_count / (time.time() - self.start_time):.2f} msg/sec")


if __name__ == "__main__":
    SYMBOLS = [
        'btcusdt',
        'ethusdt',
        'bnbusdt',
    ]

    producer = BinanceStreamProducer(SYMBOLS)
    producer.start()
