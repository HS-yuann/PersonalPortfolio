"""
Simple Kafka consumer to verify messages are flowing.

Reads from crypto-prices topic and displays messages.
"""

from confluent_kafka import Consumer, KafkaError
import json


def main():
    """Consume and display messages from Kafka."""
    # Consumer configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'crypto-consumer-group',
        'auto.offset.reset': 'earliest',  # Start from beginning if no offset
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['crypto-prices'])
    
    print("Kafka consumer started. Waiting for messages...")
    print("Press Ctrl+C to stop\n")
    
    message_count = 0
    
    try:
        while True:
            # Poll for messages (timeout 1 second)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition - not an error
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            
            # Process message
            message_count += 1
            data = json.loads(msg.value().decode('utf-8'))
            
            print(f"[{message_count}] {data['symbol']}: ${data['price']:,.2f} "
                  f"({data['price_change_percent']:+.2f}%) "
                  f"at {data['human_time']}")
            
    except KeyboardInterrupt:
        print(f"\nStopping consumer. Processed {message_count} messages.")
    
    finally:
        consumer.close()


if __name__ == "__main__":
    main()