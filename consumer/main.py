"""
Kafka Consumer: Reads events from 'message-events' topic,
return those events (messages) that are not older than 48 hours, prints in consumer console and writes them to output.jsonl.
"""

from confluent_kafka import Consumer, KafkaException
import json
import signal
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

# Global flag to signal shutdown
running = True

def signal_handler(sig, frame):
    """
    Signal handler to allow graceful shutdown on Ctrl+C or SIGTERM.
    """
    global running
    print("\nGraceful shutdown requested...")
    running = False

def create_consumer() -> Consumer:
    """
    Create and configure a Kafka consumer.
    """
    return Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'event-transformer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })

def transform_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return messages that are at less or equal to 48 hours old (within the last 48 hours).
    """
    timestamp_str = event.get("timestamp")
    if timestamp_str:
        try:
            event_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            current_time = datetime.now(timezone.utc)
            if current_time - event_time <= timedelta(days=2):
                return {
                    "message_id": event.get("message_id"),
                    "sender": event.get("sender"),
                    "text": event.get("text"),
                    "timestamp": event.get("timestamp")
                }
        except ValueError as e:
            print(f"Invalid timestamp format: {e}")
    return {}

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = create_consumer()
    consumer.subscribe(['message-events'])

    print("Consumer started. Listening for messages...")

    with open(r'data\output.jsonl', 'a', encoding='utf-8') as outfile:
        try:
            while running:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    transformed = transform_event(event)

                    if transformed:
                        outfile.write(json.dumps(transformed) + '\n')
                        print(f"Processed event: {transformed}")

                    # Manual offset commit
                    consumer.commit(msg)
                except Exception as e:
                    print(f"Error processing message: {e}")

        finally:
            print("Closing consumer...")
            consumer.close()

if __name__ == '__main__':
    main()
