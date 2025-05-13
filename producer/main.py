"""
Kafka Producer: Reads events from input.jsonl, serializes to JSON,
and publishes them to Kafka topic 'message-events' with retry logic.
"""

import json
import time
from datetime import datetime, timezone
from typing import Dict, Any
from confluent_kafka import Producer, KafkaException, KafkaError


# Kafka configuration with durability
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',  # Ensure all replicas ack the message (strong durability)
})

# Callback to confirm message delivery
def delivery_report(err: KafkaError, msg) -> None:
    """
    Callback function triggered when a message is successfully delivered or fails.
    """
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            timestamp_str = payload.get("timestamp")
            if timestamp_str:
                event_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                current_time = datetime.now(timezone.utc)
                hours_ago = (current_time - event_time).total_seconds() / 3600
                print(
                    f"Message `{payload['text']}` sent by `{payload['sender']}` "
                    f"{hours_ago:.2f} hours ago to the topic: `{msg.topic()}` at offset {msg.offset()}"
                )
            else:
                print("Timestamp missing in payload.")
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"Error processing message: {e}")


def send_event(event: Dict[str, Any], max_retries: int = 5) -> None:
    """
    Try sending an event to Kafka with retries on transient errors.
    """
    retries = 0
    while retries < max_retries:
        try:
            # Serialize and send
            producer.produce(
                topic='message-events',
                value=json.dumps(event),
                callback=delivery_report
            )
            producer.poll(0)  # Trigger callbacks
            return  # Exit after successful send
        except BufferError:
            print("⚠️ Buffer full. Retrying...")
            time.sleep(1)
            retries += 1
        except KafkaException as ke:
            print(f"⚠️ Kafka error: {ke}. Retrying...")
            time.sleep(1)
            retries += 1
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}. Retrying...")
            time.sleep(1)
            retries += 1

    print(f"Failed to send event after {max_retries} attempts: {event}")


def main():
    input_file = r'data\input.jsonl'

    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    send_event(event)
                except json.JSONDecodeError as je:
                    print(f"Skipping invalid JSON: {line} — {je}")

    except FileNotFoundError:
        print(f"❗ Input file {input_file} not found.")
    except Exception as e:
        print(f"❗ Unexpected error: {e}")
    finally:
        print("Flushing remaining messages...")
        producer.flush()


if __name__ == '__main__':
    main()
