#!/usr/bin/env python3

"""
Simple Kafka Consumer

Reads 10 JSON messages from the "simple" topic, prints each with its partition
and offset, commits the offsets, and exits. Run it again and it picks up where
the consumer group left off.
"""

import os
import json
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "simple"
MAX_MESSAGES = 10
MAX_EMPTY_POLLS = 3

# Kafka app; a new consumer group starts from the oldest message in the topic
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="simple-consumer",
    auto_offset_reset="earliest",
    consumer_extra_config={"broker.address.family": "v4"},
)

# Read up to MAX_MESSAGES, then commit offsets so the next run resumes after them
def consume_messages():
    try:
        with app.get_consumer() as consumer:
            consumer.subscribe([TOPIC])
            print(f"Consuming from topic '{TOPIC}' on {KAFKA_BROKER}")
            count, empty_polls = 0, 0
            while count < MAX_MESSAGES and empty_polls < MAX_EMPTY_POLLS:
                batch = consumer.consume(num_messages=MAX_MESSAGES - count, timeout=5)
                if not batch:
                    empty_polls += 1
                    print("No new messages, waiting...")
                    continue
                for msg in batch:
                    if msg.error():
                        print(f"Kafka error: {msg.error()}")
                        continue
                    count += 1
                    print(f"partition={msg.partition()} offset={msg.offset()} value={json.loads(msg.value())}")
                    # Mark as processed; Quix disables auto offset storing, so commit() needs this
                    consumer.store_offsets(message=msg)
            # Synchronous commit of the stored offsets so they're saved before exit
            if count:
                consumer.commit(asynchronous=False)
            print(f"Read {count} messages, offsets committed. Exiting.")
    except KeyboardInterrupt:
        print("Stopped")
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")

if __name__ == "__main__":
    consume_messages()
