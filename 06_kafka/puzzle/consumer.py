#!/usr/bin/env python3

"""
Reads puzzle messages from the "puzzle" topic and prints each one's key,
timestamp, and full JSON value. Storing, sorting, and reassembling are left
to you.
"""

import os
import json
from datetime import datetime, timezone
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "puzzle"
MAX_EMPTY_POLLS = 5

# Kafka app; start from the oldest message. Uncomment extra_config to force IPv4 if using "localhost"
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="puzzle-consumer",
    auto_offset_reset="earliest",
    # consumer_extra_config={"broker.address.family": "v4"},
)

# Print every message until the topic goes quiet for MAX_EMPTY_POLLS seconds
def consume_messages():
    try:
        # No offset commits, so every run re-reads the topic from the start
        with app.get_consumer(auto_commit_enable=False) as consumer:
            consumer.subscribe([TOPIC])
            print(f"Consuming from topic '{TOPIC}' on {KAFKA_BROKER}")
            empty_polls = 0
            while empty_polls < MAX_EMPTY_POLLS:
                msg = consumer.poll(1)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                empty_polls = 0
                # here is the real message data for each log entry:
                # each are part of the `msg`: msg.key, msg.timestamp(), msg.value()

                # key / timestamp / value
                key = msg.key().decode("utf8")

                # Kafka timestamps are (type, epoch milliseconds)
                ts = datetime.fromtimestamp(msg.timestamp()[1] / 1000, tz=timezone.utc).isoformat()

                # value is the actual json payload of the message content
                value = json.loads(msg.value())

                print(f"key={key} timestamp={ts} value={json.dumps(value)}")
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")

if __name__ == "__main__":
    consume_messages()
