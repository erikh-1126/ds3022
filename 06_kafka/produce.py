#!/usr/bin/env python3

"""
Simple Kafka Producer

Produces 50 small JSON messages to the "simple" topic, each with an
8-character unique id, a UTC timestamp, and a short payload.
"""

import os
import uuid
from datetime import datetime, timezone
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "simple"
NUM_MESSAGES = 50

# Kafka app and topic; force IPv4 to avoid localhost resolving to IPv6
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4"},
)
topic = app.topic(name=TOPIC, value_serializer="json")


# Build and send NUM_MESSAGES messages, keyed by their unique id
def produce_messages():
    try:
        with app.get_producer() as producer:
            for i in range(NUM_MESSAGES):
                msg_id = uuid.uuid4().hex[:8]
                value = {
                    "id": msg_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "message": f"hello from message {i + 1}",
                }
                msg = topic.serialize(key=msg_id, value=value)
                producer.produce(topic=topic.name, key=msg.key, value=msg.value)
                print(value)
        print(f"Produced {NUM_MESSAGES} messages to '{TOPIC}'")
    except Exception as e:
        print(f"Error producing to Kafka: {e}")

if __name__ == "__main__":
    produce_messages()
