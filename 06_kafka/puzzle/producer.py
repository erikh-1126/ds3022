#!/usr/bin/env python3

import json
import os
from quixstreams import Application
from websockets.sync.client import connect

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
WS_URL = os.getenv("WS_URL", "ws://127.0.0.1:8765")
TOPIC = "puzzle"

# Kafka app and topic; uncomment extra_config to force IPv4 if using "localhost"
app = Application(
    broker_address=KAFKA_BROKER,
    # producer_extra_config={"broker.address.family": "v4"},
)
topic = app.topic(name=TOPIC, value_serializer="json")

# Read messages from the WebSocket until the server closes it, producing each to Kafka
def produce_messages():
    try:
        with connect(WS_URL) as ws, app.get_producer() as producer:
            print(f"Connected to {WS_URL} (first message arrives after ~30s)")
            # Each loop blocks until the next message arrives; ends when the server closes
            for raw in ws:
                value = json.loads(raw)
                # do
                # things
                # here
        print(f"Round complete; messages produced to '{TOPIC}'")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    produce_messages()
