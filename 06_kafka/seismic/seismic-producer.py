#!/usr/bin/env python3

"""
Seismic Stream Producer

Connects to the EMSC seismic portal websocket stream and produces
each earthquake event to the "seismic" Kafka topic.
"""

import os
import json
from quixstreams import Application
from quixstreams.models import TopicConfig
from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
SEISMIC_URI = "wss://www.seismicportal.eu/standing_order/websocket"

# Kafka app and topic; force IPv4 to avoid localhost resolving to IPv6
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4"},
)
# Topic settings apply only when the topic is first created; an existing topic keeps its config
topic = app.topic(
    name="seismic",
    value_serializer="json",
    config=TopicConfig(
        num_partitions=3,                         # parallelism: max concurrent consumers in a group
        replication_factor=3,                     # copies of each partition across the 3 brokers
        extra_config={
            "retention.ms": str(24 * 60 * 60 * 1000),  # "TTL": delete data older than 24 hours
            "retention.bytes": str(100 * 1024 * 1024), # or once a partition exceeds 100 MB
            "min.insync.replicas": "2",                # replicas that must ack when acks=all
        },
    ),
)

# Parse one websocket message and produce it to Kafka, keyed by event unid
def process(message, producer):
    try:
        data = json.loads(message)
        info = data["data"]["properties"]
        msg = topic.serialize(key=info["unid"], value=data)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value)
        print(f"{data['action']:7} | {info['time']} | Mag {info['mag']} | {info['flynn_region']}")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing event: {e}")
    except Exception as e:
        print(f"Error publishing to Kafka: {e}")


# Open the websocket and produce messages until the server closes it
async def main():
    print(f"Producing to topic '{topic.name}' on {KAFKA_BROKER}")
    try:
        ws = await websocket_connect(SEISMIC_URI, ping_interval=15)
    except Exception as e:
        print(f"Error connecting to stream: {e}")
        raise
    print(f"Connected to {SEISMIC_URI}. Press Ctrl+C to stop\n")
    with app.get_producer() as producer:
        while (message := await ws.read_message()) is not None:
            process(message, producer)
    print("Stream closed by server")


if __name__ == "__main__":
    try:
        IOLoop.current().run_sync(main)
    except KeyboardInterrupt:
        print("\nStopping stream...")
