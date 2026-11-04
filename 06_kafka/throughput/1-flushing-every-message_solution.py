"""
SOLUTION 1: Let the producer batch; flush once, on shutdown.

Fixes:
  - No flush() per message. produce() only appends to librdkafka's local buffer, and
    quixstreams' produce() already calls poll(0) to serve delivery callbacks.
  - Tune batching: linger.ms lets the client wait a few ms to fill a batch, batch.size
    allows bigger batches, and lz4 compression shrinks bytes on the wire.
  - The `with` block flushes once at exit, so nothing buffered is lost.

Watch:  `lag` stays near 0 and `missed` stays 0.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

# Log to the console and to a local file named after this script
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(Path(__file__).with_suffix(".log")), logging.StreamHandler()])
log = logging.getLogger(__name__)

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        "linger.ms": 10,                                # wait up to 10 ms to fill a batch
        "batch.size": 262_144,                          # up to 256 KB per batch
        "compression.type": "lz4",                      # cheap CPU, fewer bytes sent
    },
)
topic = app.topic(name="trades", value_serializer="json")


async def main():
    """Read trades from the WebSocket and produce each one to Kafka without waiting for acks."""
    count, last_seq, missed = 0, None, 0
    with app.get_producer() as producer:                # __exit__ flushes once at the end
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    data = json.loads(message)
                    msg = topic.serialize(key=data["symbol"], value=data)
                    producer.produce(topic=topic.name, key=msg.key, value=msg.value)  # <-- THE FIX: no flush()

                    # Bookkeeping: count gaps in `seq` and how far behind real time we are
                    if last_seq is not None and data["seq"] != last_seq + 1:
                        missed += data["seq"] - last_seq - 1
                    last_seq, count = data["seq"], count + 1
                    if count % 1000 == 0:
                        log.info(f"produced={count} missed={missed} lag={time.time() - data['ts']:.2f}s")
        except Exception as e:
            log.error(f"Connection lost: {e!r}")
    log.info(f"Total produced: {count}, missed: {missed}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
