"""
SOLUTION 5: Reconnect with ?cursor=, back off with jitter, and skip duplicates.

Fixes:
  - Track the last `seq` successfully handed to the producer and reconnect with
    ?cursor=<last_seq + 1>. The server replays everything we missed from its 60 s
    buffer, including messages that were still unread in the dead socket.
  - Exponential backoff with jitter (capped) instead of a fixed 1 s: a flapping server
    isn't hammered, and it resets after a successful connection.
  - Skip any seq <= last_seq, so an overlapping replay never writes duplicates.
  - The replay arrives as a burst. produce() is non-blocking and batched (linger.ms,
    lz4, a larger local buffer), so the producer absorbs it without stalling reads.

Watch:  `missed` stays 0 across simulated drops; `lag` briefly rises, then recovers.
"""
import asyncio
import json
import logging
import os
import random
import time
from pathlib import Path
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
DROP_CHANCE = 0.0005                                    # per message: a simulated drop every ~2 s
MAX_BACKOFF = 30                                        # seconds; must stay well under the 60 s replay window

# Log to the console and to a local file named after this script
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(Path(__file__).with_suffix(".log")), logging.StreamHandler()])
log = logging.getLogger(__name__)

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        "linger.ms": 10,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500_000,        # room for a full 60 s replay burst
    },
)
topic = app.topic(name="trades", value_serializer="json")


async def main():
    """Produce trades to Kafka forever, resuming from the last seen seq after any drop (Ctrl+C to stop)."""
    count, last_seq, missed, attempt = 0, None, 0, 0
    with app.get_producer() as producer:
        while True:
            url = URL if last_seq is None else f"{URL}?cursor={last_seq + 1}"  # <-- THE FIX: resume cursor
            try:
                async with websockets.connect(url) as ws:
                    attempt = 0                         # connected: reset the backoff
                    async for message in ws:
                        data = json.loads(message)
                        if last_seq is not None and data["seq"] <= last_seq:
                            continue                    # duplicate from an overlapping replay
                        msg = topic.serialize(key=data["symbol"], value=data)
                        producer.produce(topic=topic.name, key=msg.key, value=msg.value)

                        # Bookkeeping: count gaps in `seq` and how far behind real time we are
                        if last_seq is not None and data["seq"] != last_seq + 1:
                            missed += data["seq"] - last_seq - 1
                        last_seq, count = data["seq"], count + 1
                        if count % 1000 == 0:
                            log.info(f"produced={count} missed={missed} lag={time.time() - data['ts']:.2f}s")

                        if random.random() < DROP_CHANCE:
                            raise ConnectionError("simulated network drop")
            except Exception as e:
                delay = min(MAX_BACKOFF, 2 ** attempt) * random.uniform(0.5, 1.0)  # backoff + jitter
                attempt += 1
                log.warning(f"Connection lost: {e!r}; resuming at seq {last_seq + 1 if last_seq is not None else 'live'} "
                            f"in {delay:.1f}s")
                await asyncio.sleep(delay)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
