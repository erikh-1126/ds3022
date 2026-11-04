"""
SOLUTION 3: Bound the queue, and make the writer faster by batching.

Fixes:
  - asyncio.Queue(maxsize=...): when the writer falls behind, put() waits, the reader
    stops draining the socket, and the backlog stays on the server (which can replay
    it) instead of in our memory (which can't). Memory use is capped at QUEUE_MAX.
  - The slow downstream step has a fixed cost per CALL (a DB write, an HTTP request),
    not per message. The writer drains everything waiting, up to BATCH, and pays that
    cost once per batch. Throughput becomes ~BATCH / cost instead of 1 / cost.
    (Note: on Windows asyncio.sleep(0.002) really takes ~15 ms, so per-message calls
    are even slower than the comment in misstep 3 suggests; batching still wins.)

Watch:  `queued` stays well under QUEUE_MAX and `lag` stays near 0.
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
QUEUE_MAX = 5_000                                       # hard cap on buffered messages
BATCH = 500                                             # max messages per downstream call

# Log to the console and to a local file named after this script
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(Path(__file__).with_suffix(".log")), logging.StreamHandler()])
log = logging.getLogger(__name__)

app = Application(broker_address=KAFKA_BROKER,
                  producer_extra_config={"broker.address.family": "v4", "linger.ms": 10, "compression.type": "lz4"})
topic = app.topic(name="trades", value_serializer="json")


async def receiver(queue):
    """Read the socket; put() waits whenever the queue is full (this is the backpressure)."""
    async with websockets.connect(URL) as ws:
        async for message in ws:
            await queue.put(message)                    # <-- THE FIX: bounded queue, put() can wait


async def writer(queue, producer):
    """Drain up to BATCH messages, do the slow step once per batch, then produce them all."""
    count = 0
    while True:
        batch = [await queue.get()]                     # wait for at least one message
        while len(batch) < BATCH and not queue.empty():
            batch.append(queue.get_nowait())            # then grab whatever else is waiting
        try:
            await asyncio.sleep(0.002)                  # slow downstream work, now once per batch
            for raw in batch:
                data = json.loads(raw)
                msg = topic.serialize(key=data["symbol"], value=data)
                producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            count += len(batch)
            if count // 1000 != (count - len(batch)) // 1000:  # log roughly every 1000
                log.info(f"produced={count} batch={len(batch)} queued={queue.qsize():,} "
                         f"lag={time.time() - data['ts']:.2f}s")
        except Exception as e:
            log.error(f"Batch of {len(batch)} failed: {e!r}")
        finally:
            for _ in batch:
                queue.task_done()


async def main():
    """Run the receiver and writer side by side; Ctrl+C to stop."""
    queue = asyncio.Queue(maxsize=QUEUE_MAX)
    with app.get_producer() as producer:
        task = asyncio.create_task(writer(queue, producer))
        try:
            await receiver(queue)
        except Exception as e:
            log.error(f"Connection lost: {e!r}")
        finally:
            try:
                await asyncio.wait_for(queue.join(), timeout=10)  # produce whatever is still queued
            except (TimeoutError, asyncio.CancelledError):
                log.warning(f"Shutdown: {queue.qsize()} messages still queued")
            task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
