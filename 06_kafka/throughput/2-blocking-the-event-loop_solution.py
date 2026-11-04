"""
SOLUTION 2: Move blocking work off the event loop, and run it concurrently.

Fixes:
  - The reader only parses and enqueues, so it always keeps reading the socket and
    answering keepalive pings.
  - enrich() runs in a thread pool via run_in_executor(). This alone isn't enough:
    awaiting ONE 5 ms call at a time still caps us at ~200 msg/s. WORKERS tasks each
    keep one call in flight, so capacity is ~WORKERS / 5 ms (~6000 msg/s with 32).
  - A bounded queue between them: if enrichment ever falls behind, put() waits, the
    reader slows, and the backlog is pushed back to the server instead of into memory.

Trade-off: with several workers, trades can reach Kafka slightly out of order. If
per-symbol order matters, shard by key (one queue + worker per symbol hash).
Even better, when possible: use an async client (httpx/aiohttp) or cache lookups
(functools.lru_cache) so the blocking call disappears entirely.

Watch:  `lag` stays near 0 and `queued` stays small.
"""
import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
WORKERS = 32                                            # concurrent enrichment calls

# Log to the console and to a local file named after this script
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(Path(__file__).with_suffix(".log")), logging.StreamHandler()])
log = logging.getLogger(__name__)

app = Application(broker_address=KAFKA_BROKER,
                  producer_extra_config={"broker.address.family": "v4", "linger.ms": 10, "compression.type": "lz4"})
topic = app.topic(name="trades", value_serializer="json")
pool = ThreadPoolExecutor(max_workers=WORKERS)          # threads that run the blocking calls
stats = {"produced": 0, "lag": 0.0}


def enrich(data):
    """Stand-in for a synchronous lookup, e.g. requests.get("https://geo.example.com/...")."""
    time.sleep(0.005)                                   # still blocking, but now in a pool thread
    data["enriched"] = True
    return data


async def worker(queue, producer):
    """Take one trade, enrich it in the thread pool, and produce it to Kafka."""
    loop = asyncio.get_running_loop()
    while True:
        data = await queue.get()
        try:
            data = await loop.run_in_executor(pool, enrich, data)  # <-- THE FIX: off the loop
            msg = topic.serialize(key=data["symbol"], value=data)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            stats["produced"] += 1
            stats["lag"] = time.time() - data["ts"]
        except Exception as e:
            log.error(f"Failed seq={data.get('seq')}: {e!r}")
        finally:
            queue.task_done()


async def main():
    """Read trades and hand them to the worker pool; count gaps in `seq`."""
    count, last_seq, missed = 0, None, 0
    queue = asyncio.Queue(maxsize=2000)                 # bounded: a slow pool slows the reader
    with app.get_producer() as producer:
        workers = [asyncio.create_task(worker(queue, producer)) for _ in range(WORKERS)]
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    data = json.loads(message)
                    await queue.put(data)

                    # Bookkeeping: count gaps in `seq` and how far behind real time we are
                    if last_seq is not None and data["seq"] != last_seq + 1:
                        missed += data["seq"] - last_seq - 1
                    last_seq, count = data["seq"], count + 1
                    if count % 1000 == 0:
                        log.info(f"received={count} produced={stats['produced']} missed={missed} "
                                 f"queued={queue.qsize()} lag={stats['lag']:.2f}s")
        except Exception as e:
            log.error(f"Connection lost: {e!r}")
        finally:
            try:
                await asyncio.wait_for(queue.join(), timeout=10)  # finish what's already queued
            except (TimeoutError, asyncio.CancelledError):
                log.warning(f"Shutdown: {queue.qsize()} messages still queued")
            for w in workers:
                w.cancel()
    pool.shutdown()
    log.info(f"Total produced: {stats['produced']}, missed: {missed}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
