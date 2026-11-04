"""
SOLUTION 4: A fixed worker pool instead of a task per message, plus batched requests.

Fixes:
  - Bound the work in flight: a bounded queue feeds exactly CONCURRENCY worker tasks
    (matching the service's 10-request limit). The reader awaits put(), so a slow
    service slows the reader instead of piling up thousands of waiting tasks.
  - Bounding alone can't beat the service's ceiling: 10 slots x 20 ms = ~500 req/s
    against 1000 msg/s. So each request carries up to BATCH trades (most enrichment
    APIs have a bulk endpoint). Capacity becomes ~10 x BATCH / 20 ms.

Minimal alternative if you can't batch: acquire a Semaphore BEFORE create_task() in
the reader, so the reader blocks when the limit is reached. That bounds memory, but
the lag will still grow, because the service is slower than the source.

Watch:  `queued` stays small, `lag` stays near 0.
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
CONCURRENCY = 10                                        # the service's concurrent-request limit
BATCH = 100                                             # trades per enrichment request

# Log to the console and to a local file named after this script
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(Path(__file__).with_suffix(".log")), logging.StreamHandler()])
log = logging.getLogger(__name__)

app = Application(broker_address=KAFKA_BROKER,
                  producer_extra_config={"broker.address.family": "v4", "linger.ms": 10, "compression.type": "lz4"})
topic = app.topic(name="trades", value_serializer="json")
downstream = asyncio.Semaphore(CONCURRENCY)             # the enrichment service: 10 concurrent requests
stats = {"produced": 0, "lag": 0.0}


async def enrich_batch(batch):
    """One simulated 20 ms async HTTP call that enriches a whole batch."""
    async with downstream:
        await asyncio.sleep(0.02)
    for data in batch:
        data["enriched"] = True
    return batch


async def worker(queue, producer):
    """Pull up to BATCH trades, enrich them in one request, then produce them."""
    while True:
        batch = [await queue.get()]
        while len(batch) < BATCH and not queue.empty():
            batch.append(queue.get_nowait())
        try:
            for data in await enrich_batch(batch):
                msg = topic.serialize(key=data["symbol"], value=data)
                producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            stats["produced"] += len(batch)
            stats["lag"] = time.time() - batch[-1]["ts"]
        except Exception as e:
            log.error(f"Batch of {len(batch)} failed: {e!r}")
        finally:
            for _ in batch:
                queue.task_done()


async def main():
    """Read trades into a bounded queue served by a fixed pool of workers."""
    count = 0
    queue = asyncio.Queue(maxsize=CONCURRENCY * BATCH * 2)  # bounded: a slow service slows the reader
    with app.get_producer() as producer:
        workers = [asyncio.create_task(worker(queue, producer)) for _ in range(CONCURRENCY)]  # <-- THE FIX
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    await queue.put(json.loads(message))
                    count += 1
                    if count % 1000 == 0:
                        log.info(f"received={count} produced={stats['produced']} "
                                 f"queued={queue.qsize():,} lag={stats['lag']:.2f}s")
        except Exception as e:
            log.error(f"Connection lost: {e!r}")
        finally:
            try:
                await asyncio.wait_for(queue.join(), timeout=10)  # finish in-flight work, lose nothing
            except (TimeoutError, asyncio.CancelledError):
                log.warning(f"Shutdown: {queue.qsize()} messages still queued")
            for w in workers:
                w.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
