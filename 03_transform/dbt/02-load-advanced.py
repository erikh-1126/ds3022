import asyncio
import logging
import os
import sys
import threading
from pathlib import Path

import httpx
import psutil
from dbt.cli.main import dbtRunner

PROJECT_DIR = Path(__file__).parent
DATA_DIR = PROJECT_DIR / "data"
URL = "https://s3.amazonaws.com/uvasds-data/taxi/yellow_tripdata_2025-{i}.parquet"

logging.basicConfig(
    filename=PROJECT_DIR / "load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,  # importing dbt already attached a root handler, which would make basicConfig a no-op
)


# ---------- RESOURCE MONITORING ----------

PROC = psutil.Process()


def monitor_usage(stop, interval=5):
    """Background thread: log this process's CPU% and memory every `interval` seconds.

    CPU% is measured over each interval and can exceed 100% when multiple cores are busy
    (e.g. DuckDB running in parallel threads). RSS is physical RAM currently in use.
    """
    peak = 0
    PROC.cpu_percent(None)  # first call primes the counter; it always returns 0.0
    while not stop.wait(interval):
        try:
            rss = PROC.memory_info().rss / 1e6
            peak = max(peak, rss)
            logging.info(f"[usage] cpu={PROC.cpu_percent(None):.0f}% rss={rss:.0f} MB "
                         f"threads={PROC.num_threads()}")
        except psutil.Error as e:
            logging.warning(f"[usage] sampling failed: {e}")
    # Summary on shutdown: cumulative CPU seconds, comparable to the `time` command output
    t = PROC.cpu_times()
    logging.info(f"[usage] summary: user={t.user:.1f}s system={t.system:.1f}s peak_rss={peak:.0f} MB")


# ---------- EXTRACT / LOAD ----------

async def download_files(base_url, start, end):
    """Download files start..end concurrently (max 4 at once) and save each to data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient(timeout=120) as client:
        semaphore = asyncio.Semaphore(4)

        async def fetch(i):
            """Fetch one file, write it to disk, and log a status line. Returns None on failure."""
            async with semaphore:
                url = base_url.format(i=f"{i:02d}")
                path = DATA_DIR / url.rsplit("/", 1)[-1]
                # Skip files we already have, so re-running the pipeline is cheap
                if path.exists():
                    logging.info(f"Skipped {path.name} (already downloaded)")
                    return path
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                except httpx.HTTPError as e:
                    logging.error(f"Failed {url}: {e}")
                    print(f"FAILED {path.name}: {e}")
                    return None
                path.write_bytes(response.content)
                msg = f"Fetched {path.name} ({len(response.content) / 1e6:.1f} MB)"
                logging.info(msg)
                print(msg)
                return path

        tasks = [fetch(i) for i in range(start, end + 1)]
        return await asyncio.gather(*tasks)


# ---------- TRANSFORM (dbt) ----------

def run_dbt(command="build"):
    """Invoke dbt programmatically (same as typing `dbt build` in this folder).

    No separate "load to database" step is needed: sources.yml points
    dbt-duckdb straight at data/*.parquet, so dbt reads the files we just downloaded.
    """
    # sources.yml and profiles.yml use relative paths (data/, nyc_taxi.duckdb),
    # so dbt must run from the project folder no matter where this script was launched.
    try:
        os.chdir(PROJECT_DIR)
        res = dbtRunner().invoke([command])
    except Exception as e:
        # dbt couldn't even start (bad install, missing project folder, etc.)
        logging.exception(f"dbt {command} failed to start: {e}")
        print(f"FAILED dbt {command}: {e}")
        return False

    # res.success is False if any model/test failed; res.exception is set if dbt itself crashed
    if res.exception:
        logging.error(f"dbt {command} crashed: {res.exception}")
        print(f"FAILED dbt {command}: {res.exception}")
    # Log each node's status; failed models/tests get ERROR level so they stand out in load.log
    for r in res.result or []:
        level = {"error": logging.ERROR, "fail": logging.ERROR, "warn": logging.WARNING}.get(r.status, logging.INFO)
        logging.log(level, f"dbt {r.node.name}: {r.status} {r.message or ''}".rstrip())
    logging.info(f"dbt {command} success={res.success}")
    return res.success


def main():
    """Download all months, then run dbt. Returns a process exit code."""
    paths = asyncio.run(download_files(URL, start=1, end=12))
    ok = [p for p in paths if p]
    print(f"Downloaded: {len(ok)}/{len(paths)} files in {DATA_DIR}")

    # Don't transform a partial dataset
    if len(ok) < len(paths):
        logging.error("Some downloads failed; skipping dbt")
        return 1
    return 0 if run_dbt("build") else 1


if __name__ == "__main__":
    # Sample CPU/memory in the background for the whole run; stop it (and log the summary) on exit
    stop = threading.Event()
    monitor = threading.Thread(target=monitor_usage, args=(stop,), daemon=True)
    monitor.start()
    try:
        code = main()
    finally:
        stop.set()
        monitor.join()
    sys.exit(code)
