import logging
import os
import sys
from pathlib import Path
import httpx
from dbt.cli.main import dbtRunner

# ---------- 0. SETUP: vallues, environment, logging ------------

PROJECT_DIR = Path(__file__).parent
DATA_DIR = PROJECT_DIR / "data"
BASE = "https://s3.amazonaws.com/uvasds-data/taxi/"

logging.basicConfig(
    filename=PROJECT_DIR / "load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,  # importing dbt already attached a root handler, which would make basicConfig a no-op
)

# ---------- 1. EXTRACT: download all 12 months of 2025 ----------

DATA_DIR.mkdir(exist_ok=True)
for m in range(1, 13):
    name = f"yellow_tripdata_2025-{m:02d}.parquet"
    url = BASE + name
    try:
        response = httpx.get(url, timeout=120)
        response.raise_for_status()
        (DATA_DIR / name).write_bytes(response.content)  # saves to data/<name>
        print(f"Success: Fetched {url}")
        logging.info(f"Fetched {url}")
    except httpx.HTTPError as e:
        print(f"Error: Failed {url}: {e}")
        logging.error(f"Failed {url}: {e}")

# ---------- 2. TRANSFORM: run dbt build ----------
# sources.yml reads data/*.parquet directly, so no separate load step is needed.

try:
    os.chdir(PROJECT_DIR)
    res = dbtRunner().invoke(["build"])
except Exception as e:
    # dbt couldn't even start (bad install, missing project folder, etc.)
    print(f"Error: dbt failed to start: {e}")
    logging.error(f"dbt failed to start: {e}")
    sys.exit(1)

# res.exception is set if dbt itself crashed (e.g. bad profiles.yml);
# res.success is False if any model or test failed
if res.exception:
    print(f"Error: dbt crashed: {res.exception}")
    logging.error(f"dbt crashed: {res.exception}")
elif not res.success:
    print("Error: dbt build finished with model/test failures")
    logging.error("dbt build finished with model/test failures")
else:
    print("Success: dbt build completed")
    logging.info("dbt build completed")
sys.exit(0 if res.success else 1)
