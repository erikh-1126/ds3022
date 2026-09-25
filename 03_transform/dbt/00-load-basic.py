import os
from pathlib import Path
import requests

URL = "https://s3.amazonaws.com/uvasds-data/taxi/yellow_tripdata_2025-{i:02d}.parquet"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Download months 1-12 one at a time and save each file to data/
for i in range(1, 13):
    url = URL.format(i=i)
    path = DATA_DIR / url.rsplit("/", 1)[-1]
    response = requests.get(url)
    path.write_bytes(response.content)
    print(f"Fetched {path.name} ({len(response.content) / 1e6:.1f} MB)")

# Run dbt
os.system("dbt build")
