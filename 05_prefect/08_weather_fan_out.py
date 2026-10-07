"""
Fan-out / fan-in demo: fetch July 2025 daily highs for several cities in parallel
(Open-Meteo, no API key), then rank them by average high.
"""

import requests
from prefect import flow, task, unmapped, get_run_logger
from prefect.futures import as_completed
from prefect.task_runners import ThreadPoolTaskRunner

CITIES = {
  "Charlottesville": (38.03, -78.48),
  "Phoenix": (33.45, -112.07),
  "Anchorage": (61.22, -149.90),
  "Miami": (25.76, -80.19),
  "Seattle": (47.61, -122.33),
  "Denver": (39.74, -104.99),
}


# One API call per city to get average high temp
@task(retries=3, retry_delay_seconds=5)
def fetch_avg_high(city: str, coords: tuple, dates: dict) -> dict:
  lat, lon = coords
  r = requests.get("https://archive-api.open-meteo.com/v1/archive", timeout=30, params={
    "latitude": lat, "longitude": lon, "daily": "temperature_2m_max",
    "temperature_unit": "fahrenheit", **dates})
  r.raise_for_status()
  highs = r.json()["daily"]["temperature_2m_max"]
  return {"city": city, "avg_high": round(sum(highs) / len(highs), 1)}


# assemble results into a final output
@task
def rank(results: list[dict]) -> list[dict]:
  # Fan-in: many results -> one sorted list
  return sorted(results, key=lambda r: r["avg_high"], reverse=True)


# flow that fans out city requests, all running at once
@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=len(CITIES)))
def weather_fan_out(start: str = "2026-08-01", end: str = "2026-08-31"):
  logger = get_run_logger()
  try:
    futures = fetch_avg_high.map(list(CITIES), list(CITIES.values()),
                                 unmapped({"start_date": start, "end_date": end}))
    for f in as_completed(futures):
      print(f"finished: {f.result()['city']}")

    for r in rank(futures):
      print(f"{r['city']:<16} {r['avg_high']}°F")
  except Exception as e:
    logger.error(f"Flow failed: {e}")
    raise


if __name__ == "__main__":
  weather_fan_out()
