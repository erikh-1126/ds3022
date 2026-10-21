import gzip
import json
from calendar import monthrange
from collections import Counter
import requests
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner


# one file per hour, e.g. 2025-01-01-0 ... 2025-01-31-23 (hours are not zero-padded)
URL = "https://data.gharchive.org/{hour}.json.gz"

@task(retries=3)
def count_events_for_hour(hour):
    # stream one hourly file and return {event_type: count}
    counts = Counter()
    with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
        r.raise_for_status()  # an HTTP error fails the task, so Prefect retries it
        # decompress the raw byte stream as it arrives; each line is one JSON event
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for line in gz:
                try:
                    counts[json.loads(line)["type"]] += 1
                except (json.JSONDecodeError, KeyError):
                    counts["_malformed"] += 1  # count bad lines instead of failing the hour
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # sum each hour's {event_type: count} dict into one Counter
    totals = sum((Counter(h) for h in hourly_counts), Counter())
    # order by count, descending, and print each type
    ordered = dict(totals.most_common())
    for event_type, n in ordered.items():
        print(f"{event_type}: {n}")
    print(f"total: {totals.total()} events across {len(hourly_counts)} hours")
    return ordered


# cap concurrent downloads so a full month doesn't open hundreds of connections at once
@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=16))
def gharchive_flow(year: int = 2025, month: int = 1, num_days: int = 1):
    # build an "YYYY-MM-DD-H" key for every hour of the first num_days days of the month
    # (set num_days=31 or higher to pull the full month)
    days = min(num_days, monthrange(year, month)[1])
    hours = [f"{year}-{month:02d}-{d:02d}-{h}" for d in range(1, days + 1) for h in range(24)]
    # map one task per hour
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)


if __name__ == "__main__":
    gharchive_flow()
