import random
import time
from prefect import flow, task

# @task(log_prints=True, retries=3, retry_delay_seconds=2)

@task(log_prints=True)
def print_and_sleep(value: int):
    delay = random.randint(3,40)
    time.sleep(delay)
    print(f"Value: {value} - (delayed {delay}s)")
    time.sleep(5)
    return value

@task(log_prints=True)
def range_task(start: int = 1, end: int = 40):
    futures = print_and_sleep.map(range(start, end + 1))
    results = futures.result()
    print(f"Completed {len(results)} subtasks")
    return results

@flow
def fan_out_flow():
    range_task()

if __name__ == "__main__":
    fan_out_flow()