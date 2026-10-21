from prefect import flow, task, get_run_logger

@task(log_prints=True)
def iterate_number(number):
  # compute x^2 through x^5 for a single number, printing each (captured as Prefect logs)
  results = {}
  for p in range(2, 6):
    results[f"x^{p}"] = number ** p
    print(f"{number}^{p} = {results[f'x^{p}']}")
  return results

@flow
def map_them_all(nums):
  logger = get_run_logger()
  # .map() submits one task run per item in nums and returns a list of futures
  futures = iterate_number.map(nums)
  # .result() waits for every mapped run to finish and collects their return values
  results = futures.result()
  for n, r in zip(nums, results):
    logger.info(f"{n}: {r}")
  return results


if __name__ == "__main__":
  vals = list(range(1, 5))
  map_them_all(vals)
