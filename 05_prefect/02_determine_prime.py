from prefect import flow, task, get_run_logger
from sympy import isprime
import random

@task(log_prints=True)
def get_number():
  # Generate a random integer between 1000 and 100000 (inclusive)
  num = random.randint(1000, 100000)
  print(f"The number is: {num}.")
  return num

@task(log_prints=True)
def determine_prime(number):
  # Test primality with sympy and report the result
  if isprime(number):
    print(f"{number} is a prime number.")
  else:
    print(f"{number} is NOT a prime number.")

@flow
def determine_prime_flow():
  logger = get_run_logger()
  try:
    number = get_number()
    determine_prime(number)
  except Exception as e:
    logger.error(f"Flow failed: {e}")
    raise

if __name__ == "__main__":
  determine_prime_flow()

