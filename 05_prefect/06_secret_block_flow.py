from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.blocks.system import Secret

YEAR = 2026

@task(retries=3, retry_delay_seconds=10)
def print_out_block():
    logger = get_run_logger()
    logger.info("Getting secret fname")
    bfname = Secret.load("fname")
    fname = bfname.get()
    logger.info(f"The fname is {fname}")
    return fname

@flow(name="block-print-flow", task_runner=ThreadPoolTaskRunner(max_workers=2))
def block_print():
    logger = get_run_logger()
    logger.info(f"Starting the fname block print {YEAR}")
    print_out_block()

if __name__ == "__main__":
    block_print()
