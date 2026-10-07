from prefect import flow, task

@task
def create_message():
  # Build the greeting returned to the flow
  msg = "Hello there"
  return msg

@flow(log_prints=True)
def hello_world_flow():
  # Call the task and print its result (captured in Prefect logs)
  message = create_message()
  print(message)

if __name__ == "__main__":
  hello_world_flow()
