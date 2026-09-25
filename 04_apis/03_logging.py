import httpx
import json
import logging

USER = "schaconz"
URL = "https://api.github.com/users/{user}/events/public"

logging.basicConfig(
  filename = "events.log",
  level = logging.INFO,
  format = "%(asctime)s - %(levelname)s - %(message)s"
)

try:
  response = httpx.get(URL.format(user=USER))
  response.raise_for_status()
  data = response.json()

  for item in data:
    print(item["repo"]["name"], " - ", item["type"])

  logging.info(f"Fetched {len(data)} events for {USER}")

except httpx.HTTPError as e:
  print(e)
  logging.error(f"Error fetching events for {USER}: {e}")
