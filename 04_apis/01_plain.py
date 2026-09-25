# 01 - The happy path. No error handling at all.
# Break it: misspell USER, or turn off Wi-Fi, and read the traceback.

import httpx
import json

USER = "schaconxyz"
URL = "https://api.github.com/users/{user}/events/public"

response = httpx.get(URL.format(user=USER))

data = response.json()

for item in data:
  print(item["repo"]["name"], " - ", item["type"])
