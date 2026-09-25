# 01 - The happy path. No error handling at all.
# Break it: misspell USER, or turn off Wi-Fi, and read the traceback.

import httpx
import json

USER = "schacon"
URL = "https://api.github.com/users/{user}/events/public"

response = httpx.get(URL.format(user=USER))

data = response.json()
# print(json.dumps(data, indent=2))

for item in data:
	print(item["repo"]["name"], "-", item["type"])