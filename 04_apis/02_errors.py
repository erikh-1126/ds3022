# 01 - The happy path. No error handling at all.
# Break it: misspell USER, or turn off Wi-Fi, and read the traceback.

import httpx
import json

USER = "schaconz"
URL = "https://api.github.com/users/{user}/events/public"

try:
	response = httpx.get(URL.format(user=USER))
	response.raise_for_status()
	data = response.json()
	for item in data:
		print(item["repo"]["name"], "-", item["type"])

except httpx.HTTPError as e:
	print(e)

finally:
	print("I am done now")