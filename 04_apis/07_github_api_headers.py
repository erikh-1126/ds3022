"""
Requests from modern APIs are rate-limited to prevent abuse
and denial of service attacks by flooding requests. 

Unauthenticated users have a limit of 60 requests/hour.
Authenticated users have a limit of 5000 requests/hour.

https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2026-03-10

The x-ratelimit response headers returned by the GitHub core API:

x-ratelimit-limit: 5000            # max requests per hour
x-ratelimit-remaining: 4957        # requests left in the current window
x-ratelimit-reset: 1792511353      # epoch seconds when the window resets
x-ratelimit-used: 43               # requests used in the current window
x-ratelimit-resource: core         # which rate-limit bucket this call counts against

You can also call the GET /rate_limit endpoint for a periodic overview of all 
resource limits for the authenticated user. 
"""

import os
import httpx

REPO = "numpy/numpy"       # org/repo to fetch commits from

TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = f"https://api.github.com/repos/{REPO}/commits"

# make a single one-page request and print every response header
if __name__ == "__main__":
  try:
    # r = httpx.get(URL, headers=HEADERS, params={"per_page": 1})
    r = httpx.get(URL, params={"per_page": 1})
    r.raise_for_status()
    for key, value in r.headers.items():
      print(f"{key}: {value}")
  except httpx.HTTPError as e:
    print(f"Request failed: {e}")
