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

You can call the GET /rate_limit endpoint for a periodic overview of all 
resource families for the authenticated user. 
"""

import os
import httpx
import time

REPO = "duckdb/duckdb"     # org/repo to fetch commits from
PER_PAGE = 100             # GitHub max is 100

# set your GH token before running
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = f"https://api.github.com/repos/{REPO}/commits"

# function to pull a page from the API and check quota/limit
# if the quota is exhausted, sleep until the reset time and retry the same page
def pull_and_check_quota(page):
  while True:
    try:
      r = httpx.get(URL, headers=HEADERS, params={"per_page": PER_PAGE, "page": page})
      limit = int(r.headers.get("x-ratelimit-limit", 0))
      remaining = int(r.headers.get("x-ratelimit-remaining", 0))
      reset = int(r.headers.get("x-ratelimit-reset", time.time()))
      print(f"page {page} | Remaining: {remaining}/{limit}")

      # check quota before raise_for_status: GitHub returns 403/429 once exhausted
      if r.status_code in (403, 429) and remaining == 0:
        wait = max(reset - time.time(), 0) + 1
        print(f"Credits exhausted, sleeping {wait:.0f}s until reset")
        time.sleep(wait)
        continue                     # retry the same page
      elif remaining == 0:
        print("This was the last call in the window")
      else:
        # seconds left until the quota window resets
        secs_left = max(int(reset - time.time()), 0)
        print(f"Credits remaining | Resets in {secs_left} seconds")

      r.raise_for_status()
      return r.json()
    except httpx.HTTPError as e:
      print(f"page {page} failed: {e}")
      return None

if __name__ == "__main__":
  for page in range(1, 101):
    pull_and_check_quota(page)
