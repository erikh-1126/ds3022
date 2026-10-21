"""
GitHub GraphQL API

Why the GraphQL?
- One endpoint (POST https://api.github.com/graphql) instead of many REST URLs.
- You ask for exactly the fields you want -- no over-fetching of huge JSON payloads.
- Related data (issue -> author -> labels) comes back in a single request.
- Pagination is cursor-based: each page returns `endCursor`, which you pass as `after`.

Rate limits are measured in "points" (5000/hour), not requests. The `rateLimit`
field can be added to any query to see the cost and what remains.

https://docs.github.com/en/graphql
"""

import os
import httpx


PER_PAGE = 100                     # GitHub max is 100
MAX_PAGES = 3                      # keep the demo small

TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"

# the query: the repo is hard-coded; $cursor is a variable 
# because it changes every page
QUERY = """
query($cursor: String) {
  repository(owner: "pandas-dev", name: "pandas") {
    issues(states: CLOSED, first: %d, after: $cursor,
           orderBy: {field: CREATED_AT, direction: DESC}) {
      totalCount
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        closedAt
        author { login }
      }
    }
  }
}
""" % PER_PAGE

# function to POST one page of the query; returns the `issues` block or None on failure
def fetch_page(cursor=None):
  try:
    payload = {"query": QUERY, "variables": {"cursor": cursor}}
    r = httpx.post(URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    # GraphQL returns HTTP 200 even for query errors -- check the "errors" key
    if "errors" in data:
      print(f"GraphQL errors: {data['errors']}")
      return None

    return data["data"]["repository"]["issues"]
  except httpx.HTTPError as e:
    print(f"request failed: {e}")
    return None

if __name__ == "__main__":
  cursor, issues = None, []
  for page in range(1, MAX_PAGES + 1):
    block = fetch_page(cursor)
    if block is None:
      break
    issues.extend(block["nodes"])

    # follow the cursor to the next page, stop when there isn't one
    if not block["pageInfo"]["hasNextPage"]:
      break
    cursor = block["pageInfo"]["endCursor"]

  for i in issues[:10]:
    author = i["author"]["login"] if i["author"] else "ghost"   # deleted users return null
    print(f"#{i['number']:<6} {i['closedAt']}  {author:<20} {i['title'][:60]}")
  print(f"fetched {len(issues)} closed issues")
