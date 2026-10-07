import os
import httpx


REPO = "duckdb/duckdb"     # org/repo to fetch commits from
PER_PAGE = 10              # GitHub max is 100
MAX_PAGES = 5              # stop early so we don't page through 50,000 commits


# set your GH token before running
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = f"https://api.github.com/repos/{REPO}/commits"


# Request page 1, 2, 3... until a page comes back empty or we hit MAX_PAGES
page = 1
while page <= MAX_PAGES:
  response = httpx.get(URL, headers=HEADERS, params={"per_page": PER_PAGE, "page": page})
  commits = response.json()

  if not commits:
    break

  print(f"--- page {page}: {len(commits)} commits ---")
  for c in commits:
    print(c["sha"][:7], c["commit"]["author"]["date"], c["commit"]["message"].splitlines()[0])

  page += 1

# --- Alternative: follow the Link header instead of counting pages ---
# GitHub returns a "Link" header with the URL of the next page. httpx parses it into
# response.links. The "next" URL already includes per_page and page, so params are only
# needed on the first request. The loop ends when there is no "next" link (last page).
#
# url = URL
# params = {"per_page": PER_PAGE}
# page = 1
# while url and page <= MAX_PAGES:
#   response = httpx.get(url, headers=HEADERS, params=params)
#   commits = response.json()
#
#   print(f"--- page {page}: {len(commits)} commits ---")
#   for c in commits:
#     print(c["sha"][:7], c["commit"]["author"]["date"], c["commit"]["message"].splitlines()[0])
#
#   url = response.links.get("next", {}).get("url")   # None on the last page
#   params = None
#   page += 1
