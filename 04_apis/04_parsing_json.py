import os
import json
import httpx


# set your GH token before running
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
REPO = "uvasds-systems/ds3022"
URL = f"https://api.github.com/repos/{REPO}/commits/HEAD"

# Fetch ONE commit (the most recent)
response = httpx.get(URL, headers=HEADERS)

# response.text is the raw JSON string; 
# response.json() deserializes it into Python dicts/lists
commit = response.json()

# Print the full blob, pretty-printed, so we can walk through its structure
print(json.dumps(commit, indent=2))


# --- Step 2: navigate the structure ---
# Objects become dicts (index by key), arrays become lists (index by position or loop).
#
# print(commit["sha"])
# print(commit["commit"]["author"]["name"])
# print(commit["commit"]["author"]["date"])     # a str -- JSON has no date type
# print(commit["stats"])                        # nested dict: total, additions, deletions
# print(len(commit["files"]))                   # list of dicts, one per changed file
# for f in commit["files"]:
#   print(f["filename"], f["status"], f["changes"])
#
# # Top-level "author" is the GitHub account, and is null (None) when the commit email
# # isn't linked to one. commit["author"]["login"] then raises TypeError; guard with .get()
# login = (commit.get("author") or {}).get("login")
# print(login)


# --- Step 3: flatten into a record ---
# Pick the fields we care about, convert types, and produce one flat dict = one row.
# Do this for many commits and you have a list of rows ready for a dataframe or table.
#
# from datetime import datetime
#
# record = {
#   "sha": commit["sha"],
#   "author_name": commit["commit"]["author"]["name"],
#   "author_login": (commit.get("author") or {}).get("login"),
#   "committed_at": datetime.fromisoformat(commit["commit"]["author"]["date"]),
#   "message": commit["commit"]["message"].splitlines()[0],   # first line only
#   "files_changed": len(commit["files"]),
#   "additions": commit["stats"]["additions"],
#   "deletions": commit["stats"]["deletions"],
# }
# print(record)
