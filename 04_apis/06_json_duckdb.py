import os
# import logging

import duckdb
import httpx


REPO = "pandas-dev/pandas"
PER_PAGE = 100
DB_FILE = "github.duckdb"
TABLE = "commits"
RAW_FILE = "commits_raw.json"

# set your GH token before running
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = f"https://api.github.com/repos/{REPO}/commits"

try:
  # --- Step 1: fetch a single page of commits and save the raw JSON ---
  response = httpx.get(URL, headers=HEADERS, params={"per_page": PER_PAGE})
  response.raise_for_status()
  with open(RAW_FILE, "w") as f:
    f.write(response.text)

  # --- Step 2: let DuckDB parse the JSON ---
  # read_json infers a nested schema (structs for objects, lists for arrays).
  # Pull nested fields out with dot notation: column.field.subfield
  with duckdb.connect(DB_FILE) as con:
    con.execute(f"""
      CREATE OR REPLACE TABLE {TABLE} AS
      SELECT sha,
             commit.author.name                   AS author_name,
             author.login                         AS author_login,   -- null when email isn't linked to an account
             commit.author.date::TIMESTAMPTZ      AS committed_at,
             split_part(commit.message, chr(10), 1) AS message,      -- first line only
             html_url                             AS url
      FROM read_json('{RAW_FILE}')
    """)

    # --- Step 3: confirm it landed ---
    count = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"{count} rows in {DB_FILE}:{TABLE}")
    con.sql(f"SELECT sha[:7] AS sha, author_login, committed_at, message FROM {TABLE} ORDER BY committed_at DESC LIMIT 10").show()

except httpx.HTTPError as e:
  print(e)
except OSError as e:
  print(e)
except duckdb.Error as e:
  print(e)


# --- Alternative: flatten the JSON in Python first ---
# Walk each nested commit object and build a flat dict (one table row), then insert
# the rows into a table with an explicit schema. More code, but full control in Python.
#
# from datetime import datetime
#
# def flatten(c):
#   return {
#     "sha": c["sha"],
#     "author_name": c["commit"]["author"]["name"],
#     "author_login": (c.get("author") or {}).get("login"),
#     "committed_at": datetime.fromisoformat(c["commit"]["author"]["date"]),
#     "message": c["commit"]["message"].splitlines()[0],
#     "url": c["html_url"],
#   }
#
# rows = [flatten(c) for c in response.json()]
# with duckdb.connect(DB_FILE) as con:
#   con.execute(f"""
#     CREATE OR REPLACE TABLE {TABLE} (
#       sha VARCHAR PRIMARY KEY, author_name VARCHAR, author_login VARCHAR,
#       committed_at TIMESTAMPTZ, message VARCHAR, url VARCHAR
#     )
#   """)
#   con.executemany(
#     f"INSERT INTO {TABLE} VALUES ($sha, $author_name, $author_login, $committed_at, $message, $url)",
#     rows,
#   )
