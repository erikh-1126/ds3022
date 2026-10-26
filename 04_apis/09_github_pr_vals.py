"""
Find a recent commit on a repo's default branch that was created by merging
a pull request, then print the key values of that merge and its PR.

Steps (3 REST calls per commit checked, plus 1 for the commit list):
  1. GET /repos/{repo}/commits                 recent commits on the default branch
  2. GET /repos/{repo}/commits/{sha}/pulls     PRs associated with a commit
     -> a commit is a PR merge if a PR lists it as its merge_commit_sha
  3. GET /repos/{repo}/pulls/{number}          PR details: additions, deletions, changed_files
  4. GET /repos/{repo}/commits/{sha}           commit stats + per-file changes

Note: pandas uses "squash and merge", so each PR lands as a single commit with
one parent (no classic two-parent merge commit). Matching on merge_commit_sha
works for squash, rebase, and regular merges alike.

Rate limits: 60 req/hr unauthenticated, 5000 req/hr authenticated.
https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2026-03-10
"""

import os
import time
import httpx

REPO = "pandas-dev/pandas"   # org/repo to inspect
MAX_COMMITS = 40             # how many recent commits to scan for a PR merge
API = f"https://api.github.com/repos/{REPO}"

# set your GH token before running
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"}

# GET a URL and check quota; if exhausted, sleep until reset and retry
def get_and_check_quota(url, params=None):
  while True:
    try:
      r = httpx.get(url, headers=HEADERS, params=params)
      remaining = int(r.headers.get("x-ratelimit-remaining", 0))
      reset = int(r.headers.get("x-ratelimit-reset", time.time()))

      # GitHub returns 403/429 once the quota is exhausted
      if r.status_code in (403, 429) and remaining == 0:
        wait = max(reset - time.time(), 0) + 1
        print(f"Quota exhausted, sleeping {wait:.0f}s")
        time.sleep(wait)
        continue

      r.raise_for_status()
      return r.json()
    except httpx.HTTPError as e:
      print(f"GET {url} failed: {e}")
      return None

# walk recent commits and return (commit_sha, pr) for the first PR-merge commit
def find_pr_merge():
  commits = get_and_check_quota(f"{API}/commits", {"per_page": MAX_COMMITS}) or []
  for c in commits:
    sha = c["sha"]
    pulls = get_and_check_quota(f"{API}/commits/{sha}/pulls") or []
    # the merge commit is the one the PR points to as merge_commit_sha
    for pr in pulls:
      if pr.get("merged_at") and pr.get("merge_commit_sha") == sha:
        return sha, pr
  return None, None

# print the key values of the merge commit and its PR
def show_merge(sha, pr_summary):
  pr = get_and_check_quota(f"{API}/pulls/{pr_summary['number']}")
  commit = get_and_check_quota(f"{API}/commits/{sha}")
  if not pr or not commit:
    return

  stats = commit["stats"]   # {"additions": n, "deletions": n, "total": n}
  print(f"\nMerge commit for PR #{pr['number']} in {REPO}")
  print(f"  Title:          {pr['title']}")
  print(f"  URL:            {pr['html_url']}")
  print(f"  Author:         {pr['user']['login']}")
  print(f"  Merged by:      {(pr.get('merged_by') or {}).get('login')}")
  print(f"  Created at:     {pr['created_at']}")
  print(f"  Merged at:      {pr['merged_at']}")
  print(f"  Branch:         {pr['head']['ref']} -> {pr['base']['ref']}")
  print(f"  Merge SHA:      {sha}")
  print(f"  Parents:        {len(commit['parents'])}")
  print(f"  PR commits:     {pr['commits']}")
  print(f"  Labels:         {', '.join(l['name'] for l in pr['labels']) or '-'}")
  print(f"  Files changed:  {pr['changed_files']}")
  print(f"  Lines added:    +{stats['additions']}")
  print(f"  Lines deleted:  -{stats['deletions']}")
  print(f"  Lines changed:  {stats['total']}")

  # per-file breakdown of the merge
  print("\n  Per-file changes:")
  for f in commit["files"]:
    print(f"    {f['changes']:>6}  (+{f['additions']}/-{f['deletions']})  {f['filename']}")


if __name__ == "__main__":
  sha, pr = find_pr_merge()
  if sha:
    show_merge(sha, pr)
  else:
    print(f"No PR merge found in last {MAX_COMMITS} commits of {REPO}")
